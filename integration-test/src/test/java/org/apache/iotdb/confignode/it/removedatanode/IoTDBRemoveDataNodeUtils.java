/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.it.removedatanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;

import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class IoTDBRemoveDataNodeUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBRemoveDataNodeUtils.class);

  public static String generateRemoveString(Set<Integer> dataNodes) {
    StringBuilder sb = new StringBuilder("remove datanode ");

    for (Integer node : dataNodes) {
      sb.append(node).append(", ");
    }

    sb.setLength(sb.length() - 2);

    return sb.toString();
  }

  public static Connection getConnectionWithSQLType(SQLModel model) throws SQLException {
    if (SQLModel.TABLE_MODEL_SQL.equals(model)) {
      return EnvFactory.getEnv().getTableConnection();
    } else {
      return EnvFactory.getEnv().getConnection();
    }
  }

  public static Set<Integer> selectRemoveDataNodes(
      Set<Integer> allDataNodeId, int removeDataNodeNum) {
    List<Integer> shuffledDataNodeIds = new ArrayList<>(allDataNodeId);
    Collections.shuffle(shuffledDataNodeIds);
    return new HashSet<>(shuffledDataNodeIds.subList(0, removeDataNodeNum));
  }

  /**
   * Select {@code removeDataNodeNum} DataNodes to remove such that no two of them host a replica of
   * the same consensus group. Removing two DataNodes that share a region group would make the
   * generated {@code RemoveDataNodesProcedure} try to migrate two replicas of the same group at
   * once, which the ConfigNode rejects ("Only one replica of the same consensus group is allowed to
   * be migrated at the same time."). Randomly picking DataNodes (see {@link
   * #selectRemoveDataNodes}) therefore makes multi-DataNode-remove tests flaky; this method instead
   * searches exhaustively (with backtracking) for a conflict-free selection, so it fails only when
   * no such selection exists rather than when an unlucky pick order dead-ends.
   *
   * @param allDataNodeId all registered DataNode ids
   * @param removeDataNodeNum how many DataNodes to remove
   * @param regionMap regionId -&gt; the set of DataNode ids hosting a replica of that region group
   *     (all region types)
   * @return the selected DataNode ids, or throws if no conflict-free selection of the requested
   *     size exists
   */
  public static Set<Integer> selectRemoveDataNodesWithoutRegionConflict(
      Set<Integer> allDataNodeId, int removeDataNodeNum, Map<Integer, Set<Integer>> regionMap) {
    // dataNodeId -> the set of region groups it hosts a replica of
    Map<Integer, Set<Integer>> dataNodeToRegions = new HashMap<>();
    for (Integer dataNodeId : allDataNodeId) {
      dataNodeToRegions.put(dataNodeId, new HashSet<>());
    }
    regionMap.forEach(
        (regionId, dataNodeIds) ->
            dataNodeIds.forEach(
                dataNodeId ->
                    dataNodeToRegions
                        .computeIfAbsent(dataNodeId, id -> new HashSet<>())
                        .add(regionId)));

    // Shuffle so that, when several conflict-free selections exist, a random one is returned (keeps
    // the coverage of this test varied across runs).
    List<Integer> shuffledDataNodeIds = new ArrayList<>(allDataNodeId);
    Collections.shuffle(shuffledDataNodeIds);

    // Search exhaustively (with backtracking) for a set of DataNodes whose hosted region groups are
    // pairwise disjoint. A single-pass greedy that unconditionally commits to the first shuffled
    // DataNode can dead-end and wrongly throw even though a valid conflict-free selection exists -
    // e.g. when that first DataNode happens to share a consensus group with every other DataNode -
    // which made this test flaky.
    Set<Integer> selected = new LinkedHashSet<>();
    if (searchConflictFreeDataNodes(
        shuffledDataNodeIds, 0, removeDataNodeNum, new HashSet<>(), selected, dataNodeToRegions)) {
      return selected;
    }
    throw new IllegalStateException(
        String.format(
            "Cannot select %d DataNodes to remove without a same-region-group conflict. "
                + "allDataNodeId=%s, regionMap=%s",
            removeDataNodeNum, allDataNodeId, regionMap));
  }

  /**
   * Depth-first search with backtracking for {@code need} DataNodes whose hosted region groups are
   * pairwise disjoint. On success returns {@code true} and leaves the chosen ids in {@code
   * selected}; on failure returns {@code false} with {@code selected} restored to its prior state.
   *
   * @param dataNodeIds candidate DataNode ids (iterated from {@code start} onwards)
   * @param start index to start picking from, so each combination is visited at most once
   * @param need how many DataNodes must be selected in total
   * @param coveredRegions region groups already covered by the currently-selected DataNodes
   * @param selected the DataNodes chosen so far (mutated during the search)
   * @param dataNodeToRegions dataNodeId -&gt; the region groups it hosts a replica of
   */
  private static boolean searchConflictFreeDataNodes(
      List<Integer> dataNodeIds,
      int start,
      int need,
      Set<Integer> coveredRegions,
      Set<Integer> selected,
      Map<Integer, Set<Integer>> dataNodeToRegions) {
    if (selected.size() == need) {
      return true;
    }
    for (int i = start; i < dataNodeIds.size(); i++) {
      Integer dataNodeId = dataNodeIds.get(i);
      Set<Integer> regions = dataNodeToRegions.getOrDefault(dataNodeId, Collections.emptySet());
      if (Collections.disjoint(coveredRegions, regions)) {
        selected.add(dataNodeId);
        Set<Integer> nextCovered = new HashSet<>(coveredRegions);
        nextCovered.addAll(regions);
        if (searchConflictFreeDataNodes(
            dataNodeIds, i + 1, need, nextCovered, selected, dataNodeToRegions)) {
          return true;
        }
        selected.remove(dataNodeId);
      }
    }
    return false;
  }

  public static void restartDataNodes(List<DataNodeWrapper> dataNodeWrappers) {
    dataNodeWrappers.parallelStream()
        .forEach(
            nodeWrapper -> {
              nodeWrapper.stopForcibly();
              Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .pollDelay(2, TimeUnit.SECONDS)
                  .until(() -> !nodeWrapper.isAlive());
              LOGGER.info("Node {} stopped.", nodeWrapper.getId());
              nodeWrapper.start();
              Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .pollDelay(2, TimeUnit.SECONDS)
                  .until(nodeWrapper::isAlive);
              try {
                TimeUnit.SECONDS.sleep(10);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              LOGGER.info("Node {} restarted.", nodeWrapper.getId());
            });
  }

  public static void stopDataNodes(List<DataNodeWrapper> dataNodeWrappers) {
    dataNodeWrappers.parallelStream()
        .forEach(
            nodeWrapper -> {
              nodeWrapper.stopForcibly();
              Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .pollDelay(2, TimeUnit.SECONDS)
                  .until(() -> !nodeWrapper.isAlive());
              LOGGER.info("Node {} stopped.", nodeWrapper.getId());
            });
  }

  public static void awaitUntilSuccess(
      AtomicReference<SyncConfigNodeIServiceClient> clientRef,
      List<TDataNodeLocation> removeDataNodeLocations) {
    AtomicReference<List<TDataNodeLocation>> lastTimeDataNodeLocations = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();

    try {
      Awaitility.await()
          .atMost(5, TimeUnit.MINUTES)
          .pollDelay(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  List<TDataNodeLocation> remainingDataNodes =
                      clientRef
                          .get()
                          .getDataNodeConfiguration(-1)
                          .getDataNodeConfigurationMap()
                          .values()
                          .stream()
                          .map(TDataNodeConfiguration::getLocation)
                          .collect(Collectors.toList());
                  lastTimeDataNodeLocations.set(remainingDataNodes);
                  for (TDataNodeLocation location : removeDataNodeLocations) {
                    if (remainingDataNodes.contains(location)) {
                      return false;
                    }
                  }
                  return true;
                } catch (TException e) {
                  clientRef.set(
                      (SyncConfigNodeIServiceClient)
                          EnvFactory.getEnv().getLeaderConfigNodeConnection());
                  lastException.set(e);
                  return false;
                } catch (Exception e) {
                  // Any exception can be ignored
                  lastException.set(e);
                  return false;
                }
              });
    } catch (ConditionTimeoutException e) {
      if (lastTimeDataNodeLocations.get() == null) {
        LOGGER.error(
            "Maybe getDataNodeConfiguration fail, lastTimeDataNodeLocations is null, last Exception:",
            lastException.get());
        throw e;
      }
      String actualSetStr = lastTimeDataNodeLocations.get().toString();
      lastTimeDataNodeLocations.get().removeAll(removeDataNodeLocations);
      String expectedSetStr = lastTimeDataNodeLocations.get().toString();
      LOGGER.error(
          "Remove DataNodes timeout in 5 minutes, expected set: {}, actual set: {}",
          expectedSetStr,
          actualSetStr);
      if (lastException.get() == null) {
        LOGGER.info("No exception during awaiting");
      } else {
        LOGGER.error("Last exception during awaiting:", lastException.get());
      }
      throw e;
    }

    LOGGER.info("DataNodes has been successfully changed to {}", lastTimeDataNodeLocations.get());
  }
}
