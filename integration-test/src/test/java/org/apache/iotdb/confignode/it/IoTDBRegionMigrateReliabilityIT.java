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

package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.DataNodeKillPoints;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.itbase.exception.InconsistentDataException;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class IoTDBRegionMigrateReliabilityIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionMigrateReliabilityIT.class);
  private static final String INSERTION =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)";
  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // region Normal tests

  @Test
  public void normal1C2DTest() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet());
  }

  @Test
  public void normal3C3DTest() throws Exception {
    generalTest(2, 3, 3, 3, buildSet(), buildSet());
  }

  // endregion

  // region ConfigNode crash tests
  @Test
  public void cnCrashDuringPreCheck() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RegionTransitionState.REGION_MIGRATE_PREPARE.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringCreatePeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(AddRegionPeerState.CREATE_NEW_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringDoAddPeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(AddRegionPeerState.DO_ADD_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringUpdateCache() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE.toString()),
        buildSet());
  }

  @Test
  public void cnCrashDuringChangeRegionLeader() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RegionTransitionState.CHANGE_REGION_LEADER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringRemoveRegionPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RemoveRegionPeerState.REMOVE_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringDeleteOldRegionPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RemoveRegionPeerState.DELETE_OLD_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringRemoveRegionLocationCache() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE.toString()),
        buildSet());
  }

  @Test
  public void cnCrashTest() throws Exception {
    KeySetView<String, Boolean> killConfigNodeKeywords = buildSet();
    killConfigNodeKeywords.addAll(
        Arrays.stream(AddRegionPeerState.values())
            .map(Enum::toString)
            .collect(Collectors.toList()));
    killConfigNodeKeywords.addAll(
        Arrays.stream(RemoveRegionPeerState.values())
            .map(Enum::toString)
            .collect(Collectors.toList()));
    generalTest(1, 1, 1, 2, killConfigNodeKeywords, buildSet());
  }

  @Ignore
  @Test
  public void badKillPoint() throws Exception {
    generalTest(1, 1, 1, 2, buildSet("??"), buildSet());
  }

  // endregion

  // region coordinator DataNode crash tests

  @Test
  public void coordinatorCrashDuringRemovePeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.CoordinatorRemovePeer.name()));
  }

  // endregion

  // region original DataNode crash tests

  @Test
  public void originalCrashDuringRemovePeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.OriginalRemovePeer.name()));
  }

  @Test
  public void originalCrashDuringDeleteLocalPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.OriginalDeleteOldRegionPeer.name()));
  }

  // region Helpers

  public void generalTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> killConfigNodeKeywords,
      KeySetView<String, Boolean> killDataNodeKeywords)
      throws Exception {
    // prepare env
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(dataReplicateFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor);
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    ExecutorService service = IoTDBThreadPoolFactory.newCachedThreadPool("regionMigrateIT");
    EnvFactory.getEnv()
        .getConfigNodeWrapperList()
        .forEach(
            configNodeWrapper ->
                service.submit(() -> nodeLogKillPoint(configNodeWrapper, killConfigNodeKeywords)));
    EnvFactory.getEnv()
        .getDataNodeWrapperList()
        .forEach(
            dataNodeWrapper ->
                service.submit(() -> nodeLogKillPoint(dataNodeWrapper, killDataNodeKeywords)));

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      statement.execute(INSERTION);

      ResultSet result = statement.executeQuery(SHOW_REGIONS);
      Map<Integer, Set<Integer>> regionMap = getRegionMap(result);

      result = statement.executeQuery(SHOW_DATANODES);
      Set<Integer> dataNodeSet = new HashSet<>();
      while (result.next()) {
        dataNodeSet.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      final int selectedRegion = selectRegion(regionMap);
      final int originalDataNode = selectOriginalDataNode(regionMap, selectedRegion);
      final int destDataNode = selectDestDataNode(dataNodeSet, regionMap, selectedRegion);

      statement.execute(regionMigrateCommand(selectedRegion, originalDataNode, destDataNode));

      awaitUntilSuccess(statement, selectedRegion, originalDataNode, destDataNode);

      // make sure all kill points have been triggered
      Assert.assertTrue(killConfigNodeKeywords.isEmpty());
      Assert.assertTrue(killDataNodeKeywords.isEmpty());

      checkRegionFileClear(originalDataNode);
    } catch (InconsistentDataException ignore) {

    }
    LOGGER.info("test pass");
  }

  /**
   * Monitor the node's log and do something.
   *
   * @param nodeWrapper Easy to understand
   * @param killNodeKeywords When detect these keywords in node's log, stop the node forcibly
   */
  private static void nodeLogKillPoint(
      AbstractNodeWrapper nodeWrapper, KeySetView<String, Boolean> killNodeKeywords) {
    if (killNodeKeywords.isEmpty()) {
      return;
    }
    final String logFileName;
    if (nodeWrapper instanceof ConfigNodeWrapper) {
      logFileName = "log_confignode_all.log";
    } else {
      logFileName = "log_datanode_all.log";
    }
    ProcessBuilder builder =
        new ProcessBuilder(
            "tail",
            "-f",
            nodeWrapper.getNodePath() + File.separator + "logs" + File.separator + logFileName);
    builder.redirectErrorStream(true);

    try {
      Process process = builder.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          // if trigger more than one keyword at a same time, test code may have mistakes
          Assert.assertTrue(killNodeKeywords.stream().filter(line::contains).count() <= 1);
          String finalLine = line;
          Optional<String> detectedKeyword =
              killNodeKeywords.stream()
                  .filter(keyword -> finalLine.contains("breakpoint:" + keyword))
                  .findAny();
          if (detectedKeyword.isPresent()) {
            // each keyword only trigger once
            killNodeKeywords.remove(detectedKeyword.get());
            // reboot the node
            nodeWrapper.stopForcibly();
            nodeWrapper.start();
          }
          if (killNodeKeywords.isEmpty()) {
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String regionMigrateCommand(int who, int from, int to) {
    return String.format(REGION_MIGRATE_COMMAND_FORMAT, who, from, to);
  }

  private static Map<Integer, Set<Integer>> getRegionMap(ResultSet showRegionsResult)
      throws SQLException {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      if (String.valueOf(TConsensusGroupType.DataRegion)
          .equals(showRegionsResult.getString(ColumnHeaderConstant.TYPE))) {
        int region = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
        int dataNode = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        regionMap.putIfAbsent(region, new HashSet<>());
        regionMap.get(region).add(dataNode);
      }
    }
    return regionMap;
  }

  private static int selectRegion(Map<Integer, Set<Integer>> regionMap) {
    return regionMap.keySet().stream().findAny().orElseThrow(() -> new RuntimeException("gg"));
  }

  private static int selectOriginalDataNode(
      Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return regionMap.get(selectedRegion).stream()
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find original DataNode"));
  }

  private static int selectDestDataNode(
      Set<Integer> dataNodeSet, Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return dataNodeSet.stream()
        .filter(dataNodeId -> !regionMap.get(selectedRegion).contains(dataNodeId))
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find dest DataNode"));
  }

  private static void awaitUntilSuccess(
      Statement statement, int selectedRegion, int originalDataNode, int destDataNode) {
    AtomicReference<Set<Integer>> lastTimeDataNodes = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();
    try {
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .until(
              () -> {
                try {
                  Map<Integer, Set<Integer>> newRegionMap =
                      getRegionMap(statement.executeQuery(SHOW_REGIONS));
                  Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
                  lastTimeDataNodes.set(dataNodes);
                  return !dataNodes.contains(originalDataNode) && dataNodes.contains(destDataNode);
                } catch (Exception e) {
                  // Any exception can be ignored
                  lastException.set(e);
                  return false;
                }
              });
    } catch (ConditionTimeoutException e) {
      if (lastTimeDataNodes.get() == null) {
        LOGGER.error(
            "maybe show regions fail, lastTimeDataNodes is null, last Exception:",
            lastException.get());
        throw e;
      }
      String actualSetStr = lastTimeDataNodes.get().toString();
      lastTimeDataNodes.get().remove(originalDataNode);
      lastTimeDataNodes.get().add(destDataNode);
      String expectSetStr = lastTimeDataNodes.toString();
      LOGGER.info("DataNode Set {} is unexpected, expect {}", actualSetStr, expectSetStr);
      throw e;
    }
  }

  /** Check whether the original DataNode's region file has been deleted. */
  private static void checkRegionFileClear(int dataNode) {
    String nodePath = EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().getNodePath();
    File originalRegionDir =
        new File(
            nodePath
                + File.separator
                + IoTDBConstant.DATA_FOLDER_NAME
                + File.separator
                + "datanode"
                + File.separator
                + IoTDBConstant.CONSENSUS_FOLDER_NAME
                + File.separator
                + "data_region");
    Assert.assertTrue(originalRegionDir.isDirectory());
    Assert.assertEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
  }

  private static KeySetView<String, Boolean> buildSet(String... keywords) {
    KeySetView<String, Boolean> result = ConcurrentHashMap.newKeySet();
    result.addAll(Arrays.asList(keywords));
    return result;
  }

  // endregion
}
