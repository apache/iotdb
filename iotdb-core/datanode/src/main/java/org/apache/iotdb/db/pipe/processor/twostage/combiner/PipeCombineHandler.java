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

package org.apache.iotdb.db.pipe.processor.twostage.combiner;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultResponse;
import org.apache.iotdb.db.pipe.processor.twostage.operator.Operator;
import org.apache.iotdb.db.pipe.processor.twostage.state.State;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class PipeCombineHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeCombineHandler.class);

  private final String pipeName;
  private final long creationTime;

  private final Function<String, Operator> /* <combineId, operator> */ operatorConstructor;

  private static final Map<Integer, Integer> ALL_REGION_ID_2_DATANODE_ID_MAP = new HashMap<>();
  private static final AtomicLong ALL_REGION_ID_2_DATANODE_ID_MAP_LAST_UPDATE_TIME =
      new AtomicLong(0);
  private final ConcurrentMap<Integer, Integer> expectedRegionId2DataNodeIdMap =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Combiner> combineId2Combiner = new ConcurrentHashMap<>();

  public PipeCombineHandler(
      String pipeName, long creationTime, Function<String, Operator> operatorConstructor) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;

    this.operatorConstructor = operatorConstructor;

    fetchAndUpdateExpectedRegionId2DataNodeIdMap();
  }

  public synchronized TSStatus combine(int regionId, String combineId, State state) {
    return combineId2Combiner
        .computeIfAbsent(
            combineId,
            id ->
                new Combiner(operatorConstructor.apply(combineId), expectedRegionId2DataNodeIdMap))
        .combine(regionId, state);
  }

  public synchronized FetchCombineResultResponse fetchCombineResult(List<String> combineIdList)
      throws IOException {
    final Map<String, FetchCombineResultResponse.CombineResultType> combineId2ResultType =
        new HashMap<>();
    for (String combineId : combineIdList) {
      final Combiner combiner = combineId2Combiner.get(combineId);

      if (combiner == null || combiner.isOutdated()) {
        combineId2ResultType.put(combineId, FetchCombineResultResponse.CombineResultType.OUTDATED);
        continue;
      }

      combineId2ResultType.put(
          combineId,
          combiner.isComplete()
              ? FetchCombineResultResponse.CombineResultType.SUCCESS
              : FetchCombineResultResponse.CombineResultType.INCOMPLETE);
    }

    return FetchCombineResultResponse.toTPipeTransferResp(combineId2ResultType);
  }

  public void fetchAndUpdateExpectedRegionId2DataNodeIdMap() {
    updateExpectedRegionId2DataNodeIdMap(fetchExpectedRegionId2DataNodeIdMap());
  }

  private Map<Integer, Integer> fetchExpectedRegionId2DataNodeIdMap() {
    synchronized (ALL_REGION_ID_2_DATANODE_ID_MAP) {
      if (System.currentTimeMillis() - ALL_REGION_ID_2_DATANODE_ID_MAP_LAST_UPDATE_TIME.get()
          > PipeConfig.getInstance().getTwoStageAggregateDataRegionInfoCacheTimeInMs()) {
        ALL_REGION_ID_2_DATANODE_ID_MAP.clear();

        try (final ConfigNodeClient configNodeClient =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          final TShowRegionResp showRegionResp =
              configNodeClient.showRegion(
                  new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.DataRegion));
          if (showRegionResp == null || !showRegionResp.isSetRegionInfoList()) {
            throw new PipeException("Failed to fetch data region ids");
          }
          for (final TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
            if (!RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
              continue;
            }
            ALL_REGION_ID_2_DATANODE_ID_MAP.put(
                regionInfo.getConsensusGroupId().getId(), regionInfo.getDataNodeId());
          }
        } catch (ClientManagerException | TException e) {
          throw new PipeException("Failed to fetch data nodes", e);
        }

        ALL_REGION_ID_2_DATANODE_ID_MAP_LAST_UPDATE_TIME.set(System.currentTimeMillis());

        LOGGER.info(
            "Fetched data region ids {} at {}",
            ALL_REGION_ID_2_DATANODE_ID_MAP,
            ALL_REGION_ID_2_DATANODE_ID_MAP_LAST_UPDATE_TIME.get());
      }

      final Set<Integer> pipeRelatedRegionIdSet =
          new HashSet<>(PipeDataNodeAgent.task().getPipeTaskRegionIdSet(pipeName, creationTime));
      pipeRelatedRegionIdSet.removeIf(
          regionId -> !ALL_REGION_ID_2_DATANODE_ID_MAP.containsKey(regionId));
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "Two stage aggregate pipe (pipeName={}, creationTime={}) related region ids {}",
            pipeName,
            creationTime,
            pipeRelatedRegionIdSet);
      }
      return ALL_REGION_ID_2_DATANODE_ID_MAP.entrySet().stream()
          .filter(entry -> pipeRelatedRegionIdSet.contains(entry.getKey()))
          .collect(
              HashMap::new,
              (map, entry) -> map.put(entry.getKey(), entry.getValue()),
              HashMap::putAll);
    }
  }

  private synchronized void updateExpectedRegionId2DataNodeIdMap(
      Map<Integer, Integer> newExpectedRegionId2DataNodeIdMap) {
    expectedRegionId2DataNodeIdMap.clear();
    expectedRegionId2DataNodeIdMap.putAll(newExpectedRegionId2DataNodeIdMap);
  }

  public synchronized Set<Integer> getExpectedDataNodeIdSet() {
    return new HashSet<>(expectedRegionId2DataNodeIdMap.values());
  }

  public synchronized void cleanOutdatedCombiner() {
    combineId2Combiner
        .entrySet()
        .removeIf(
            entry -> {
              if (!entry.getValue().isComplete()) {
                LOGGER.info(
                    "Clean outdated incomplete combiner: pipeName={}, creationTime={}, combineId={}",
                    pipeName,
                    creationTime,
                    entry.getKey());
              }
              return entry.getValue().isOutdated();
            });
  }

  public synchronized void close() {
    combineId2Combiner.clear();
  }
}
