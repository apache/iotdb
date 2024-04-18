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
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
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

  private static final Set<Integer> ALL_REGION_ID_SET = new HashSet<>();
  private static final AtomicLong ALL_REGION_ID_SET_LAST_UPDATE_TIME = new AtomicLong(0);
  private final Set<Integer> expectedRegionIdSet;

  private final ConcurrentMap<String, Combiner> combineId2Combiner;

  public PipeCombineHandler(
      String pipeName, long creationTime, Function<String, Operator> operatorConstructor) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;

    this.operatorConstructor = operatorConstructor;

    expectedRegionIdSet = new HashSet<>();
    fetchAndUpdateExpectedRegionIdSet();

    combineId2Combiner = new ConcurrentHashMap<>();
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public synchronized TSStatus combine(int regionId, String combineId, State state) {
    return combineId2Combiner
        .computeIfAbsent(
            combineId,
            id -> new Combiner(operatorConstructor.apply(combineId), expectedRegionIdSet))
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

  public void fetchAndUpdateExpectedRegionIdSet() {
    updateExpectedRegionIdSet(fetchExpectedRegionIdSet());
  }

  private Set<Integer> fetchExpectedRegionIdSet() {
    synchronized (ALL_REGION_ID_SET) {
      if (System.currentTimeMillis() - ALL_REGION_ID_SET_LAST_UPDATE_TIME.get()
          > 5 * 60 * 1000) { // 5 minutes
        ALL_REGION_ID_SET.clear();

        try (final ConfigNodeClient configNodeClient =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          final TShowRegionResp showRegionResp =
              configNodeClient.showRegion(
                  new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.DataRegion));
          if (showRegionResp == null || !showRegionResp.isSetRegionInfoList()) {
            throw new PipeException("Failed to fetch data region ids");
          }
          for (final TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
            ALL_REGION_ID_SET.add(regionInfo.getConsensusGroupId().getId());
          }
        } catch (ClientManagerException | TException e) {
          throw new PipeException("Failed to fetch data nodes", e);
        }

        ALL_REGION_ID_SET_LAST_UPDATE_TIME.set(System.currentTimeMillis());

        LOGGER.info(
            "Fetched data region ids {} at {}",
            ALL_REGION_ID_SET,
            ALL_REGION_ID_SET_LAST_UPDATE_TIME.get());
      }
    }

    final Set<Integer> pipeRelatedRegionIdSet =
        new HashSet<>(PipeAgent.task().getPipeTaskRegionIdSet(pipeName, creationTime));
    pipeRelatedRegionIdSet.removeIf(regionId -> !ALL_REGION_ID_SET.contains(regionId));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Pipe (pipeName={}, creationTime={} related region ids {}",
          pipeName,
          creationTime,
          pipeRelatedRegionIdSet);
    }
    return pipeRelatedRegionIdSet;
  }

  private synchronized void updateExpectedRegionIdSet(Set<Integer> newRegionIdSet) {
    expectedRegionIdSet.clear();
    expectedRegionIdSet.addAll(newRegionIdSet);
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
