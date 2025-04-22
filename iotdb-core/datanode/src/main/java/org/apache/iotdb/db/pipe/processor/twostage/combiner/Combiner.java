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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.processor.twostage.operator.Operator;
import org.apache.iotdb.db.pipe.processor.twostage.state.State;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Combiner {

  private static final Logger LOGGER = LoggerFactory.getLogger(Combiner.class);

  private final long creationTimeInMs;

  private final Operator operator;

  private final ConcurrentMap<Integer, Integer> expectedRegionId2DataNodeIdMap;
  private final Set<Integer> receivedRegionIdSet;

  private final AtomicBoolean isComplete = new AtomicBoolean(false);

  public Combiner(
      Operator operator, ConcurrentMap<Integer, Integer> expectedRegionId2DataNodeIdMap) {
    this.creationTimeInMs = System.currentTimeMillis();

    this.operator = operator;

    this.expectedRegionId2DataNodeIdMap = expectedRegionId2DataNodeIdMap;
    this.receivedRegionIdSet = new HashSet<>();
  }

  public TSStatus combine(int regionId, State state) {
    final Set<Integer> finalExpectedRegionIdSet =
        new HashSet<>(expectedRegionId2DataNodeIdMap.keySet());

    if (finalExpectedRegionIdSet.isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_ERROR, "Expected region id set is empty. Sender should try again.");
    }

    receivedRegionIdSet.add(regionId);
    operator.combine(state);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Combiner combine: regionId: {}, state: {}, receivedRegionIdSet: {}, expectedRegionIdSet: {}",
          regionId,
          state,
          receivedRegionIdSet,
          finalExpectedRegionIdSet);
    }

    if (receivedRegionIdSet.containsAll(finalExpectedRegionIdSet)) {
      operator.onComplete();
      isComplete.set(true);

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "Combiner combine completed: regionId: {}, state: {}, receivedRegionIdSet: {}, expectedRegionIdSet: {}",
            regionId,
            state,
            receivedRegionIdSet,
            finalExpectedRegionIdSet);
      }
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public boolean isOutdated() {
    return System.currentTimeMillis() - creationTimeInMs
        > PipeConfig.getInstance().getTwoStageAggregateMaxCombinerLiveTimeInMs();
  }

  public boolean isComplete() {
    return isComplete.get();
  }
}
