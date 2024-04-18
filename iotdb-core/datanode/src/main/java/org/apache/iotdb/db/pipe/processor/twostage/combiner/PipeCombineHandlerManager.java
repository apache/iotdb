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

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.CombineRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultResponse;
import org.apache.iotdb.db.pipe.processor.twostage.operator.Operator;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PipeCombineHandlerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeCombineHandlerManager.class);

  private final ConcurrentMap<String, PipeCombineHandler> pipeId2CombineHandler =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicInteger> pipeId2ReferenceCount =
      new ConcurrentHashMap<>();

  public synchronized void register(
      String pipeName, long creationTime, Function<String, Operator> operatorConstructor) {
    final String pipeId = generatePipeId(pipeName, creationTime);

    pipeId2CombineHandler.putIfAbsent(
        pipeId, new PipeCombineHandler(pipeName, creationTime, operatorConstructor));
    pipeId2ReferenceCount.putIfAbsent(pipeId, new AtomicInteger(0));

    pipeId2ReferenceCount.get(pipeId).incrementAndGet();
  }

  public synchronized void deregister(String pipeName, long creationTime) {
    final String pipeId = generatePipeId(pipeName, creationTime);

    if (pipeId2ReferenceCount.containsKey(pipeId)
        && pipeId2ReferenceCount.get(pipeId).decrementAndGet() <= 0) {
      pipeId2ReferenceCount.remove(pipeId);
      try {
        pipeId2CombineHandler.remove(pipeId).close();
      } catch (Exception e) {
        LOGGER.warn("Error occurred when closing CombineHandler(id = {})", pipeId, e);
      }
    }
  }

  public TPipeTransferResp handle(CombineRequest combineRequest) {
    final String pipeId =
        generatePipeId(combineRequest.getPipeName(), combineRequest.getCreationTime());

    final PipeCombineHandler handler = pipeId2CombineHandler.get(pipeId);
    if (Objects.isNull(handler)) {
      throw new PipeException("CombineHandler not found for pipeId = " + pipeId);
    }

    handler.combine(
        combineRequest.getRegionId(), combineRequest.getCombineId(), combineRequest.getState());
    return new TPipeTransferResp().setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  public FetchCombineResultResponse handle(FetchCombineResultRequest fetchCombineResultRequest)
      throws IOException {
    final String pipeId =
        generatePipeId(
            fetchCombineResultRequest.getPipeName(), fetchCombineResultRequest.getCreationTime());

    final PipeCombineHandler handler = pipeId2CombineHandler.get(pipeId);
    if (Objects.isNull(handler)) {
      throw new PipeException("CombineHandler not found for pipeId = " + pipeId);
    }

    return handler.fetchCombineResult(fetchCombineResultRequest.getCombineIdList());
  }

  public void fetchExpectedRegionIdSetAndCleanOutdatedCombiner() {
    final Map<String, PipeCombineHandler> pipeId2CombineHandlerSnapshot;
    synchronized (this) {
      pipeId2CombineHandlerSnapshot = new HashMap<>(pipeId2CombineHandler);
    }

    pipeId2CombineHandlerSnapshot.forEach(
        (pipeId, handler) -> {
          handler.fetchAndUpdateExpectedRegionIdSet();
          handler.cleanOutdatedCombiner();
        });
  }

  private static String generatePipeId(String pipeName, long creationTime) {
    return pipeName + "-" + creationTime;
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private PipeCombineHandlerManager() {
    PipeAgent.runtime()
        .registerPeriodicalJob(
            "CombineHandlerManager#fetchExpectedRegionIdSetAndCleanOutdatedCombiner",
            this::fetchExpectedRegionIdSetAndCleanOutdatedCombiner,
            60);
  }

  private static class CombineHandlerManagerHolder {
    private static final PipeCombineHandlerManager INSTANCE = new PipeCombineHandlerManager();
  }

  public static PipeCombineHandlerManager getInstance() {
    return CombineHandlerManagerHolder.INSTANCE;
  }
}
