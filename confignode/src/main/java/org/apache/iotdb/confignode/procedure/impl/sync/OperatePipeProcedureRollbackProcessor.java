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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.BaseNodeCache;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class OperatePipeProcedureRollbackProcessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OperatePipeProcedureRollbackProcessor.class);
  private static final int TIME_INTERVAL = 5_000;

  private final NodeManager nodeManager;

  private final Map<Integer, Queue<TOperatePipeOnDataNodeReq>> messageMap =
      new ConcurrentHashMap<>();
  private volatile ScheduledFuture<?> promise;
  private ScheduledFuture<?> canceller;
  private final ScheduledExecutorService executorService =
      IoTDBThreadPoolFactory.newScheduledThreadPool(2, "OperatePipeProcedureRollback");

  public OperatePipeProcedureRollbackProcessor(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public void retryRollbackReq(List<Integer> dataNodeIds, TOperatePipeOnDataNodeReq req) {
    for (int id : dataNodeIds) {
      messageMap.computeIfAbsent(id, i -> new LinkedList<>()).add(req);
    }
    if (canceller != null && !canceller.isDone()) {
      canceller.cancel(true);
    }
    if (promise == null || promise.isDone()) {
      promise =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              executorService, this::rollback, TIME_INTERVAL, TIME_INTERVAL, TimeUnit.MILLISECONDS);
    }
    canceller =
        executorService.schedule(
            () -> {
              LOGGER.info("Cancel rollback retry thread.");
              promise.cancel(false);
            },
            BaseNodeCache.HEARTBEAT_TIMEOUT_TIME,
            TimeUnit.MILLISECONDS);
  }

  private void rollback() {
    LOGGER.info("Scheduled OperatePipeProcedureRollbackProcessor.");
    for (Map.Entry<Integer, Queue<TOperatePipeOnDataNodeReq>> entry : messageMap.entrySet()) {
      int dataNodeId = entry.getKey();
      if (NodeStatus.Running.equals(nodeManager.getNodeStatusByNodeId(dataNodeId))) {
        final Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
        dataNodeLocationMap.put(
            dataNodeId, nodeManager.getRegisteredDataNodeLocations().get(dataNodeId));
        TOperatePipeOnDataNodeReq request;
        while ((request = entry.getValue().peek()) != null) {
          AsyncClientHandler<TOperatePipeOnDataNodeReq, TSStatus> clientHandler =
              new AsyncClientHandler<>(
                  DataNodeRequestType.ROLLBACK_OPERATE_PIPE, request, dataNodeLocationMap);
          AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
          TSStatus tsStatus = clientHandler.getResponseList().get(0);
          if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            entry.getValue().poll();
          } else if (tsStatus.getCode() == TSStatusCode.PIPE_ERROR.getStatusCode()) {
            // skip
            LOGGER.warn(
                String.format(
                    "Roll back failed because %s. Skip this roll back request [%s].",
                    tsStatus.getMessage(), request));
          } else {
            // connection failure, keep and retry.
            LOGGER.error(
                String.format(
                    "Roll back failed because %s. This roll back request [%s] will be retried later.",
                    tsStatus.getMessage(), request));
            break;
          }
        }
      }
    }
  }
}
