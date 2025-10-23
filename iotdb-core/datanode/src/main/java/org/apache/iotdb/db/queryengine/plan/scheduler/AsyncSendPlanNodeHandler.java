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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.client.ThriftClient.isConnectionBroken;

public class AsyncSendPlanNodeHandler implements AsyncMethodCallback<TSendBatchPlanNodeResp> {

  private final List<Integer> instanceIds;
  private final AtomicLong pendingNumber;
  private final Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap;
  private final List<Integer> needRetryInstanceIndex;
  private final long sendTime;
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  public AsyncSendPlanNodeHandler(
      List<Integer> instanceIds,
      AtomicLong pendingNumber,
      Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap,
      List<Integer> needRetryInstanceIndex,
      long sendTime) {
    this.instanceIds = instanceIds;
    this.pendingNumber = pendingNumber;
    this.instanceId2RespMap = instanceId2RespMap;
    this.needRetryInstanceIndex = needRetryInstanceIndex;
    this.sendTime = sendTime;
  }

  @Override
  public void onComplete(TSendBatchPlanNodeResp sendBatchPlanNodeResp) {
    for (int i = 0; i < sendBatchPlanNodeResp.getResponses().size(); i++) {
      TSendSinglePlanNodeResp singlePlanNodeResp = sendBatchPlanNodeResp.getResponses().get(i);
      instanceId2RespMap.put(instanceIds.get(i), singlePlanNodeResp);
      if (needRetry(singlePlanNodeResp)) {
        needRetryInstanceIndex.add(instanceIds.get(i));
      }
    }
    if (pendingNumber.decrementAndGet() == 0) {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleRemoteCost(System.nanoTime() - sendTime);
      synchronized (pendingNumber) {
        pendingNumber.notifyAll();
      }
    }
  }

  @Override
  public void onError(Exception e) {
    if (needRetry(e)) {
      needRetryInstanceIndex.addAll(instanceIds);
    }
    TSendSinglePlanNodeResp resp = new TSendSinglePlanNodeResp();
    String errorMsg = String.format("Fail to send plan node, exception message: %s", e);
    resp.setAccepted(false);
    resp.setMessage(errorMsg);
    resp.setStatus(
        RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg));
    instanceIds.forEach(instanceId -> instanceId2RespMap.put(instanceId, resp));
    if (pendingNumber.decrementAndGet() == 0) {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleRemoteCost(System.nanoTime() - sendTime);
      synchronized (pendingNumber) {
        pendingNumber.notifyAll();
      }
    }
  }

  public static boolean needRetry(Exception e) {
    Throwable rootCause = ExceptionUtils.getRootCause(e);
    // 1. connection broken it means that the remote node may go offline
    // 2. or the method call is time out
    return isConnectionBroken(rootCause) || (e instanceof TimeoutException);
  }

  private boolean needRetry(TSendSinglePlanNodeResp resp) {
    return !resp.accepted && resp.status != null && StatusUtils.needRetryHelper(resp.status);
  }
}
