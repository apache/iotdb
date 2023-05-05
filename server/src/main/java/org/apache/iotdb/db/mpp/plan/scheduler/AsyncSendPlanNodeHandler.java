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
package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.commons.service.metric.enums.PerformanceOverviewMetrics;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSendPlanNodeHandler implements AsyncMethodCallback<TSendBatchPlanNodeResp> {

  private final List<Integer> instanceIds;
  private final AtomicLong pendingNumber;
  private final Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap;
  private final long sendTime;
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  public AsyncSendPlanNodeHandler(
      List<Integer> instanceIds,
      AtomicLong pendingNumber,
      Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap,
      long sendTime) {
    this.instanceIds = instanceIds;
    this.pendingNumber = pendingNumber;
    this.instanceId2RespMap = instanceId2RespMap;
    this.sendTime = sendTime;
  }

  @Override
  public void onComplete(TSendBatchPlanNodeResp tSendPlanNodeResp) {
    for (int i = 0; i < tSendPlanNodeResp.getResponses().size(); i++) {
      instanceId2RespMap.put(instanceIds.get(i), tSendPlanNodeResp.getResponses().get(i));
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
}
