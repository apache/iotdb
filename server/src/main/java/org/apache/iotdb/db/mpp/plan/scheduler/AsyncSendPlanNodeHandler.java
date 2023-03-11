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

import org.apache.iotdb.db.mpp.metric.PerformanceOverviewMetricsManager;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSendPlanNodeHandler implements AsyncMethodCallback<TSendPlanNodeResp> {
  private final int instanceId;
  private final AtomicLong pendingNumber;
  private final Map<Integer, TSendPlanNodeResp> instanceId2RespMap;
  private final long sendTime;

  public AsyncSendPlanNodeHandler(
      int instanceId,
      AtomicLong pendingNumber,
      Map<Integer, TSendPlanNodeResp> instanceId2RespMap,
      long sendTime) {
    this.instanceId = instanceId;
    this.pendingNumber = pendingNumber;
    this.instanceId2RespMap = instanceId2RespMap;
    this.sendTime = sendTime;
  }

  @Override
  public void onComplete(TSendPlanNodeResp tSendPlanNodeResp) {
    instanceId2RespMap.put(instanceId, tSendPlanNodeResp);
    if (pendingNumber.decrementAndGet() == 0) {
      PerformanceOverviewMetricsManager.recordScheduleRemoteCost(System.nanoTime() - sendTime);
      synchronized (pendingNumber) {
        pendingNumber.notifyAll();
      }
    }
  }

  @Override
  public void onError(Exception e) {
    TSendPlanNodeResp resp = new TSendPlanNodeResp();
    String errorMsg = String.format("Fail to send plan node, exception message: %s", e);
    resp.setAccepted(false);
    resp.setMessage(errorMsg);
    resp.setStatus(
        RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg));
    instanceId2RespMap.put(instanceId, resp);
    if (pendingNumber.decrementAndGet() == 0) {
      PerformanceOverviewMetricsManager.recordScheduleRemoteCost(System.nanoTime() - sendTime);
      synchronized (pendingNumber) {
        pendingNumber.notifyAll();
      }
    }
  }
}
