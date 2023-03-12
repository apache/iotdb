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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class AsyncPlanNodeSender {
  private static final Logger logger = LoggerFactory.getLogger(AsyncPlanNodeSender.class);
  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;
  private final List<FragmentInstance> instances;
  private final Map<Integer, TSendPlanNodeResp> instanceId2RespMap;
  private final AtomicLong pendingNumber;

  public AsyncPlanNodeSender(
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager,
      List<FragmentInstance> instances) {
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
    this.instances = instances;
    this.instanceId2RespMap = new ConcurrentHashMap<>();
    this.pendingNumber = new AtomicLong(instances.size());
  }

  public void sendAll() {
    long startSendTime = System.nanoTime();
    for (int i = 0; i < instances.size(); ++i) {
      FragmentInstance instance = instances.get(i);
      AsyncSendPlanNodeHandler handler =
          new AsyncSendPlanNodeHandler(i, pendingNumber, instanceId2RespMap, startSendTime);
      try {
        TSendPlanNodeReq sendPlanNodeReq =
            new TSendPlanNodeReq(
                new TPlanNode(instance.getFragment().getPlanNodeTree().serializeToByteBuffer()),
                instance.getRegionReplicaSet().getRegionId());
        AsyncDataNodeInternalServiceClient client =
            asyncInternalServiceClientManager.borrowClient(
                instance.getHostDataNode().getInternalEndPoint());
        client.sendPlanNode(sendPlanNodeReq, handler);
      } catch (Exception e) {
        handler.onError(e);
      }
    }
  }

  public void waitUntilCompleted() throws InterruptedException {
    synchronized (pendingNumber) {
      while (pendingNumber.get() != 0) {
        pendingNumber.wait();
      }
    }
  }

  public Future<FragInstanceDispatchResult> getResult() {
    for (Map.Entry<Integer, TSendPlanNodeResp> entry : instanceId2RespMap.entrySet()) {
      if (!entry.getValue().accepted) {
        logger.warn(
            "dispatch write failed. status: {}, code: {}, message: {}, node {}",
            entry.getValue().status,
            TSStatusCode.representOf(entry.getValue().status.code),
            entry.getValue().message,
            instances.get(entry.getKey()).getHostDataNode().getInternalEndPoint());
        if (entry.getValue().getStatus() == null) {
          return immediateFuture(
              new FragInstanceDispatchResult(
                  RpcUtils.getStatus(
                      TSStatusCode.WRITE_PROCESS_ERROR, entry.getValue().getMessage())));
        } else {
          return immediateFuture(new FragInstanceDispatchResult(entry.getValue().getStatus()));
        }
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }
}
