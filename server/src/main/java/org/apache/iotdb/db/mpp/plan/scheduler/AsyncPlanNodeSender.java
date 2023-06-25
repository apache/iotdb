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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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

  private final Map<TEndPoint, BatchRequestWithIndex> batchRequests;
  private final Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap;
  private final AtomicLong pendingNumber;
  private final long startSendTime;

  public AsyncPlanNodeSender(
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager,
      List<FragmentInstance> instances) {
    this.startSendTime = System.nanoTime();
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
    this.instances = instances;
    this.batchRequests = new HashMap<>();
    for (int i = 0; i < instances.size(); i++) {
      this.batchRequests
          .computeIfAbsent(
              instances.get(i).getHostDataNode().getInternalEndPoint(),
              x -> new BatchRequestWithIndex())
          .addSinglePlanNodeReq(
              i,
              new TSendSinglePlanNodeReq(
                  new TPlanNode(
                      instances.get(i).getFragment().getPlanNodeTree().serializeToByteBuffer()),
                  instances.get(i).getRegionReplicaSet().getRegionId()));
    }
    this.instanceId2RespMap = new ConcurrentHashMap<>(instances.size() + 1, 1);
    this.pendingNumber = new AtomicLong(batchRequests.keySet().size());
  }

  public void sendAll() {
    for (Map.Entry<TEndPoint, BatchRequestWithIndex> entry : batchRequests.entrySet()) {
      AsyncSendPlanNodeHandler handler =
          new AsyncSendPlanNodeHandler(
              entry.getValue().getIndexes(), pendingNumber, instanceId2RespMap, startSendTime);
      try {
        AsyncDataNodeInternalServiceClient client =
            asyncInternalServiceClientManager.borrowClient(entry.getKey());
        client.sendBatchPlanNode(entry.getValue().getBatchRequest(), handler);
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

  public List<TSStatus> getFailureStatusList() {
    List<TSStatus> failureStatusList = new ArrayList<>();
    TSStatus status;
    for (Map.Entry<Integer, TSendSinglePlanNodeResp> entry : instanceId2RespMap.entrySet()) {
      status = entry.getValue().getStatus();
      if (!entry.getValue().accepted) {
        if (status == null) {
          logger.warn(
              "dispatch write failed. message: {}, node {}",
              entry.getValue().message,
              instances.get(entry.getKey()).getHostDataNode().getInternalEndPoint());
          failureStatusList.add(
              RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_ERROR, entry.getValue().getMessage()));
        } else {
          logger.warn(
              "dispatch write failed. status: {}, code: {}, message: {}, node {}",
              entry.getValue().status,
              TSStatusCode.representOf(status.code),
              entry.getValue().message,
              instances.get(entry.getKey()).getHostDataNode().getInternalEndPoint());
          failureStatusList.add(status);
        }
      } else {
        // some expected and accepted status except SUCCESS_STATUS need to be returned
        if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failureStatusList.add(status);
        }
      }

      if (status != null
          && status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        int instanceIndex = entry.getKey();
        FragmentInstance fragmentInstance = instances.get(instanceIndex);
        fragmentInstance.getExecutorType().updatePreferredLocation(status.getRedirectNode());
      }
    }
    return failureStatusList;
  }

  public Future<FragInstanceDispatchResult> getResult() {
    for (Map.Entry<Integer, TSendSinglePlanNodeResp> entry : instanceId2RespMap.entrySet()) {
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

  /**
   * This class is used to aggregate PlanNode of the same datanode into one rpc. In order to ensure
   * the one-to-one correspondence between response and request, the corresponding index needs to be
   * recorded.
   */
  static class BatchRequestWithIndex {

    private final List<Integer> indexes = new ArrayList<>();
    private final TSendBatchPlanNodeReq batchRequest = new TSendBatchPlanNodeReq();

    void addSinglePlanNodeReq(int index, TSendSinglePlanNodeReq singleRequest) {
      indexes.add(index);
      batchRequest.addToRequests(singleRequest);
    }

    public List<Integer> getIndexes() {
      return indexes;
    }

    public TSendBatchPlanNodeReq getBatchRequest() {
      return batchRequest;
    }
  }
}
