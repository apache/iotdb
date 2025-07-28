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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncPlanNodeSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPlanNodeSender.class);
  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;
  private final List<FragmentInstance> instances;

  private final Map<TEndPoint, BatchRequestWithIndex> batchRequests;
  private final Map<Integer, TSendSinglePlanNodeResp> instanceId2RespMap;

  private final List<Integer> needRetryInstanceIndex;

  private final AtomicLong pendingNumber;
  private long startSendTime;

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
    this.needRetryInstanceIndex = Collections.synchronizedList(new ArrayList<>());
    this.pendingNumber = new AtomicLong(batchRequests.keySet().size());
  }

  public void sendAll() {
    for (Map.Entry<TEndPoint, BatchRequestWithIndex> entry : batchRequests.entrySet()) {
      AsyncSendPlanNodeHandler handler =
          new AsyncSendPlanNodeHandler(
              entry.getValue().getIndexes(),
              pendingNumber,
              instanceId2RespMap,
              needRetryInstanceIndex,
              startSendTime);
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

  public List<FailedFragmentInstanceWithStatus> getFailedInstancesWithStatuses() {
    List<FailedFragmentInstanceWithStatus> failureFragmentInstanceWithStatusList =
        new ArrayList<>();
    TSStatus status;
    for (Map.Entry<Integer, TSendSinglePlanNodeResp> entry : instanceId2RespMap.entrySet()) {
      status = entry.getValue().getStatus();
      final FragmentInstance instance = instances.get(entry.getKey());
      if (!entry.getValue().accepted) {
        if (status == null) {
          LOGGER.warn(
              "dispatch write failed. message: {}, node {}",
              entry.getValue().message,
              instances.get(entry.getKey()).getHostDataNode().getInternalEndPoint());
          failureFragmentInstanceWithStatusList.add(
              new FailedFragmentInstanceWithStatus(
                  instance,
                  RpcUtils.getStatus(
                      TSStatusCode.WRITE_PROCESS_ERROR, entry.getValue().getMessage())));
        } else {
          LOGGER.warn(
              "dispatch write failed. status: {}, code: {}, message: {}, node {}",
              entry.getValue().status,
              TSStatusCode.representOf(status.code),
              entry.getValue().message,
              instances.get(entry.getKey()).getHostDataNode().getInternalEndPoint());
          failureFragmentInstanceWithStatusList.add(
              new FailedFragmentInstanceWithStatus(instance, status));
        }
      } else {
        // some expected and accepted status except SUCCESS_STATUS need to be returned
        if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failureFragmentInstanceWithStatusList.add(
              new FailedFragmentInstanceWithStatus(instance, status));
        }
      }
    }
    return failureFragmentInstanceWithStatusList;
  }

  public boolean needRetry() {
    // retried FI list is not empty and data region replica number is greater than 1
    return !needRetryInstanceIndex.isEmpty()
        && instances.get(0).getRegionReplicaSet().dataNodeLocations.size() > 1;
  }

  /**
   * This function should be called after all last batch responses were received. This function will
   * do the cleaning work and caller won't need to do the cleaning work outside this function. We
   * will retry all failed FIs in last batch whose id were all saved in needRetryInstanceIds, if
   * there are still failed FIs this time, they will be also saved in needRetryInstanceIds.
   *
   * <p>It's a sync function which means that once this function returned, the results of this retry
   * have been received, and you don't need to call waitUntilCompleted.
   */
  public void retry() throws InterruptedException {
    // 1. rebuild the batchRequests using remaining failed FIs, change the replica for each failed
    // FI in this step
    batchRequests.clear();
    for (int fragmentInstanceIndex : needRetryInstanceIndex) {
      this.batchRequests
          .computeIfAbsent(
              instances
                  .get(fragmentInstanceIndex)
                  .getNextRetriedHostDataNode()
                  .getInternalEndPoint(),
              x -> new BatchRequestWithIndex())
          .addSinglePlanNodeReq(
              fragmentInstanceIndex,
              new TSendSinglePlanNodeReq(
                  new TPlanNode(
                      instances
                          .get(fragmentInstanceIndex)
                          .getFragment()
                          .getPlanNodeTree()
                          .serializeToByteBuffer()),
                  instances.get(fragmentInstanceIndex).getRegionReplicaSet().getRegionId()));
    }

    // 2. reset the pendingNumber, needRetryInstanceIds and startSendTime
    needRetryInstanceIndex.clear();
    pendingNumber.set(batchRequests.keySet().size());
    startSendTime = System.nanoTime();

    // 3. call sendAll() to retry
    sendAll();

    // 4. call waitUntilCompleted() to wait for the responses
    waitUntilCompleted();
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
