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

package org.apache.iotdb.db.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.StepTracker;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogRes;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.db.consensus.statemachine.visitor.DataExecutionVisitor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.snapshot.SnapshotLoader;
import org.apache.iotdb.db.engine.snapshot.SnapshotTaker;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class DataRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(DataRegionStateMachine.class);

  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  private DataRegion region;

  private static final int MAX_REQUEST_CACHE_SIZE = 5;
  private static final long CACHE_WINDOW_TIME_IN_MS = 10_000;
  private final PriorityQueue<InsertNodeWrapper> requestCache;

  public DataRegionStateMachine(DataRegion region) {
    this.region = region;
    this.requestCache = new PriorityQueue<>();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    try {
      return new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
    } catch (Exception e) {
      logger.error(
          "Exception occurs when taking snapshot for {}-{} in {}",
          region.getLogicalStorageGroupName(),
          region.getDataRegionId(),
          snapshotDir,
          e);
      return false;
    }
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    DataRegion newRegion =
        new SnapshotLoader(
                latestSnapshotRootDir.getAbsolutePath(),
                region.getLogicalStorageGroupName(),
                region.getDataRegionId())
            .loadSnapshotForStateMachine();
    if (newRegion == null) {
      logger.error("Fail to load snapshot from {}", latestSnapshotRootDir);
      return;
    }
    this.region = newRegion;
    try {
      StorageEngineV2.getInstance()
          .setDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionId())), region);
    } catch (Exception e) {
      logger.error("Exception occurs when replacing data region in storage engine.", e);
    }
  }

  private InsertNodeWrapper cacheAndGetLatestInsertNode(
      long syncIndex,
      List<InsertNode> insertNodes,
      AsyncMethodCallback<TSyncLogRes> resultHandler) {
    synchronized (requestCache) {
      requestCache.add(new InsertNodeWrapper(syncIndex, insertNodes, resultHandler));
      if (requestCache.size() == MAX_REQUEST_CACHE_SIZE) {
        return requestCache.poll();
      }
      return null;
    }
  }

  private static class InsertNodeWrapper implements Comparable<InsertNodeWrapper> {
    private final long syncIndex;
    private final List<InsertNode> insertNodes;
    private final AsyncMethodCallback<TSyncLogRes> resultHandler;

    public InsertNodeWrapper(
        long syncIndex,
        List<InsertNode> insertNode,
        AsyncMethodCallback<TSyncLogRes> resultHandler) {
      this.syncIndex = syncIndex;
      this.insertNodes = insertNode;
      this.resultHandler = resultHandler;
    }

    @Override
    public int compareTo(@NotNull InsertNodeWrapper o) {
      return Long.compare(syncIndex, o.syncIndex);
    }

    public long getSyncIndex() {
      return syncIndex;
    }

    public List<InsertNode> getInsertNodes() {
      return insertNodes;
    }

    public AsyncMethodCallback<TSyncLogRes> getResultHandler() {
      return resultHandler;
    }
  }

  public void multiLeaderWriteAsync(
      List<IndexedConsensusRequest> requests, AsyncMethodCallback<TSyncLogRes> resultHandler) {
    List<TSStatus> statuses = new LinkedList<>();
    try {
      List<InsertNode> insertNodesInAllRequests = new LinkedList<>();
      for (IndexedConsensusRequest indexedRequest : requests) {
        List<InsertNode> insertNodesInOneRequest =
            new ArrayList<>(indexedRequest.getRequests().size());
        for (IConsensusRequest req : indexedRequest.getRequests()) {
          // PlanNode in IndexedConsensusRequest should always be InsertNode
          InsertNode innerNode = (InsertNode) getPlanNode(req);
          innerNode.setSearchIndex(indexedRequest.getSearchIndex());
          insertNodesInOneRequest.add(innerNode);
        }
        insertNodesInAllRequests.add(mergeInsertNodes(insertNodesInOneRequest));
      }
      long startTime = System.nanoTime();
      InsertNodeWrapper insertNodeWrapper =
          cacheAndGetLatestInsertNode(
              requests.get(0).getSyncIndex(), insertNodesInAllRequests, resultHandler);
      StepTracker.trace("cacheAndGet", startTime, System.nanoTime());
      if (insertNodeWrapper != null) {
        //        for (InsertNode insertNode : insertNodeWrapper.getInsertNodes()) {
        //          statuses.add(write(insertNode));
        //        }
        insertNodeWrapper.resultHandler.onComplete(new TSyncLogRes(statuses));
      }
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      resultHandler.onError(e);
    }
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    PlanNode planNode;
    try {
      if (request instanceof IndexedConsensusRequest) {
        IndexedConsensusRequest indexedRequest = (IndexedConsensusRequest) request;
        List<InsertNode> insertNodes = new ArrayList<>(indexedRequest.getRequests().size());
        for (IConsensusRequest req : indexedRequest.getRequests()) {
          // PlanNode in IndexedConsensusRequest should always be InsertNode
          InsertNode innerNode = (InsertNode) getPlanNode(req);
          innerNode.setSearchIndex(indexedRequest.getSearchIndex());
          insertNodes.add(innerNode);
        }
        planNode = mergeInsertNodes(insertNodes);
      } else {
        planNode = getPlanNode(request);
      }
      return write(planNode);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  /**
   * Merge insert nodes sharing same search index ( e.g. tablet-100, tablet-100, tablet-100 will be
   * merged to one multi-tablet). <br>
   * Notice: the continuity of insert nodes sharing same search index should be protected by the
   * upper layer.
   */
  private InsertNode mergeInsertNodes(List<InsertNode> insertNodes) {
    int size = insertNodes.size();
    if (size == 0) {
      throw new RuntimeException();
    }
    if (size == 1) {
      return insertNodes.get(0);
    }

    InsertNode result;
    if (insertNodes.get(0) instanceof InsertTabletNode) { // merge to InsertMultiTabletsNode
      List<Integer> index = new ArrayList<>(size);
      List<InsertTabletNode> insertTabletNodes = new ArrayList<>(size);
      int i = 0;
      for (InsertNode insertNode : insertNodes) {
        insertTabletNodes.add((InsertTabletNode) insertNode);
        index.add(i);
        i++;
      }
      result =
          new InsertMultiTabletsNode(insertNodes.get(0).getPlanNodeId(), index, insertTabletNodes);
    } else { // merge to InsertRowsNode or InsertRowsOfOneDeviceNode
      boolean sameDevice = true;
      PartialPath device = insertNodes.get(0).getDevicePath();
      List<Integer> index = new ArrayList<>(size);
      List<InsertRowNode> insertRowNodes = new ArrayList<>(size);
      int i = 0;
      for (InsertNode insertNode : insertNodes) {
        if (sameDevice && !insertNode.getDevicePath().equals(device)) {
          sameDevice = false;
        }
        insertRowNodes.add((InsertRowNode) insertNode);
        index.add(i);
        i++;
      }
      result =
          sameDevice
              ? new InsertRowsOfOneDeviceNode(
                  insertNodes.get(0).getPlanNodeId(), index, insertRowNodes)
              : new InsertRowsNode(insertNodes.get(0).getPlanNodeId(), index, insertRowNodes);
    }
    result.setSearchIndex(insertNodes.get(0).getSearchIndex());
    result.setDevicePath(insertNodes.get(0).getDevicePath());
    return result;
  }

  protected TSStatus write(PlanNode planNode) {
    long startTime = System.nanoTime();
    try {
      return planNode.accept(new DataExecutionVisitor(), region);
    } finally {
      StepTracker.trace("StateMachineWrite", startTime, System.nanoTime());
    }
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    if (request instanceof GetConsensusReqReaderPlan) {
      return region.getWALNode();
    } else {
      FragmentInstance fragmentInstance;
      try {
        fragmentInstance = getFragmentInstance(request);
      } catch (IllegalArgumentException e) {
        logger.error(e.getMessage());
        return null;
      }
      return QUERY_INSTANCE_MANAGER.execDataQueryFragmentInstance(fragmentInstance, region);
    }
  }
}
