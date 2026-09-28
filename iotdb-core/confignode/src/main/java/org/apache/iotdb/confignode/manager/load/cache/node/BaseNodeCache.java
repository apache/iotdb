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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * NodeCache caches the NodeHeartbeatSamples of a Node. Update and cache the current statistics of
 * the Node based on the latest NodeHeartbeatSample.
 */
public abstract class BaseNodeCache extends AbstractLoadCache {

  protected final int nodeId;

  private BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusPersister;

  /** Constructor for NodeCache with default NodeStatistics. */
  protected BaseNodeCache(int nodeId) {
    super();
    this.nodeId = nodeId;
    this.currentStatistics.set(NodeStatistics.generateDefaultNodeStatistics());
  }

  public int getNodeId() {
    return nodeId;
  }

  /** Initialize a new cache before adding it to LoadCache's nodeCacheMap. */
  public void initPersistence(
      NodeStatus persistedStatus,
      BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusPersister) {
    this.nodeStatusPersister = nodeStatusPersister;
    if (persistedStatus != null) {
      currentStatistics.set(
          new NodeStatistics(System.nanoTime(), persistedStatus, null, Long.MAX_VALUE));
    }
  }

  public TSStatus updateNodeStatistics() {
    synchronized (slidingWindow) {
      return applyNodeStatistics(calculateCurrentStatistics(), false);
    }
  }

  /**
   * Try to set the requested status and record a sample for later statistics refreshes. Transition
   * rules may retain the previous status even when this method returns success. A persistence
   * failure is returned without discarding the new statistics.
   */
  public TSStatus trySetNodeStatus(NodeStatus status, boolean force) {
    synchronized (slidingWindow) {
      NodeHeartbeatSample sample = new NodeHeartbeatSample(status);
      cacheHeartbeatSample(sample);
      return applyNodeStatistics(
          new NodeStatistics(
              sample.getSampleLogicalTimestamp(),
              status,
              null,
              NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE),
          force);
    }
  }

  private TSStatus applyNodeStatistics(NodeStatistics newStats, boolean force) {
    newStats = NodeStatistics.transition((NodeStatistics) currentStatistics.get(), newStats, force);
    TSStatus result =
        nodeStatusPersister == null
            ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
            : nodeStatusPersister.apply(this, newStats.getStatus());
    currentStatistics.set(newStats);
    return result;
  }

  /** Called with the slidingWindow lock held through calculation, persistence and publication. */
  protected NodeStatistics calculateCurrentStatistics() {
    NodeStatus status;
    String statusReason = null;
    long currentNanoTime = System.nanoTime();
    NodeHeartbeatSample lastSample = (NodeHeartbeatSample) getLastSample();
    List<AbstractHeartbeatSample> heartbeatHistory = Collections.unmodifiableList(slidingWindow);
    if (lastSample == null || !failureDetector.isAvailable(nodeId, heartbeatHistory)) {
      status = NodeStatus.Unknown;
    } else {
      status = lastSample.getStatus();
      statusReason = lastSample.getStatusReason();
    }
    return new NodeStatistics(
        currentNanoTime,
        status,
        statusReason,
        NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE);
  }

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public long getLoadScore() {
    return ((NodeStatistics) currentStatistics.get()).getLoadScore();
  }

  /**
   * @return The current status of the Node.
   */
  public NodeStatus getNodeStatus() {
    return ((NodeStatistics) currentStatistics.get()).getStatus();
  }

  /**
   * @return The reason why lead to current NodeStatus.
   */
  public String getNodeStatusWithReason() {
    NodeStatistics statistics = (NodeStatistics) this.currentStatistics.get();
    return statistics.getStatusReason() == null
        ? statistics.getStatus().getStatus()
        : statistics.getStatus().getStatus() + "(" + statistics.getStatusReason() + ")";
  }
}
