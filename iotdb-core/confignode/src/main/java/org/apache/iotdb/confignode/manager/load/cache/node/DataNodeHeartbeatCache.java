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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.mpp.rpc.thrift.TLoadSample;

import java.util.concurrent.atomic.AtomicReference;

/** {@link DataNodeHeartbeatCache} caches and maintains all the heartbeat data. */
public class DataNodeHeartbeatCache extends BaseNodeCache {

  private final AtomicReference<TLoadSample> latestLoadSample;

  /** Constructor for create DataNodeHeartbeatCache with default NodeStatistics. */
  public DataNodeHeartbeatCache(int dataNodeId) {
    super(dataNodeId);
    this.latestLoadSample = new AtomicReference<>(new TLoadSample());
  }

  @Override
  protected void updateCurrentStatistics() {
    NodeHeartbeatSample lastSample = null;
    synchronized (slidingWindow) {
      if (!slidingWindow.isEmpty()) {
        lastSample = slidingWindow.getLast();
      }
    }
    long lastSendTime = lastSample == null ? 0 : lastSample.getSendTimestamp();

    /* Update load sample */
    if (lastSample != null && lastSample.isSetLoadSample()) {
      latestLoadSample.set(lastSample.getLoadSample());
    }

    /* Update Node status */
    NodeStatus status = null;
    String statusReason = null;
    // TODO: Optimize judge logic
    if (System.currentTimeMillis() - lastSendTime > HEARTBEAT_TIMEOUT_TIME) {
      status = NodeStatus.Unknown;
    } else if (lastSample != null) {
      status = lastSample.getStatus();
      statusReason = lastSample.getStatusReason();
    }

    /* Update loadScore */
    // Only consider Running DataNode as available currently
    // TODO: Construct load score module
    long loadScore = NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE;

    NodeStatistics newStatistics = new NodeStatistics(loadScore, status, statusReason);
    if (!currentStatistics.get().equals(newStatistics)) {
      // Update the current NodeStatistics if necessary
      currentStatistics.set(newStatistics);
    }
  }

  public double getFreeDiskSpace() {
    return latestLoadSample.get().getFreeDiskSpace();
  }
}
