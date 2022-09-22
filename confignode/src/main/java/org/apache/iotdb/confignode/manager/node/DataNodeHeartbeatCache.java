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
package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.commons.cluster.NodeStatus;

/** DataNodeHeartbeatCache caches and maintains all the heartbeat data */
public class DataNodeHeartbeatCache extends BaseNodeCache {

  /** For guiding queries, the higher the score the higher the load */
  private volatile long loadScore;

  public DataNodeHeartbeatCache() {
    this.loadScore = 0;
  }

  @Override
  public void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential HeartbeatSamples are accepted.
      // And un-sequential HeartbeatSamples will be discarded.
      if (slidingWindow.size() == 0
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.removeFirst();
      }
    }
  }

  @Override
  public boolean updateNodeStatus() {
    NodeHeartbeatSample lastSample = null;
    synchronized (slidingWindow) {
      if (slidingWindow.size() > 0) {
        lastSample = slidingWindow.getLast();
      }
    }
    long lastSendTime = lastSample == null ? 0 : lastSample.getSendTimestamp();

    /* Update loadScore */
    loadScore = -lastSendTime;

    /* Update Node status */
    String originStatus = status.getStatus();
    // TODO: Optimize judge logic
    if (System.currentTimeMillis() - lastSendTime > HEARTBEAT_TIMEOUT_TIME) {
      status = NodeStatus.Unknown;
    } else if (lastSample != null) {
      status = lastSample.getStatus();
    }

    return NodeStatus.isNormalStatus(status)
        != NodeStatus.isNormalStatus(NodeStatus.parse(originStatus));
  }

  @Override
  public long getLoadScore() {
    // The DataNode whose status is abnormal will get the highest loadScore
    return NodeStatus.isNormalStatus(status) ? loadScore : Long.MAX_VALUE;
  }
}
