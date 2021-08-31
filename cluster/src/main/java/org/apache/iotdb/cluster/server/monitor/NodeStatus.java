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

package org.apache.iotdb.cluster.server.monitor;

import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;

import java.util.Objects;

/** NodeStatus contains the last-known spec and load of a node in the cluster. */
@SuppressWarnings("java:S1135")
public class NodeStatus implements Comparable<NodeStatus> {

  // if a node is deactivated lastly too long ago, it is also assumed activated because it may
  // have restarted but there is no heartbeat between the two nodes, and the local node cannot
  // know the fact that it is normal again. Notice that we cannot always rely on the start-up
  // hello, because it only occurs once and may be lost.
  private static final long DEACTIVATION_VALID_INTERVAL_MS = 600_000L;

  private TNodeStatus status;
  // when is the status last updated, millisecond timestamp, to judge whether we should update
  // the status or not
  private long lastUpdateTime;
  // how long does it take to get the status in the last attempt, in nanoseconds, which partially
  // reflect the node's load or network condition
  private long lastResponseLatency;

  // if a node is judged down by heartbeats or other attempts to connect, isActivated will be set
  // to false, so further attempts to get clients of this node will fail without a timeout, but
  // getting clients for heartbeat will not fail so the node can be activated as soon as it is up
  // again. Clients of associated nodes should take the responsibility to activate or deactivate
  // the node.
  private volatile boolean isActivated = true;

  // if there is no heartbeat between the local node and this node, when this node is marked
  // deactivated, it cannot be reactivated in a normal way. So we also consider it reactivated if
  // its lastDeactivatedTime is too old.
  private long lastDeactivatedTime;

  // TODO-Cluster: decide what should be contained in NodeStatus and how two compare two NodeStatus
  @Override
  public int compareTo(NodeStatus o) {
    return Long.compare(this.lastResponseLatency, o.lastResponseLatency);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeStatus that = (NodeStatus) o;
    return lastUpdateTime == that.lastUpdateTime
        && lastResponseLatency == that.lastResponseLatency
        && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, lastUpdateTime, lastResponseLatency);
  }

  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public long getLastResponseLatency() {
    return lastResponseLatency;
  }

  public TNodeStatus getStatus() {
    return status;
  }

  public void setStatus(TNodeStatus status) {
    this.status = status;
  }

  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public void setLastResponseLatency(long lastResponseLatency) {
    this.lastResponseLatency = lastResponseLatency;
  }

  public void activate() {
    isActivated = true;
  }

  public void deactivate() {
    isActivated = false;
    lastDeactivatedTime = System.currentTimeMillis();
  }

  public boolean isActivated() {
    return isActivated
        || (System.currentTimeMillis() - lastDeactivatedTime) > DEACTIVATION_VALID_INTERVAL_MS;
  }
}
