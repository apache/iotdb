/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query.manage;

import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;

/**
 * NodeStatus contains the last-known spec and load of a node in the cluster.
 */
public class NodeStatus implements Comparable<NodeStatus> {

  private TNodeStatus status;
  // when is the status last updated, millisecond timestamp, to judge whether we should update
  // the status or not
  private long lastUpdateTime;
  // how long does it take to get the status in the last attempt, in nanoseconds, which partially
  // reflect the node's load or network condition
  private long lastResponseTime;

  //TODO-Cluster: decide what should be contained in NodeStatus and how two compare two NodeStatus
  @Override
  public int compareTo(NodeStatus o) {
    return Long.compare(this.lastResponseTime, o.lastResponseTime);
  }

  long getLastUpdateTime() {
    return lastUpdateTime;
  }

  long getLastResponseTime() {
    return lastResponseTime;
  }

  TNodeStatus getStatus() {
    return status;
  }

  void setStatus(TNodeStatus status) {
    this.status = status;
  }

  void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  void setLastResponseTime(long lastResponseTime) {
    this.lastResponseTime = lastResponseTime;
  }
}
