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

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TLoadSample;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

/** NodeHeartbeatSample records the heartbeat sample of a Node. */
public class NodeHeartbeatSample extends AbstractHeartbeatSample {

  // Node status and the status's reason
  private final NodeStatus status;
  private final String statusReason;
  // The Node hardware's load sample
  private final TLoadSample loadSample;

  /** Constructor for generating default sample with specified status */
  public NodeHeartbeatSample(NodeStatus status) {
    super(System.nanoTime());
    this.status = status;
    this.statusReason = null;
    this.loadSample = null;
  }

  /** Constructor for generating default sample with specified status and timestamp */
  public NodeHeartbeatSample(long sampleNanoTimestamp, NodeStatus status) {
    super(sampleNanoTimestamp);
    this.status = status;
    this.statusReason = null;
    this.loadSample = null;
  }

  /** Constructor for DataNode sample. */
  public NodeHeartbeatSample(TDataNodeHeartbeatResp heartbeatResp) {
    super(heartbeatResp.getHeartbeatTimestamp());
    this.status = NodeStatus.parse(heartbeatResp.getStatus());
    this.statusReason = heartbeatResp.isSetStatusReason() ? heartbeatResp.getStatusReason() : null;
    this.loadSample = heartbeatResp.isSetLoadSample() ? heartbeatResp.getLoadSample() : null;
  }

  /** Constructor for AINode sample. */
  public NodeHeartbeatSample(TAIHeartbeatResp heartbeatResp) {
    super(heartbeatResp.getHeartbeatTimestamp());
    this.status = NodeStatus.parse(heartbeatResp.getStatus());
    this.statusReason = heartbeatResp.isSetStatusReason() ? heartbeatResp.getStatusReason() : null;
    if (heartbeatResp.isSetLoadSample()) {
      this.loadSample = heartbeatResp.getLoadSample();
    } else {
      this.loadSample = null;
    }
  }

  /** Constructor for ConfigNode sample. */
  public NodeHeartbeatSample(TConfigNodeHeartbeatResp heartbeatResp) {
    super(heartbeatResp.getTimestamp());
    this.status = NodeStatus.Running;
    this.statusReason = null;
    this.loadSample = null;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public String getStatusReason() {
    return statusReason;
  }

  public boolean isSetLoadSample() {
    return loadSample != null;
  }

  public TLoadSample getLoadSample() {
    return loadSample;
  }
}
