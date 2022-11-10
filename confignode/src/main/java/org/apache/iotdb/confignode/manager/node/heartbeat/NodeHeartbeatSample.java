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
package org.apache.iotdb.confignode.manager.node.heartbeat;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TLoadSample;

public class NodeHeartbeatSample {

  // Unit: ms
  private final long sendTimestamp;
  private final long receiveTimestamp;

  private NodeStatus status;
  private String statusReason;

  private short cpuUsageRate;
  private double memoryUsageRate;
  private double diskUsageRate;

  /** Constructor for ConfigNode sample */
  public NodeHeartbeatSample(long sendTimestamp, long receiveTimestamp) {
    this.sendTimestamp = sendTimestamp;
    this.receiveTimestamp = receiveTimestamp;
  }

  /** Constructor for DataNode sample */
  public NodeHeartbeatSample(THeartbeatResp heartbeatResp, long receiveTimestamp) {
    this.sendTimestamp = heartbeatResp.getHeartbeatTimestamp();
    this.receiveTimestamp = receiveTimestamp;

    this.status = NodeStatus.parse(heartbeatResp.getStatus());
    this.statusReason = heartbeatResp.isSetStatusReason() ? heartbeatResp.getStatusReason() : null;

    if (heartbeatResp.isSetLoadSample()) {
      TLoadSample loadSample = heartbeatResp.getLoadSample();
      this.cpuUsageRate = loadSample.getCpuUsageRate();
      this.memoryUsageRate = loadSample.getMemoryUsageRate();
      this.diskUsageRate = loadSample.getDiskUsageRate();
    }
  }

  public long getSendTimestamp() {
    return sendTimestamp;
  }

  public long getReceiveTimestamp() {
    return receiveTimestamp;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public String getStatusReason() {
    return statusReason;
  }

  public short getCpuUsageRate() {
    return cpuUsageRate;
  }

  public double getMemoryUsageRate() {
    return memoryUsageRate;
  }

  public double getDiskUsageRate() {
    return diskUsageRate;
  }
}
