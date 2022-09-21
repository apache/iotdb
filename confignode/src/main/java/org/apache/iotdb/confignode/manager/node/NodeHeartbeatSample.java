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
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;

public class NodeHeartbeatSample {

  // Unit: ms
  private final long sendTimestamp;
  private final long receiveTimestamp;

  private NodeStatus status;
  private String statusReason;

  private short cpuOccupancyRatio;
  private double memoryOccupancyRatio;
  private double diskOccupancyRatio;

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

    this.cpuOccupancyRatio = heartbeatResp.getLoadSample().getCpuOccupancyRatio();
    this.memoryOccupancyRatio = heartbeatResp.getLoadSample().getMemoryOccupancyRatio();
    this.diskOccupancyRatio = heartbeatResp.getLoadSample().getDiskOccupancyRatio();
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

  public short getCpuOccupancyRatio() {
    return cpuOccupancyRatio;
  }

  public double getMemoryOccupancyRatio() {
    return memoryOccupancyRatio;
  }

  public double getDiskOccupancyRatio() {
    return diskOccupancyRatio;
  }
}
