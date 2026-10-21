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

package org.apache.iotdb.commons.pipe.receiver.runtime;

public class PipeReceiverRuntimeSnapshot {

  private final String receiverNodeType;
  private final int receiverNodeId;
  private final String protocol;
  private final String senderAddress;
  private final String senderPorts;
  private final int connectionCount;
  private final int pipeCount;
  private final String pipeIds;
  private final String userName;
  private final String senderClusterId;
  private final long lastHandshakeTime;
  private final long lastTransferTime;

  public PipeReceiverRuntimeSnapshot(
      String receiverNodeType,
      int receiverNodeId,
      String protocol,
      String senderAddress,
      String senderPorts,
      int connectionCount,
      int pipeCount,
      String pipeIds,
      String userName,
      String senderClusterId,
      long lastHandshakeTime,
      long lastTransferTime) {
    this.receiverNodeType = receiverNodeType;
    this.receiverNodeId = receiverNodeId;
    this.protocol = protocol;
    this.senderAddress = senderAddress;
    this.senderPorts = senderPorts;
    this.connectionCount = connectionCount;
    this.pipeCount = pipeCount;
    this.pipeIds = pipeIds;
    this.userName = userName;
    this.senderClusterId = senderClusterId;
    this.lastHandshakeTime = lastHandshakeTime;
    this.lastTransferTime = lastTransferTime;
  }

  public String getReceiverNodeType() {
    return receiverNodeType;
  }

  public int getReceiverNodeId() {
    return receiverNodeId;
  }

  public boolean isReceiverNodeIdKnown() {
    return receiverNodeId >= 0;
  }

  public String getProtocol() {
    return protocol;
  }

  public String getSenderAddress() {
    return senderAddress;
  }

  public String getSenderPorts() {
    return senderPorts;
  }

  public int getConnectionCount() {
    return connectionCount;
  }

  public int getPipeCount() {
    return pipeCount;
  }

  public String getPipeIds() {
    return pipeIds;
  }

  public String getUserName() {
    return userName;
  }

  public String getSenderClusterId() {
    return senderClusterId;
  }

  public long getLastHandshakeTime() {
    return lastHandshakeTime;
  }

  public long getLastTransferTime() {
    return lastTransferTime;
  }
}
