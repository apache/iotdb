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

package org.apache.iotdb.confignode.consensus.response.model;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.nio.ByteBuffer;

public class GetModelInfoResp implements DataSet {

  private final TSStatus status;
  private ByteBuffer serializedModelInformation;

  private int targetAINodeId;
  private TEndPoint targetAINodeAddress;

  public TSStatus getStatus() {
    return status;
  }

  public GetModelInfoResp(TSStatus status) {
    this.status = status;
  }

  public void setModelInfo(ByteBuffer serializedModelInformation) {
    this.serializedModelInformation = serializedModelInformation;
  }

  public int getTargetAINodeId() {
    return targetAINodeId;
  }

  public void setTargetAINodeId(int targetAINodeId) {
    this.targetAINodeId = targetAINodeId;
  }

  public void setTargetAINodeAddress(TAINodeConfiguration aiNodeConfiguration) {
    if (aiNodeConfiguration.getLocation() == null) {
      return;
    }
    this.targetAINodeAddress = aiNodeConfiguration.getLocation().getInternalEndPoint();
  }

  public TGetModelInfoResp convertToThriftResponse() {
    TGetModelInfoResp resp = new TGetModelInfoResp(status);
    resp.setModelInfo(serializedModelInformation);
    resp.setAiNodeAddress(targetAINodeAddress);
    return resp;
  }
}
