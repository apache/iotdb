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

package org.apache.iotdb.confignode.consensus.request.read.ainode;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetAINodeConfigurationPlan extends ConfigPhysicalPlan {

  // if aiNodeId is set to -1, return all AINode configurations.
  private int aiNodeId;

  public GetAINodeConfigurationPlan() {
    super(ConfigPhysicalPlanType.GetAINodeConfiguration);
  }

  public GetAINodeConfigurationPlan(int aiNodeId) {
    this();
    this.aiNodeId = aiNodeId;
  }

  public int getAiNodeId() {
    return aiNodeId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(aiNodeId);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.aiNodeId = buffer.getInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetAINodeConfigurationPlan)) {
      return false;
    }
    GetAINodeConfigurationPlan that = (GetAINodeConfigurationPlan) o;
    return aiNodeId == that.aiNodeId;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(aiNodeId);
  }
}
