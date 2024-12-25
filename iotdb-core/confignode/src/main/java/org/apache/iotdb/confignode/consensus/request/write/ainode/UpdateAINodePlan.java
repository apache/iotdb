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

package org.apache.iotdb.confignode.consensus.request.write.ainode;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateAINodePlan extends ConfigPhysicalPlan {

  private TAINodeConfiguration aiNodeConfiguration;

  public UpdateAINodePlan() {
    super(ConfigPhysicalPlanType.UpdateAINodeConfiguration);
  }

  public UpdateAINodePlan(TAINodeConfiguration aiNodeConfiguration) {
    this();
    this.aiNodeConfiguration = aiNodeConfiguration;
  }

  public TAINodeConfiguration getAINodeConfiguration() {
    return aiNodeConfiguration;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTAINodeConfiguration(aiNodeConfiguration, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    aiNodeConfiguration = ThriftCommonsSerDeUtils.deserializeTAINodeConfiguration(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!getType().equals(((UpdateAINodePlan) o).getType())) {
      return false;
    }
    UpdateAINodePlan that = (UpdateAINodePlan) o;
    return aiNodeConfiguration.equals(that.aiNodeConfiguration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), aiNodeConfiguration);
  }
}
