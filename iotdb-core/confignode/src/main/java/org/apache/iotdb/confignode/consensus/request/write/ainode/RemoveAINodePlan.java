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

import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveAINodePlan extends ConfigPhysicalPlan {

  private TAINodeLocation aiNodeLocation;

  public RemoveAINodePlan() {
    super(ConfigPhysicalPlanType.RemoveAINode);
  }

  public RemoveAINodePlan(TAINodeLocation taiNodeLocation) {
    this();
    this.aiNodeLocation = taiNodeLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTAINodeLocation(aiNodeLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.aiNodeLocation = ThriftCommonsSerDeUtils.deserializeTAINodeLocation(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RemoveAINodePlan that = (RemoveAINodePlan) o;
    return aiNodeLocation.equals(that.aiNodeLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aiNodeLocation);
  }

  public TAINodeLocation getAINodeLocation() {
    return aiNodeLocation;
  }
}
