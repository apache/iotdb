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

package org.apache.iotdb.confignode.consensus.request.write.model;

import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UpdateModelInfoPlan extends ConfigPhysicalPlan {

  private String modelName;
  private ModelInformation modelInformation;

  // The node which has the model which is only updated in model registration
  private List<Integer> nodeIds;

  public UpdateModelInfoPlan() {
    super(ConfigPhysicalPlanType.UpdateModelInfo);
  }

  public UpdateModelInfoPlan(String modelName, ModelInformation modelInformation) {
    super(ConfigPhysicalPlanType.UpdateModelInfo);
    this.modelName = modelName;
    this.modelInformation = modelInformation;
    this.nodeIds = Collections.emptyList();
  }

  public UpdateModelInfoPlan(
      String modelName, ModelInformation modelInformation, List<Integer> nodeIds) {
    super(ConfigPhysicalPlanType.UpdateModelInfo);
    this.modelName = modelName;
    this.modelInformation = modelInformation;
    this.nodeIds = nodeIds;
  }

  public String getModelName() {
    return modelName;
  }

  public ModelInformation getModelInformation() {
    return modelInformation;
  }

  public List<Integer> getNodeIds() {
    return nodeIds;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(modelName, stream);
    this.modelInformation.serialize(stream);
    ReadWriteIOUtils.write(nodeIds.size(), stream);
    for (Integer nodeId : nodeIds) {
      ReadWriteIOUtils.write(nodeId, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.modelName = ReadWriteIOUtils.readString(buffer);
    this.modelInformation = ModelInformation.deserialize(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    this.nodeIds = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      this.nodeIds.add(ReadWriteIOUtils.readInt(buffer));
    }
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
    UpdateModelInfoPlan that = (UpdateModelInfoPlan) o;
    return modelName.equals(that.modelName)
        && modelInformation.equals(that.modelInformation)
        && nodeIds.equals(that.nodeIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelName, modelInformation, nodeIds);
  }
}
