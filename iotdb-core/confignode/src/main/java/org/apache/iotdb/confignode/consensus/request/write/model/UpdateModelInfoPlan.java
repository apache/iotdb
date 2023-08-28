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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class UpdateModelInfoPlan extends ConfigPhysicalPlan {

  private String modelId;
  private String trailId;
  private Map<String, String> modelInfo;

  public UpdateModelInfoPlan() {
    super(ConfigPhysicalPlanType.UpdateModelInfo);
  }

  public UpdateModelInfoPlan(TUpdateModelInfoReq updateModelInfoReq) {
    super(ConfigPhysicalPlanType.UpdateModelInfo);
    this.modelId = updateModelInfoReq.getModelId();
    this.trailId = updateModelInfoReq.getTrailId();
    this.modelInfo = updateModelInfoReq.getModelInfo();
  }

  public String getModelId() {
    return modelId;
  }

  public String getTrailId() {
    return trailId;
  }

  public Map<String, String> getModelInfo() {
    return modelInfo;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(trailId, stream);
    ReadWriteIOUtils.write(modelInfo, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(buffer);
    this.trailId = ReadWriteIOUtils.readString(buffer);
    this.modelInfo = ReadWriteIOUtils.readMap(buffer);
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
    return modelId.equals(that.modelId)
        && trailId.equals(that.trailId)
        && modelInfo.equals(that.modelInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelId, trailId, modelInfo);
  }
}
