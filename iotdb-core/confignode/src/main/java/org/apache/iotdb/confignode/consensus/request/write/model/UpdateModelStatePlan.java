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

import org.apache.iotdb.common.rpc.thrift.TrainingState;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateModelStatePlan extends ConfigPhysicalPlan {

  private String modelId;
  private TrainingState state;
  private String bestTrailId;

  public UpdateModelStatePlan() {
    super(ConfigPhysicalPlanType.UpdateModelState);
  }

  public UpdateModelStatePlan(TUpdateModelStateReq updateModelStateReq) {
    super(ConfigPhysicalPlanType.UpdateModelState);
    this.modelId = updateModelStateReq.getModelId();
    this.state = updateModelStateReq.getState();
    this.bestTrailId = updateModelStateReq.getBestTrailId();
  }

  public String getModelId() {
    return modelId;
  }

  public TrainingState getState() {
    return state;
  }

  public String getBestTrailId() {
    return bestTrailId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(state.getValue(), stream);
    boolean isNull = bestTrailId == null;
    ReadWriteIOUtils.write(isNull, stream);
    if (!isNull) {
      ReadWriteIOUtils.write(bestTrailId, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(buffer);
    this.state = TrainingState.findByValue(ReadWriteIOUtils.readInt(buffer));
    boolean isNull = ReadWriteIOUtils.readBool(buffer);
    if (!isNull) {
      this.bestTrailId = ReadWriteIOUtils.readString(buffer);
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
    UpdateModelStatePlan that = (UpdateModelStatePlan) o;
    return modelId.equals(that.modelId)
        && state == that.state
        && Objects.equals(bestTrailId, that.bestTrailId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelId, state, bestTrailId);
  }
}
