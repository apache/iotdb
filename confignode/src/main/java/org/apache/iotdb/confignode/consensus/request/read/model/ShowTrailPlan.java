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

package org.apache.iotdb.confignode.consensus.request.read.model;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ShowTrailPlan extends ConfigPhysicalPlan {

  private String modelId;
  private String trailId;

  public ShowTrailPlan() {
    super(ConfigPhysicalPlanType.ShowTrail);
  }

  public ShowTrailPlan(TShowTrailReq showTrailReq) {
    super(ConfigPhysicalPlanType.ShowTrail);
    this.modelId = showTrailReq.getModelId();
    if (showTrailReq.isSetTrailId()) {
      this.trailId = showTrailReq.getTrailId();
    }
  }

  public String getModelId() {
    return modelId;
  }

  public boolean isSetTrailId() {
    return trailId != null;
  }

  public String getTrailId() {
    return trailId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(trailId != null, stream);
    ReadWriteIOUtils.write(trailId, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(buffer);
    boolean isSetTrailId = ReadWriteIOUtils.readBool(buffer);
    if (isSetTrailId) {
      this.trailId = ReadWriteIOUtils.readString(buffer);
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
    ShowTrailPlan that = (ShowTrailPlan) o;
    return modelId.equals(that.modelId) && Objects.equals(trailId, that.trailId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelId, trailId);
  }
}
