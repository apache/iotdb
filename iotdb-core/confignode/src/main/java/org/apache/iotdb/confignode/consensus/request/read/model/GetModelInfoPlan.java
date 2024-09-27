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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetModelInfoPlan extends ConfigPhysicalReadPlan {

  private String modelId;

  public GetModelInfoPlan() {
    super(ConfigPhysicalPlanType.GetModelInfo);
  }

  public GetModelInfoPlan(final TGetModelInfoReq getModelInfoReq) {
    super(ConfigPhysicalPlanType.GetModelInfo);
    this.modelId = getModelInfoReq.getModelId();
  }

  public String getModelId() {
    return modelId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(modelId, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final GetModelInfoPlan that = (GetModelInfoPlan) o;
    return Objects.equals(modelId, that.modelId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelId);
  }
}
