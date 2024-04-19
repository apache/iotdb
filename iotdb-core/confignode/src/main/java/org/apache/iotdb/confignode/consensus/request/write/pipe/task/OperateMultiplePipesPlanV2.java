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

package org.apache.iotdb.confignode.consensus.request.write.pipe.task;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OperateMultiplePipesPlanV2 extends ConfigPhysicalPlan {

  private List<ConfigPhysicalPlan> subPlans;

  public OperateMultiplePipesPlanV2() {
    super(ConfigPhysicalPlanType.OperateMultiplePipesV2);
  }

  public OperateMultiplePipesPlanV2(List<ConfigPhysicalPlan> subPlans) {
    super(ConfigPhysicalPlanType.OperateMultiplePipesV2);
    this.subPlans = subPlans;
  }

  public List<ConfigPhysicalPlan> getSubPlans() {
    return subPlans;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    if (subPlans != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(subPlans.size(), stream);
      for (ConfigPhysicalPlan subPlan : subPlans) {
        if (subPlan instanceof CreatePipePlanV2) {
          ((CreatePipePlanV2) subPlan).serializeImpl(stream);
        } else if (subPlan instanceof DropPipePlanV2) {
          ((DropPipePlanV2) subPlan).serializeImpl(stream);
        } else if (subPlan instanceof AlterPipePlanV2) {
          ((AlterPipePlanV2) subPlan).serializeImpl(stream);
        } else if (subPlan instanceof SetPipeStatusPlanV2) {
          ((SetPipeStatusPlanV2) subPlan).serializeImpl(stream);
        } else {
          throw new IOException("Unsupported sub plan type: " + subPlan.getClass().getName());
        }
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    if (ReadWriteIOUtils.readBool(buffer)) {
      subPlans = new ArrayList<>();
      int size = ReadWriteIOUtils.readInt(buffer);
      for (int i = 0; i < size; i++) {
        short type = ReadWriteIOUtils.readShort(buffer);
        if (type == ConfigPhysicalPlanType.CreatePipeV2.getPlanType()) {
          CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2();
          createPipePlanV2.deserializeImpl(buffer);
          subPlans.add(createPipePlanV2);
        } else if (type == ConfigPhysicalPlanType.DropPipeV2.getPlanType()) {
          DropPipePlanV2 dropPipePlanV2 = new DropPipePlanV2();
          dropPipePlanV2.deserializeImpl(buffer);
          subPlans.add(dropPipePlanV2);
        } else if (type == ConfigPhysicalPlanType.AlterPipeV2.getPlanType()) {
          AlterPipePlanV2 alterPipePlanV2 = new AlterPipePlanV2();
          alterPipePlanV2.deserializeImpl(buffer);
          subPlans.add(alterPipePlanV2);
        } else if (type == ConfigPhysicalPlanType.SetPipeStatusV2.getPlanType()) {
          SetPipeStatusPlanV2 setPipeStatusPlanV2 = new SetPipeStatusPlanV2();
          setPipeStatusPlanV2.deserializeImpl(buffer);
          subPlans.add(setPipeStatusPlanV2);
        } else {
          throw new IOException("Unsupported sub plan type: " + type);
        }
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    OperateMultiplePipesPlanV2 that = (OperateMultiplePipesPlanV2) obj;
    return Objects.equals(this.subPlans, that.subPlans);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subPlans);
  }

  @Override
  public String toString() {
    return "OperateMultiplePipesPlanV2{" + "subPlans='" + subPlans + "'}";
  }
}
