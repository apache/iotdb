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

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DropPipePlanV2 extends ConfigPhysicalPlan {

  private String pipeName;
  private boolean isTableModel;
  private boolean isTableModelSet;

  public DropPipePlanV2() {
    super(ConfigPhysicalPlanType.DropPipeV2);
  }

  public DropPipePlanV2(String pipeName) {
    super(ConfigPhysicalPlanType.DropPipeV2);
    this.pipeName = pipeName;
  }

  public DropPipePlanV2(String pipeName, boolean isTableModel) {
    super(ConfigPhysicalPlanType.DropPipeV2);
    this.pipeName = pipeName;
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean isTableModel() {
    return isTableModel;
  }

  public boolean isTableModelSet() {
    return isTableModelSet;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    BasicStructureSerDeUtil.write(pipeName, stream);
    if (isTableModelSet) {
      stream.writeBoolean(isTableModel);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeName = BasicStructureSerDeUtil.readString(buffer);
    deserializeIsTableModel(buffer.hasRemaining(), buffer);
  }

  void deserializeImpl(final ByteBuffer buffer, final boolean isLastSubPlan) throws IOException {
    pipeName = BasicStructureSerDeUtil.readString(buffer);
    deserializeIsTableModel(
        OperateMultiplePipesPlanV2.hasTrailingFieldInSubPlan(buffer, isLastSubPlan), buffer);
  }

  private void deserializeIsTableModel(final boolean hasIsTableModel, final ByteBuffer buffer) {
    isTableModelSet = hasIsTableModel;
    if (hasIsTableModel) {
      isTableModel = buffer.get() != 0;
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
    DropPipePlanV2 that = (DropPipePlanV2) obj;
    return isTableModel == that.isTableModel
        && isTableModelSet == that.isTableModelSet
        && pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, isTableModel, isTableModelSet);
  }

  @Override
  public String toString() {
    return "DropPipePlanV2{" + "pipeName='" + pipeName + "', isTableModel=" + isTableModel + "'}";
  }
}
