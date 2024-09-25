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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SetPipeStatusPlanV2 extends ConfigPhysicalPlan {

  private String pipeName;
  private PipeStatus status;

  public SetPipeStatusPlanV2() {
    super(ConfigPhysicalPlanType.SetPipeStatusV2);
  }

  public SetPipeStatusPlanV2(String pipeName, PipeStatus status) {
    super(ConfigPhysicalPlanType.SetPipeStatusV2);
    this.pipeName = pipeName;
    this.status = status;
  }

  public String getPipeName() {
    return pipeName;
  }

  public PipeStatus getPipeStatus() {
    return status;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write(status.getType(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeName = ReadWriteIOUtils.readString(buffer);
    status = PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(buffer));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SetPipeStatusPlanV2 that = (SetPipeStatusPlanV2) obj;
    return pipeName.equals(that.pipeName) && status.equals(that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, status);
  }

  @Override
  public String toString() {
    return "SetPipeStatusPlanV2{" + "pipeName='" + pipeName + "', status='" + status + "'}";
  }
}
