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

public class SetPipeStatusWithStoppedByRuntimeExceptionPlanV2 extends ConfigPhysicalPlan {

  private String pipeName;
  private PipeStatus status;
  private boolean stoppedByRuntimeException;

  public SetPipeStatusWithStoppedByRuntimeExceptionPlanV2() {
    super(ConfigPhysicalPlanType.SetPipeStatusWithStoppedByRuntimeExceptionV2);
  }

  public SetPipeStatusWithStoppedByRuntimeExceptionPlanV2(
      final String pipeName, final PipeStatus status, final boolean stoppedByRuntimeException) {
    super(ConfigPhysicalPlanType.SetPipeStatusWithStoppedByRuntimeExceptionV2);
    this.pipeName = pipeName;
    this.status = status;
    this.stoppedByRuntimeException = stoppedByRuntimeException;
  }

  public String getPipeName() {
    return pipeName;
  }

  public PipeStatus getPipeStatus() {
    return status;
  }

  public boolean isStoppedByRuntimeException() {
    return stoppedByRuntimeException;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write(status.getType(), stream);
    ReadWriteIOUtils.write(stoppedByRuntimeException, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    pipeName = ReadWriteIOUtils.readString(buffer);
    status = PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(buffer));
    stoppedByRuntimeException = ReadWriteIOUtils.readBool(buffer);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final SetPipeStatusWithStoppedByRuntimeExceptionPlanV2 that =
        (SetPipeStatusWithStoppedByRuntimeExceptionPlanV2) obj;
    return stoppedByRuntimeException == that.stoppedByRuntimeException
        && pipeName.equals(that.pipeName)
        && status.equals(that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, status, stoppedByRuntimeException);
  }

  @Override
  public String toString() {
    return "SetPipeStatusWithStoppedByRuntimeExceptionPlanV2{"
        + "pipeName='"
        + pipeName
        + "', status='"
        + status
        + "', stoppedByRuntimeException='"
        + stoppedByRuntimeException
        + "'}";
  }
}
