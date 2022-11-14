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
package org.apache.iotdb.confignode.consensus.request.write.sync;

import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SetPipeStatusPlan extends ConfigPhysicalPlan {
  private String pipeName;
  private PipeStatus pipeStatus;

  public SetPipeStatusPlan() {
    super(ConfigPhysicalPlanType.SetPipeStatus);
  }

  public SetPipeStatusPlan(String pipeName, PipeStatus pipeStatus) {
    this();
    this.pipeName = pipeName;
    this.pipeStatus = pipeStatus;
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public PipeStatus getPipeStatus() {
    return pipeStatus;
  }

  public void setPipeStatus(PipeStatus pipeStatus) {
    this.pipeStatus = pipeStatus;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write((byte) pipeStatus.ordinal(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeName = ReadWriteIOUtils.readString(buffer);
    pipeStatus = PipeStatus.values()[ReadWriteIOUtils.readByte(buffer)];
  }
}
