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

import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Pipe operation includes CREATE, PRE_START, START, PRE_STOP, STOP, PRE_DROP and DROP. */
public class OperatePipePlan extends ConfigPhysicalPlan {
  private String pipeName;
  private SyncOperation operation;

  public OperatePipePlan() {
    super(ConfigPhysicalPlanType.OperatePipe);
  }

  public OperatePipePlan(String pipeName, SyncOperation operation) {
    this();
    this.pipeName = pipeName;
    this.operation = operation;
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public SyncOperation getOperation() {
    return operation;
  }

  public void setOperation(SyncOperation operation) {
    this.operation = operation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.OperatePipe.ordinal());
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write((byte) operation.ordinal(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeName = ReadWriteIOUtils.readString(buffer);
    operation = SyncOperation.values()[ReadWriteIOUtils.readByte(buffer)];
  }
}
