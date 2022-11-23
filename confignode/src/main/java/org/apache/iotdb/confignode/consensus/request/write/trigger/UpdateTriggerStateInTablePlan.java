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

package org.apache.iotdb.confignode.consensus.request.write.trigger;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UpdateTriggerStateInTablePlan extends ConfigPhysicalPlan {

  private String triggerName;
  private TTriggerState triggerState;

  public UpdateTriggerStateInTablePlan() {
    super(ConfigPhysicalPlanType.UpdateTriggerStateInTable);
  }

  public UpdateTriggerStateInTablePlan(String triggerName, TTriggerState triggerState) {
    super(ConfigPhysicalPlanType.UpdateTriggerStateInTable);
    this.triggerName = triggerName;
    this.triggerState = triggerState;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  public TTriggerState getTriggerState() {
    return triggerState;
  }

  public void setTriggerState(TTriggerState triggerState) {
    this.triggerState = triggerState;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(triggerName, stream);
    ReadWriteIOUtils.write(triggerState.getValue(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    triggerName = ReadWriteIOUtils.readString(buffer);
    triggerState = TTriggerState.findByValue(ReadWriteIOUtils.readInt(buffer));
  }
}
