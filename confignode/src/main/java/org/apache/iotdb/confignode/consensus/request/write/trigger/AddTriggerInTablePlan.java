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

import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AddTriggerInTablePlan extends ConfigPhysicalPlan {

  private TriggerInformation triggerInformation;
  private Binary jarFile;

  public AddTriggerInTablePlan() {
    super(ConfigPhysicalPlanType.AddTriggerInTable);
  }

  public AddTriggerInTablePlan(TriggerInformation triggerInformation, Binary jarFile) {
    super(ConfigPhysicalPlanType.AddTriggerInTable);
    this.triggerInformation = triggerInformation;
    this.jarFile = jarFile;
  }

  public TriggerInformation getTriggerInformation() {
    return triggerInformation;
  }

  public void setTriggerInformation(TriggerInformation triggerInformation) {
    this.triggerInformation = triggerInformation;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  public void setJarFile(Binary jarFile) {
    this.jarFile = jarFile;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    triggerInformation.serialize(stream);
    if (jarFile == null) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    triggerInformation = TriggerInformation.deserialize(buffer);
    if (ReadWriteIOUtils.readBool(buffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(buffer);
  }
}
