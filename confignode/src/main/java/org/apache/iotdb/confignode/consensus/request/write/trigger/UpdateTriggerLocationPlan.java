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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UpdateTriggerLocationPlan extends ConfigPhysicalPlan {

  private String triggerName;
  private TDataNodeLocation dataNodeLocation;

  public UpdateTriggerLocationPlan() {
    super(ConfigPhysicalPlanType.UpdateTriggerLocation);
  }

  public UpdateTriggerLocationPlan(String triggerName, TDataNodeLocation dataNodeLocation) {
    super(ConfigPhysicalPlanType.UpdateTriggerLocation);
    this.triggerName = triggerName;
    this.dataNodeLocation = dataNodeLocation;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  public TDataNodeLocation getDataNodeLocation() {
    return dataNodeLocation;
  }

  public void setDataNodeLocation(TDataNodeLocation dataNodeLocation) {
    this.dataNodeLocation = dataNodeLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(triggerName, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNodeLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.triggerName = ReadWriteIOUtils.readString(buffer);
    this.dataNodeLocation = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
  }
}
