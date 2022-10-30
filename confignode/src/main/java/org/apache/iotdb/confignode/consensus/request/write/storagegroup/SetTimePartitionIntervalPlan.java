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
package org.apache.iotdb.confignode.consensus.request.write.storagegroup;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SetTimePartitionIntervalPlan extends ConfigPhysicalPlan {

  private String storageGroup;

  private long timePartitionInterval;

  public SetTimePartitionIntervalPlan() {
    super(ConfigPhysicalPlanType.SetTimePartitionInterval);
  }

  public SetTimePartitionIntervalPlan(String storageGroup, long timePartitionInterval) {
    this();
    this.storageGroup = storageGroup;
    this.timePartitionInterval = timePartitionInterval;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    BasicStructureSerDeUtil.write(storageGroup, stream);
    stream.writeLong(timePartitionInterval);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    storageGroup = BasicStructureSerDeUtil.readString(buffer);
    timePartitionInterval = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetTimePartitionIntervalPlan that = (SetTimePartitionIntervalPlan) o;
    return timePartitionInterval == that.timePartitionInterval
        && storageGroup.equals(that.storageGroup);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroup, timePartitionInterval);
  }
}
