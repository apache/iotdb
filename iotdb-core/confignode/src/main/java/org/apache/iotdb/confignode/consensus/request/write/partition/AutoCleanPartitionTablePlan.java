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

package org.apache.iotdb.confignode.consensus.request.write.partition;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class AutoCleanPartitionTablePlan extends ConfigPhysicalPlan {

  Map<String, Long> databaseTTLMap;
  TTimePartitionSlot currentTimeSlot;

  public AutoCleanPartitionTablePlan() {
    super(ConfigPhysicalPlanType.AutoCleanPartitionTable);
  }

  public AutoCleanPartitionTablePlan(
      Map<String, Long> databaseTTLMap, TTimePartitionSlot currentTimeSlot) {
    this();
    this.databaseTTLMap = databaseTTLMap;
    this.currentTimeSlot = currentTimeSlot;
  }

  public Map<String, Long> getDatabaseTTLMap() {
    return databaseTTLMap;
  }

  public TTimePartitionSlot getCurrentTimeSlot() {
    return currentTimeSlot;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(databaseTTLMap.size());
    for (Map.Entry<String, Long> entry : databaseTTLMap.entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey(), stream);
      stream.writeLong(entry.getValue());
    }
    ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(currentTimeSlot, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    databaseTTLMap = new TreeMap<>();
    for (int i = 0; i < size; i++) {
      String key = BasicStructureSerDeUtil.readString(buffer);
      long value = buffer.getLong();
      databaseTTLMap.put(key, value);
    }
    currentTimeSlot = ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AutoCleanPartitionTablePlan that = (AutoCleanPartitionTablePlan) o;
    return Objects.equals(databaseTTLMap, that.databaseTTLMap)
        && Objects.equals(currentTimeSlot, that.currentTimeSlot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseTTLMap, currentTimeSlot);
  }
}
