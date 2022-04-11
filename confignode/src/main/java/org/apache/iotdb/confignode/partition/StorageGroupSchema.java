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
package org.apache.iotdb.confignode.partition;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StorageGroupSchema {

  private String name;

  private final List<ConsensusGroupId> schemaRegionGroupIds;
  private final List<ConsensusGroupId> dataRegionGroupIds;

  public StorageGroupSchema() {
    schemaRegionGroupIds = new ArrayList<>();
    dataRegionGroupIds = new ArrayList<>();
  }

  public StorageGroupSchema(String name) {
    this();
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public List<ConsensusGroupId> getSchemaRegionGroupIds() {
    return schemaRegionGroupIds;
  }

  public void addSchemaRegionGroup(ConsensusGroupId id) {
    schemaRegionGroupIds.add(id);
  }

  public List<ConsensusGroupId> getDataRegionGroupIds() {
    return dataRegionGroupIds;
  }

  public void addDataRegionGroup(ConsensusGroupId id) {
    dataRegionGroupIds.add(id);
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(name.length());
    buffer.put(name.getBytes());

    buffer.putInt(schemaRegionGroupIds.size());
    for (ConsensusGroupId schemaRegionGroupId : schemaRegionGroupIds) {
      schemaRegionGroupId.serializeImpl(buffer);
    }

    buffer.putInt(dataRegionGroupIds.size());
    for (ConsensusGroupId dataRegionGroupId : dataRegionGroupIds) {
      dataRegionGroupId.serializeImpl(buffer);
    }
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    name = SerializeDeserializeUtil.readString(buffer);

    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ConsensusGroupId schemaRegionId = ConsensusGroupId.Factory.create(buffer);
      schemaRegionGroupIds.add(schemaRegionId);
    }

    length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ConsensusGroupId dataRegionId = ConsensusGroupId.Factory.create(buffer);
      dataRegionGroupIds.add(dataRegionId);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageGroupSchema that = (StorageGroupSchema) o;
    return name.equals(that.name)
        && schemaRegionGroupIds.equals(that.schemaRegionGroupIds)
        && dataRegionGroupIds.equals(that.dataRegionGroupIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, schemaRegionGroupIds, dataRegionGroupIds);
  }
}
