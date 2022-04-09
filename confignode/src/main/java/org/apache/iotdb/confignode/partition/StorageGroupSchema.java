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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StorageGroupSchema {

  private String name;

  private List<ConsensusGroupId> schemaRegionGroupIds;
  private List<ConsensusGroupId> dataRegionGroupIds;

  public StorageGroupSchema() {
    // empty constructor
  }

  public StorageGroupSchema(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public List<ConsensusGroupId> getSchemaRegionGroupIds() {
    return schemaRegionGroupIds;
  }

  public void addSchemaRegionGroup(ConsensusGroupId id) {
    if (schemaRegionGroupIds == null) {
      schemaRegionGroupIds = new ArrayList<>();
    }
    schemaRegionGroupIds.add(id);
  }

  public List<ConsensusGroupId> getDataRegionGroupIds() {
    return dataRegionGroupIds;
  }

  public void addDataRegionGroup(ConsensusGroupId id) {
    if (dataRegionGroupIds == null) {
      dataRegionGroupIds = new ArrayList<>();
    }
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

  public void deserialize(ByteBuffer buffer) {
    name = SerializeDeserializeUtil.readString(buffer);

    int length = buffer.getInt();
    schemaRegionGroupIds = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      SchemaRegionId schemaRegionId = new SchemaRegionId();
      schemaRegionId.deserializeImpl(buffer);
      schemaRegionGroupIds.add(schemaRegionId);
    }

    length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      DataRegionId dataRegionId = new DataRegionId();
      dataRegionId.deserializeImpl(buffer);
      dataRegionGroupIds.add(dataRegionId);
    }
  }
}
