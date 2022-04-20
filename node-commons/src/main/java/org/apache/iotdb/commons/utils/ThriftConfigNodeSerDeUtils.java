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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftConfigNodeSerDeUtils {

  private ThriftConfigNodeSerDeUtils() {
    // Empty constructor
  }

  public static void writeTStorageGroupSchema(
      TStorageGroupSchema storageGroupSchema, ByteBuffer buffer) {
    BasicStructureSerializeDeserializeUtil.write(storageGroupSchema.getName(), buffer);
    buffer.putLong(storageGroupSchema.getTTL());
    buffer.putInt(storageGroupSchema.getSchemaReplicationFactor());
    buffer.putInt(storageGroupSchema.getDataReplicationFactor());
    buffer.putLong(storageGroupSchema.getTimePartitionInterval());

    buffer.putInt(storageGroupSchema.getSchemaRegionGroupIdsSize());
    if (storageGroupSchema.getSchemaRegionGroupIdsSize() > 0) {
      storageGroupSchema
          .getSchemaRegionGroupIds()
          .forEach(
              schemaRegionGroupId ->
                  ThriftCommonsSerDeUtils.writeTConsensusGroupId(
                      schemaRegionGroupId, buffer));
    }

    buffer.putInt(storageGroupSchema.getDataRegionGroupIdsSize());
    if (storageGroupSchema.getDataRegionGroupIdsSize() > 0) {
      storageGroupSchema
          .getDataRegionGroupIds()
          .forEach(
              dataRegionGroupId ->
                  ThriftCommonsSerDeUtils.writeTConsensusGroupId(
                      dataRegionGroupId, buffer));
    }
  }

  public static TStorageGroupSchema readTStorageGroupSchema(ByteBuffer buffer) {
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    storageGroupSchema.setName(BasicStructureSerializeDeserializeUtil.readString(buffer));
    storageGroupSchema.setTTL(buffer.getLong());
    storageGroupSchema.setSchemaReplicationFactor(buffer.getInt());
    storageGroupSchema.setDataReplicationFactor(buffer.getInt());
    storageGroupSchema.setTimePartitionInterval(buffer.getLong());

    int groupIdNum = buffer.getInt();
    storageGroupSchema.setSchemaRegionGroupIds(new ArrayList<>());
    for (int i = 0; i < groupIdNum; i++) {
      storageGroupSchema
          .getSchemaRegionGroupIds()
          .add(ThriftCommonsSerDeUtils.readTConsensusGroupId(buffer));
    }

    groupIdNum = buffer.getInt();
    storageGroupSchema.setDataRegionGroupIds(new ArrayList<>());
    for (int i = 0; i < groupIdNum; i++) {
      storageGroupSchema
          .getDataRegionGroupIds()
          .add(ThriftCommonsSerDeUtils.readTConsensusGroupId(buffer));
    }

    return storageGroupSchema;
  }
}
