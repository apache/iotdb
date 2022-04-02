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
package org.apache.iotdb.confignode.physical.sys;

import org.apache.iotdb.confignode.partition.DataRegionInfo;
import org.apache.iotdb.confignode.partition.SchemaRegionInfo;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;

import java.nio.ByteBuffer;

public class SetStorageGroupPlan extends PhysicalPlan {

  private StorageGroupSchema schema;

  private SchemaRegionInfo schemaRegionInfo;

  private DataRegionInfo dataRegionInfo;

  public SetStorageGroupPlan() {
    super(PhysicalPlanType.SetStorageGroup);
    this.schema = new StorageGroupSchema();
  }

  public SetStorageGroupPlan(StorageGroupSchema schema) {
    this();
    this.schema = schema;
  }

  public StorageGroupSchema getSchema() {
    return schema;
  }

  public void setSchema(StorageGroupSchema schema) {
    this.schema = schema;
  }

  public SchemaRegionInfo getSchemaRegionInfo() {
    return schemaRegionInfo;
  }

  public void setSchemaRegionInfo(SchemaRegionInfo schemaRegionInfo) {
    this.schemaRegionInfo = schemaRegionInfo;
  }

  public DataRegionInfo getDataRegionInfo() {
    return dataRegionInfo;
  }

  public void setDataRegionInfo(DataRegionInfo dataRegionInfo) {
    this.dataRegionInfo = dataRegionInfo;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.SetStorageGroup.ordinal());
    schema.serialize(buffer);
    schemaRegionInfo.serializeImpl(buffer);
    dataRegionInfo.serializeImpl(buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    schema.deserialize(buffer);
    schemaRegionInfo.deserializeImpl(buffer);
    dataRegionInfo.deserializeImpl(buffer);
  }
}
