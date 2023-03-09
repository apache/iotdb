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
package org.apache.iotdb.db.metadata.mnode.mem.info;

import org.apache.iotdb.commons.schema.node.info.IDatabaseInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

public class DatabaseInfo implements IDatabaseInfo {

  private TDatabaseSchema schema;

  public DatabaseInfo(String name) {
    this.schema = new TDatabaseSchema(name);
  }

  @Override
  public void moveDataToNewMNode(IDatabaseMNode<?> newMNode) {
    newMNode.setStorageGroupSchema(schema);
  }

  @Override
  public long getDataTTL() {
    return schema.getTTL();
  }

  @Override
  public void setDataTTL(long dataTTL) {
    schema.setTTL(dataTTL);
  }

  @Override
  public void setSchemaReplicationFactor(int schemaReplicationFactor) {
    schema.setSchemaReplicationFactor(schemaReplicationFactor);
  }

  @Override
  public void setDataReplicationFactor(int dataReplicationFactor) {
    schema.setDataReplicationFactor(dataReplicationFactor);
  }

  @Override
  public void setTimePartitionInterval(long timePartitionInterval) {
    schema.setTimePartitionInterval(timePartitionInterval);
  }

  @Override
  public void setStorageGroupSchema(TDatabaseSchema schema) {
    this.schema = schema;
  }

  @Override
  public TDatabaseSchema getStorageGroupSchema() {
    return schema;
  }

  /**
   * The memory occupied by an DatabaseDeviceInfo based occupation
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>reference schema, 8B
   *   <li>object TDatabaseSchema, 112B (calculated by RamUsageEstimator)
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + 112;
  }
}
