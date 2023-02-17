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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;

public class StorageGroupEntityMNode extends EntityMNode implements IStorageGroupMNode {
  /**
   * when the data file in a database is older than dataTTL, it is considered invalid and will be
   * eventually deleted.
   */
  private long dataTTL;

  public StorageGroupEntityMNode(IMNode parent, String name, long dataTTL) {
    super(parent, name);
    this.dataTTL = dataTTL;
  }

  @Override
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath().intern();
    }
    return fullPath;
  }

  @Override
  public long getDataTTL() {
    return dataTTL;
  }

  @Override
  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  @Override
  public void setSchemaReplicationFactor(int schemaReplicationFactor) {}

  @Override
  public void setDataReplicationFactor(int dataReplicationFactor) {}

  @Override
  public void setTimePartitionInterval(long timePartitionInterval) {}

  @Override
  public void setStorageGroupSchema(TDatabaseSchema schema) {}

  @Override
  public TDatabaseSchema getStorageGroupSchema() {
    return null;
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    super.moveDataToNewMNode(newMNode);
  }

  @Override
  public boolean isStorageGroup() {
    return true;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.STORAGE_GROUP;
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitStorageGroupEntityMNode(this, context);
  }
}
