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

package org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaUtils;

import org.rocksdb.RocksDBException;

public class RStorageGroupMNode extends RInternalMNode implements IStorageGroupMNode {

  private long dataTTL;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RStorageGroupMNode(
      String fullPath, long dataTTL, RSchemaReadWriteHandler readWriteHandler) {
    super(fullPath, readWriteHandler);
    this.dataTTL = dataTTL;
  }

  public RStorageGroupMNode(
      String fullPath, byte[] value, RSchemaReadWriteHandler readWriteHandler) {
    super(fullPath, readWriteHandler);
    Object ttl = RSchemaUtils.parseNodeValue(value, RMNodeValueType.TTL);
    if (ttl == null) {
      ttl = CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs();
    }
    this.dataTTL = (long) ttl;
  }

  @Override
  void updateChildNode(String childName, int childNameMaxLevel) throws MetadataException {
    String innerName =
        RSchemaUtils.convertPartialPathToInner(
            childName, childNameMaxLevel, RMNodeType.STORAGE_GROUP.getValue());
    long ttl = getDataTTL();
    try {
      readWriteHandler.updateNode(
          innerName.getBytes(), RSchemaUtils.updateTTL(RSchemaConstants.DEFAULT_NODE_VALUE, ttl));
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public boolean isStorageGroup() {
    return true;
  }

  @Override
  public boolean isEntity() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.STORAGE_GROUP;
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
}
