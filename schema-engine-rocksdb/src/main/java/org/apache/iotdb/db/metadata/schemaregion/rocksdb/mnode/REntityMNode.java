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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Map;

public class REntityMNode extends RInternalMNode implements IEntityMNode {

  private volatile boolean isAligned = false;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public REntityMNode(String fullPath, RSchemaReadWriteHandler readWriteHandler) {
    super(fullPath, readWriteHandler);
  }

  @Override
  void updateChildNode(String childName, int childNameMaxLevel) throws MetadataException {
    String innerName =
        RSchemaUtils.convertPartialPathToInner(
            childName, childNameMaxLevel, RMNodeType.ENTITY.getValue());
    try {
      readWriteHandler.updateNode(innerName.getBytes(), new byte[] {});
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  public REntityMNode(String fullPath, byte[] value, RSchemaReadWriteHandler readWriteHandler) {
    super(fullPath, readWriteHandler);
    deserialize(value);
  }

  @Override
  public boolean addAlias(String alias, IMeasurementMNode child) {
    // In rocksdb-based mode, there is no need to update in memory
    return true;
  }

  @Override
  public void deleteAliasChild(String alias) {
    // Don't do any update in the MNode related class, it won't persistent in memory or on disk
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, IMeasurementMNode> getAliasChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPreDeactivateTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void preDeactivateTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackPreDeactivateTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deactivateTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean isAligned) {
    this.isAligned = isAligned;
  }

  private void deserialize(byte[] value) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    // skip the version flag and node type flag
    byte flag = ReadWriteIOUtils.readBytes(byteBuffer, 2)[1];
    isAligned = (RSchemaConstants.FLAG_IS_ALIGNED & flag) > 0;
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.DEVICE;
  }
}
