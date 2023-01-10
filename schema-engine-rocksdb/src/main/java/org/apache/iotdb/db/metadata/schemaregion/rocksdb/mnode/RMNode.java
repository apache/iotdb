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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheEntry;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaUtils;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class RMNode implements IMNode {
  /** from root to this node, only be set when used once for InternalMNode */
  protected String fullPath;

  protected RSchemaReadWriteHandler readWriteHandler;

  protected IMNode parent;

  protected String name;

  protected static final Logger logger = LoggerFactory.getLogger(RMNode.class);

  /** Constructor of MNode. */
  public RMNode(String fullPath, RSchemaReadWriteHandler readWriteHandler) {
    this.fullPath = fullPath.intern();
    this.name = fullPath.substring(fullPath.lastIndexOf(RSchemaConstants.PATH_SEPARATOR) + 1);
    this.readWriteHandler = readWriteHandler;
  }

  abstract void updateChildNode(String childName, int childNameMaxLevel) throws MetadataException;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public IMNode getParent() {
    if (parent != null) {
      return parent;
    }
    String parentName =
        fullPath.substring(0, fullPath.lastIndexOf(RSchemaConstants.PATH_SEPARATOR));
    parent = getNodeBySpecifiedPath(parentName);
    return parent;
  }

  protected IMNode getNodeBySpecifiedPath(String keyName) {
    byte[] value = null;
    IMNode node;
    int nodeNameMaxLevel = RSchemaUtils.getLevelByPartialPath(keyName);
    for (RMNodeType type : RMNodeType.values()) {
      String parentInnerName =
          RSchemaUtils.convertPartialPathToInner(keyName, nodeNameMaxLevel, type.getValue());
      try {
        value = readWriteHandler.get(null, parentInnerName.getBytes());
      } catch (RocksDBException e) {
        logger.error("Failed to get parent node.", e);
      }
      if (value != null) {
        switch (type.getValue()) {
          case RSchemaConstants.NODE_TYPE_SG:
            node = new RStorageGroupMNode(keyName, value, readWriteHandler);
            return node;
          case RSchemaConstants.NODE_TYPE_INTERNAL:
            node = new RInternalMNode(keyName, readWriteHandler);
            return node;
          case RSchemaConstants.NODE_TYPE_ENTITY:
            node = new REntityMNode(keyName, value, readWriteHandler);
            return node;
          case RSchemaConstants.NODE_TYPE_MEASUREMENT:
            node = new RMeasurementMNode(keyName, value, readWriteHandler);
            return node;
        }
      }
    }
    return null;
  }

  @Override
  public void setParent(IMNode parent) {
    this.parent = parent;
  }

  /**
   * get partial path of this node
   *
   * @return partial path
   */
  @Override
  public PartialPath getPartialPath() {
    try {
      return new PartialPath(fullPath);
    } catch (IllegalPathException ignored) {
      return null;
    }
  }

  /** get full path */
  @Override
  public String getFullPath() {
    return fullPath;
  }

  @Override
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  @Override
  public boolean isUseTemplate() {
    return false;
  }

  @Override
  public int getSchemaTemplateId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchemaTemplateId(int schemaTemplateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void preUnsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAboveDatabase() {
    return false;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
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
  public IStorageGroupMNode getAsStorageGroupMNode() {
    if (isStorageGroup()) {
      return (IStorageGroupMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IEntityMNode getAsEntityMNode() {
    if (isEntity()) {
      return (IEntityMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IMeasurementMNode getAsMeasurementMNode() {
    if (isMeasurement()) {
      return (IMeasurementMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RMNode mNode = (RMNode) o;
    return Objects.equals(fullPath, mNode.getFullPath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath);
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    throw new UnsupportedOperationException("Temporarily unsupported");
  }

  @Override
  public CacheEntry getCacheEntry() {
    throw new UnsupportedOperationException("Temporarily unsupported");
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {
    throw new UnsupportedOperationException("Temporarily unsupported");
  }

  @Override
  public void setChildren(IMNodeContainer children) {
    throw new UnsupportedOperationException("Temporarily unsupported");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    throw new UnsupportedOperationException("RMNode doesn't support this method");
  }

  // end
}
