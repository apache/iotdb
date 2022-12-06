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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaUtils;

import org.rocksdb.RocksDBException;

public class RInternalMNode extends RMNode {

  private volatile boolean useTemplate = false;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RInternalMNode(String fullPath, RSchemaReadWriteHandler readWriteHandler) {
    super(fullPath, readWriteHandler);
  }

  @Override
  void updateChildNode(String childName, int childNameMaxLevel) throws MetadataException {
    String innerName =
        RSchemaUtils.convertPartialPathToInner(
            childName, childNameMaxLevel, RMNodeType.INTERNAL.getValue());
    try {
      readWriteHandler.updateNode(innerName.getBytes(), new byte[] {});
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    String childPathName = fullPath.concat(RSchemaConstants.PATH_SEPARATOR).concat(name);
    IMNode node = getNodeBySpecifiedPath(childPathName);
    return node != null;
  }

  /** get the child with the name */
  @Override
  public IMNode getChild(String name) {
    String childPathName = fullPath.concat(RSchemaConstants.PATH_SEPARATOR).concat(name);
    return getNodeBySpecifiedPath(childPathName);
  }

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   * @return
   */
  @Override
  public IMNode addChild(String name, IMNode child) {
    child.setParent(this);
    String childName = fullPath.concat(RSchemaConstants.PATH_SEPARATOR).concat(name);
    int childNameMaxLevel = RSchemaUtils.getLevelByPartialPath(childName);
    try {
      if (child instanceof RMNode) {
        ((RMNode) child).updateChildNode(childName, childNameMaxLevel);
      } else {
        logger.error("Rocksdb-based is currently used, but the node type received is not RMNode!");
      }
    } catch (MetadataException e) {
      logger.error(e.getMessage());
    }
    return child;
  }

  /**
   * Add a child to the current mnode.
   *
   * <p>This method will not take the child's name as one of the inputs and will also make this
   * Mnode be child node's parent. All is to reduce the probability of mistaken by users and be more
   * convenient for users to use. And the return of this method is used to conveniently construct a
   * chain of time series for users.
   *
   * @param child child's node
   * @return return the MNode already added
   */
  @Override
  public IMNode addChild(IMNode child) {
    addChild(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public IMNode deleteChild(String name) {
    String childPathName = fullPath.concat(RSchemaConstants.PATH_SEPARATOR).concat(name);
    int nodeNameMaxLevel = RSchemaUtils.getLevelByPartialPath(childPathName);
    for (RMNodeType type : RMNodeType.values()) {
      byte[] childInnerName =
          RSchemaUtils.convertPartialPathToInner(childPathName, nodeNameMaxLevel, type.getValue())
              .getBytes();
      try {
        if (readWriteHandler.keyExist(childInnerName)) {
          readWriteHandler.deleteByKey(childInnerName);
          return null;
        }
      } catch (RocksDBException e) {
        logger.error(e.getMessage());
      }
    }
    // The return value from this method is used to estimate memory size
    // When based on Rocksdb, mNodes are not held in memory for long periods
    // Therefore, the return value here is meaningless
    return null;
  }

  /**
   * replace a child of this mnode
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    deleteChild(oldChildName);
    addChild(newChildNode);
  }

  @Override
  public IMNodeContainer getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return isConfig ? MNodeType.SG_INTERNAL : MNodeType.INTERNAL;
  }

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }
}
