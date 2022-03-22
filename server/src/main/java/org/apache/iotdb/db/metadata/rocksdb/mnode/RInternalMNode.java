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
package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.rocksdb.RSchemaUtils;
import org.apache.iotdb.db.metadata.template.Template;

import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;

public class RInternalMNode extends RMNode {

  // schema template
  protected Template schemaTemplate = null;

  private volatile boolean useTemplate = false;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RInternalMNode(String fullPath) {
    super(fullPath);
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
      if (child instanceof RStorageGroupMNode) {
        String innerName =
            RSchemaUtils.convertPartialPathToInner(
                childName, childNameMaxLevel, RMNodeType.STORAGE_GROUP.getValue());
        long ttl = ((RStorageGroupMNode) child).getDataTTL();
        readWriteHandler.updateNode(
            innerName.getBytes(), RSchemaUtils.updateTTL(RSchemaConstants.DEFAULT_NODE_VALUE, ttl));
      } else if (child instanceof REntityMNode) {
        String innerName =
            RSchemaUtils.convertPartialPathToInner(
                childName, childNameMaxLevel, RMNodeType.ENTITY.getValue());
        readWriteHandler.updateNode(innerName.getBytes(), new byte[] {});
      } else if (child instanceof RInternalMNode) {
        String innerName =
            RSchemaUtils.convertPartialPathToInner(
                childName, childNameMaxLevel, RMNodeType.INTERNAL.getValue());
        readWriteHandler.updateNode(innerName.getBytes(), new byte[] {});
      } else if (child instanceof RMeasurementMNode) {
        String innerName =
            RSchemaUtils.convertPartialPathToInner(
                childName, childNameMaxLevel, RMNodeType.MEASUREMENT.getValue());
        // todo all existing attributes of the measurementNode need to be written
        readWriteHandler.updateNode(innerName.getBytes(), new byte[] {});
      }
    } catch (RocksDBException e) {
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
  public void deleteChild(String name) {
    String childPathName = fullPath.concat(RSchemaConstants.PATH_SEPARATOR).concat(name);
    int nodeNameMaxLevel = RSchemaUtils.getLevelByPartialPath(childPathName);
    for (RMNodeType type : RMNodeType.values()) {
      byte[] childInnerName =
          RSchemaUtils.convertPartialPathToInner(childPathName, nodeNameMaxLevel, type.getValue())
              .getBytes();
      try {
        if (readWriteHandler.keyExist(childInnerName)) {
          readWriteHandler.deleteByKey(childInnerName);
          return;
        }
      } catch (RocksDBException e) {
        logger.error(e.getMessage());
      }
    }
  }

  /**
   * replace a child of this mnode
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    IMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    // newChildNode builds parent-child relationship
    Map<String, IMNode> grandChildren = oldChildNode.getChildren();
    if (!grandChildren.isEmpty()) {
      newChildNode.setChildren(grandChildren);
      grandChildren.forEach(
          (grandChildName, grandChildNode) -> grandChildNode.setParent(newChildNode));
    }

    if (newChildNode.isEntity() && oldChildNode.isEntity()) {
      Map<String, IMeasurementMNode> grandAliasChildren =
          oldChildNode.getAsEntityMNode().getAliasChildren();
      if (!grandAliasChildren.isEmpty()) {
        newChildNode.getAsEntityMNode().setAliasChildren(grandAliasChildren);
        grandAliasChildren.forEach(
            (grandAliasChildName, grandAliasChild) -> grandAliasChild.setParent(newChildNode));
      }
      newChildNode.getAsEntityMNode().setUseTemplate(oldChildNode.isUseTemplate());
    }

    newChildNode.setSchemaTemplate(oldChildNode.getSchemaTemplate());

    newChildNode.setParent(this);

    this.deleteChild(oldChildName);
    this.addChild(newChildNode.getName(), newChildNode);
  }

  @Override
  public Map<String, IMNode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    throw new UnsupportedOperationException();
  }

  /**
   * get upper template of this node, remember we get nearest template alone this node to root
   *
   * @return upper template
   */
  @Override
  public Template getUpperTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Template getSchemaTemplate() {
    return schemaTemplate;
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {
    this.schemaTemplate = schemaTemplate;
  }

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    throw new UnsupportedOperationException();
  }
}
