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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.basic;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.CacheEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.CachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.info.CacheMNodeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class CachedBasicMNode implements ICachedMNode {

  private static final long serialVersionUID = -770028375899514063L;

  private ICachedMNode parent;
  private final CacheMNodeInfo cacheMNodeInfo;

  /** from root to this node, only be set when used once for InternalMNode */
  private String fullPath;

  /** Constructor of MNode. */
  public CachedBasicMNode(ICachedMNode parent, String name) {
    this.parent = parent;
    this.cacheMNodeInfo = new CacheMNodeInfo(name);
  }

  @Override
  public String getName() {
    return cacheMNodeInfo.getName();
  }

  @Override
  public void setName(String name) {
    cacheMNodeInfo.setName(name);
  }

  @Override
  public ICachedMNode getParent() {
    return parent;
  }

  @Override
  public void setParent(ICachedMNode parent) {
    this.parent = parent;
  }

  @Override
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath();
    }
    return fullPath;
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(getName());
    ICachedMNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.getName());
    }
    return builder.toString();
  }

  @Override
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  @Override
  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    ICachedMNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return false;
  }

  /** get the child with the name */
  @Override
  public ICachedMNode getChild(String name) {
    return null;
  }

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   * @return the child of this node after addChild
   */
  @Override
  public ICachedMNode addChild(String name, ICachedMNode child) {
    return null;
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
  public ICachedMNode addChild(ICachedMNode child) {
    return null;
  }

  /** delete a child */
  @Override
  public ICachedMNode deleteChild(String name) {
    return null;
  }

  /**
   * Replace a child of this mnode. New child's name must be the same as old child's name.
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public synchronized void replaceChild(String oldChildName, ICachedMNode newChildNode) {}

  @Override
  public void moveDataToNewMNode(ICachedMNode newMNode) {
    newMNode.setParent(parent);
    newMNode.setCacheEntry(getCacheEntry());
  }

  @Override
  public IMNodeContainer<ICachedMNode> getChildren() {
    return CachedMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public void setChildren(IMNodeContainer<ICachedMNode> children) {}

  @Override
  public boolean isAboveDatabase() {
    return false;
  }

  @Override
  public boolean isDatabase() {
    return false;
  }

  @Override
  public boolean isDevice() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return isConfig ? MNodeType.SG_INTERNAL : MNodeType.INTERNAL;
  }

  @Override
  public IDatabaseMNode<ICachedMNode> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<ICachedMNode> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IMeasurementMNode<ICachedMNode> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicMNode(this, context);
  }

  @Override
  public CacheEntry getCacheEntry() {
    return cacheMNodeInfo.getCacheEntry();
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {
    cacheMNodeInfo.setCacheEntry(cacheEntry);
  }

  /**
   * The basic memory occupied by any CacheBasicMNode object
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>basicMNodeInfo reference, 8B
   *         <li>parent reference, 8B
   *         <li>fullPath reference, 8B
   *       </ol>
   *   <li>MapEntry in parent
   *       <ol>
   *         <li>key reference, 8B
   *         <li>value reference, 8B
   *         <li>entry size, see ConcurrentHashMap.Node, 28
   *       </ol>
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + 8 + 8 + 8 + 8 + 28 + cacheMNodeInfo.estimateSize();
  }

  @Override
  public ICachedMNode getAsMNode() {
    return this;
  }
}
