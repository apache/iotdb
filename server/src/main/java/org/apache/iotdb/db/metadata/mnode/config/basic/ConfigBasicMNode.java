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
package org.apache.iotdb.db.metadata.mnode.config.basic;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.mnode.config.IConfigMNode;
import org.apache.iotdb.db.metadata.mnode.config.container.ConfigMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.config.info.ConfigMNodeInfo;

import java.util.ArrayList;
import java.util.List;

public class ConfigBasicMNode implements IConfigMNode {

  private static final long serialVersionUID = -770028375899514063L;

  private IConfigMNode parent;
  private final ConfigMNodeInfo configMNodeInfo;

  /** from root to this node, only be set when used once for InternalMNode */
  private String fullPath;

  /** Constructor of MNode. */
  public ConfigBasicMNode(IConfigMNode parent, String name) {
    this.parent = parent;
    this.configMNodeInfo = new ConfigMNodeInfo(name);
  }

  @Override
  public String getName() {
    return configMNodeInfo.getName();
  }

  @Override
  public void setName(String name) {
    configMNodeInfo.setName(name);
  }

  @Override
  public IConfigMNode getParent() {
    return parent;
  }

  @Override
  public void setParent(IConfigMNode parent) {
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
    IConfigMNode curr = this;
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
    IConfigMNode temp = this;
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
  public IConfigMNode getChild(String name) {
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
  public IConfigMNode addChild(String name, IConfigMNode child) {
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
  public IConfigMNode addChild(IConfigMNode child) {
    return null;
  }

  /** delete a child */
  @Override
  public IConfigMNode deleteChild(String name) {
    return null;
  }

  /**
   * Replace a child of this mnode. New child's name must be the same as old child's name.
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public synchronized void replaceChild(String oldChildName, IConfigMNode newChildNode) {}

  @Override
  public void moveDataToNewMNode(IConfigMNode newMNode) {
    newMNode.setParent(parent);
    newMNode.setSchemaTemplateId(configMNodeInfo.getSchemaTemplateIdWithState());
  }

  @Override
  public IMNodeContainer<IConfigMNode> getChildren() {
    return ConfigMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public void setChildren(IMNodeContainer<IConfigMNode> children) {}

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
  public IDatabaseMNode<IConfigMNode> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<IConfigMNode> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IMeasurementMNode<IConfigMNode> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicMNode(this, context);
  }

  @Override
  public void setSchemaTemplateId(int id) {
    configMNodeInfo.setSchemaTemplateId(id);
  }

  @Override
  public int getSchemaTemplateId() {
    return configMNodeInfo.getSchemaTemplateId();
  }

  @Override
  public void preUnsetSchemaTemplate() {
    configMNodeInfo.preUnsetSchemaTemplate();
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    configMNodeInfo.rollbackUnsetSchemaTemplate();
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    return configMNodeInfo.isSchemaTemplatePreUnset();
  }

  @Override
  public void unsetSchemaTemplate() {
    configMNodeInfo.unsetSchemaTemplate();
  }

  /**
   * The basic memory occupied by any ConfigBasicMNode object
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
    return 8 + 8 + 8 + 8 + 8 + 8 + 28 + configMNodeInfo.estimateSize();
  }

  @Override
  public IConfigMNode getAsMNode() {
    return this;
  }
}
