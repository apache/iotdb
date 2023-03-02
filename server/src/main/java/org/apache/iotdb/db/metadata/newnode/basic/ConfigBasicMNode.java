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
package org.apache.iotdb.db.metadata.newnode.basic;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainerMapImpl;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainers;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.newnode.ConfigMNodeInfo;
import org.apache.iotdb.db.metadata.newnode.IConfigMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;

import java.util.ArrayList;
import java.util.List;

public class ConfigBasicMNode implements IConfigMNode {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * use in Measurement Node so it's protected suppress warnings reason: volatile for double
   * synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile IMNodeContainer<IConfigMNode> children = null;

  private IConfigMNode parent;
  private ConfigMNodeInfo configMNodeInfo;

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
    return (children != null && children.containsKey(name));
  }

  /** get the child with the name */
  @Override
  public IConfigMNode getChild(String name) {
    IConfigMNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    return child;
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
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new MNodeContainerMapImpl<>();
        }
      }
    }
    child.setParent(this);
    IConfigMNode existingChild = children.putIfAbsent(name, child);
    return existingChild == null ? child : existingChild;
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
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new MNodeContainerMapImpl();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public IConfigMNode deleteChild(String name) {
    if (children != null) {
      return children.remove(name);
    }
    return null;
  }

  /**
   * Replace a child of this mnode. New child's name must be the same as old child's name.
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public synchronized void replaceChild(String oldChildName, IConfigMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    IConfigMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    oldChildNode.moveDataToNewMNode(newChildNode);

    children.replace(newChildNode.getName(), newChildNode);
  }

  @Override
  public void moveDataToNewMNode(IConfigMNode newMNode) {
    newMNode.setParent(parent);

    if (children != null) {
      newMNode.setChildren(children);
      children.forEach((childName, childNode) -> childNode.setParent(newMNode));
    }
  }

  @Override
  public IMNodeContainer getChildren() {
    if (children == null) {
      return MNodeContainers.emptyMNodeContainer();
    }
    return children;
  }

  @Override
  public void setChildren(IMNodeContainer children) {
    this.children = children;
  }

  @Override
  public boolean isAboveDatabase() {
    return false;
  }

  @Override
  public boolean isDatabase() {
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
  public MNodeType getMNodeType(Boolean isConfig) {
    return isConfig ? MNodeType.SG_INTERNAL : MNodeType.INTERNAL;
  }

  @Override
  public IDatabaseMNode<IConfigMNode> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<IConfigMNode> getAsEntityMNode() {
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
}
