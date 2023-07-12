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

package org.apache.iotdb.commons.schema.node.common;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.schema.node.info.IDeviceInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;

import java.util.Map;

public abstract class AbstractDeviceMNode<N extends IMNode<N>, BasicNode extends IMNode<N>>
    implements IDeviceMNode<N> {

  private final IDeviceInfo<N> deviceInfo;
  protected final BasicNode basicMNode;

  protected AbstractDeviceMNode(BasicNode basicMNode, IDeviceInfo<N> deviceInfo) {
    this.basicMNode = basicMNode;
    this.deviceInfo = deviceInfo;
  }

  public BasicNode getBasicMNode() {
    return basicMNode;
  }

  @Override
  public String getName() {
    return basicMNode.getName();
  }

  @Override
  public void setName(String name) {
    basicMNode.setName(name);
  }

  @Override
  public N getParent() {
    return basicMNode.getParent();
  }

  @Override
  public void setParent(N parent) {
    basicMNode.setParent(parent);
  }

  @Override
  public String getFullPath() {
    return basicMNode.getFullPath();
  }

  @Override
  public void setFullPath(String fullPath) {
    basicMNode.setFullPath(fullPath);
  }

  @Override
  public PartialPath getPartialPath() {
    return basicMNode.getPartialPath();
  }

  @Override
  public boolean hasChild(String name) {
    return basicMNode.hasChild(name) || deviceInfo.hasAliasChild(name);
  }

  @Override
  public N getChild(String name) {
    N res = basicMNode.getChild(name);
    if (res == null) {
      res = deviceInfo.getAliasChild(name);
    }
    return res;
  }

  @Override
  public N addChild(String name, N child) {
    N res = basicMNode.addChild(name, child);
    if (res == child) {
      child.setParent(this.getAsMNode());
    }
    return res;
  }

  @Override
  public N addChild(N child) {
    N res = basicMNode.addChild(child);
    if (res == child) {
      child.setParent(this.getAsMNode());
    }
    return res;
  }

  @Override
  public N deleteChild(String name) {
    return basicMNode.deleteChild(name);
  }

  @Override
  public void replaceChild(String oldChildName, N newChildNode) {
    basicMNode.replaceChild(oldChildName, newChildNode);
  }

  @Override
  public void moveDataToNewMNode(N newMNode) {
    basicMNode.moveDataToNewMNode(newMNode);
    if (newMNode.isDevice()) {
      deviceInfo.moveDataToNewMNode(newMNode.getAsDeviceMNode());
    }
  }

  @Override
  public IMNodeContainer<N> getChildren() {
    return basicMNode.getChildren();
  }

  @Override
  public void setChildren(IMNodeContainer<N> children) {
    basicMNode.setChildren(children);
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
  public boolean isDevice() {
    return true;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.DEVICE;
  }

  @Override
  public IDatabaseMNode<N> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<N> getAsDeviceMNode() {
    return this;
  }

  @Override
  public IMeasurementMNode<N> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceMNode(this, context);
  }

  @Override
  public boolean addAlias(String alias, IMeasurementMNode<N> child) {
    return deviceInfo.addAlias(alias, child);
  }

  @Override
  public void deleteAliasChild(String alias) {
    deviceInfo.deleteAliasChild(alias);
  }

  @Override
  public Map<String, IMeasurementMNode<N>> getAliasChildren() {
    return deviceInfo.getAliasChildren();
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode<N>> aliasChildren) {
    deviceInfo.setAliasChildren(aliasChildren);
  }

  @Override
  public boolean isUseTemplate() {
    return deviceInfo.isUseTemplate();
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    deviceInfo.setUseTemplate(useTemplate);
  }

  @Override
  public void setSchemaTemplateId(int schemaTemplateId) {
    deviceInfo.setSchemaTemplateId(schemaTemplateId);
  }

  @Override
  public int getSchemaTemplateId() {
    return deviceInfo.getSchemaTemplateId();
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    return deviceInfo.getSchemaTemplateIdWithState();
  }

  @Override
  public boolean isPreDeactivateTemplate() {
    return deviceInfo.isPreDeactivateTemplate();
  }

  @Override
  public void preDeactivateTemplate() {
    deviceInfo.preDeactivateTemplate();
  }

  @Override
  public void rollbackPreDeactivateTemplate() {
    deviceInfo.rollbackPreDeactivateTemplate();
  }

  @Override
  public void deactivateTemplate() {
    deviceInfo.deactivateTemplate();
  }

  @Override
  public boolean isAligned() {
    Boolean align = deviceInfo.isAligned();
    if (align == null) {
      return false;
    }
    return align;
  }

  @Override
  public Boolean isAlignedNullable() {
    return deviceInfo.isAligned();
  }

  @Override
  public void setAligned(Boolean isAligned) {
    deviceInfo.setAligned(isAligned);
  }

  /**
   * The basic memory occupied by any AbstractDeviceMNode object
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>deviceInfo reference, 8B
   *         <li>basicMNode reference, 8B
   *       </ol>
   *   <li>MapEntry in parent
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + deviceInfo.estimateSize() + basicMNode.estimateSize();
  }
}
