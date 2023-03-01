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
package org.apache.iotdb.db.metadata.newnode.device;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;

import java.util.Map;

public abstract class AbstractDeviceMNode<N extends IMNode<?>, BasicNode extends IMNode<N>>
    implements IDeviceMNode<N> {

  private final IDeviceInfo deviceInfo;
  protected BasicNode basicMNode;

  /** Constructor of MNode. */
  public AbstractDeviceMNode() {
    this.deviceInfo = new DeviceInfo();
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
    return basicMNode.hasChild(name);
  }

  @Override
  public N getChild(String name) {
    return basicMNode.getChild(name);
  }

  @Override
  public N addChild(String name, N child) {
    return basicMNode.addChild(name, child);
  }

  @Override
  public N addChild(N child) {
    return basicMNode.addChild(child);
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
    // TODO
  }

  @Override
  public IMNodeContainer getChildren() {
    return basicMNode.getChildren();
  }

  @Override
  public void setChildren(IMNodeContainer children) {
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
  public boolean isEntity() {
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
  public IDeviceMNode<N> getAsEntityMNode() {
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
  public boolean addAlias(String alias, IMeasurementMNode child) {
    return deviceInfo.addAlias(alias, child);
  }

  @Override
  public void deleteAliasChild(String alias) {
    deviceInfo.deleteAliasChild(alias);
  }

  @Override
  public Map<String, IMeasurementMNode> getAliasChildren() {
    return deviceInfo.getAliasChildren();
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren) {
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
    return deviceInfo.isAligned();
  }

  @Override
  public void setAligned(boolean isAligned) {
    deviceInfo.setAligned(isAligned);
  }
}
