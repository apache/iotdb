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
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;

import java.util.Map;

// This class provides a device view for an internal node with deviceInfo.
public class DeviceMNodeWrapper<N extends IMNode<N>, BasicNode extends IInternalMNode<N>>
    implements IDeviceMNode<N> {

  protected final BasicNode basicMNode;

  public DeviceMNodeWrapper(BasicNode basicMNode) {
    this.basicMNode = basicMNode;
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
  public IMNodeContainer<N> getChildren() {
    return basicMNode.getChildren();
  }

  @Override
  public void setChildren(IMNodeContainer<N> children) {
    basicMNode.setChildren(children);
  }

  @Override
  public boolean isAboveDatabase() {
    return basicMNode.isAboveDatabase();
  }

  @Override
  public boolean isDatabase() {
    return basicMNode.isDatabase();
  }

  @Override
  public IDeviceInfo<N> getDeviceInfo() {
    return basicMNode.getDeviceInfo();
  }

  @Override
  public void setDeviceInfo(IDeviceInfo<N> deviceInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDevice() {
    return true;
  }

  @Override
  public MNodeType getMNodeType() {
    return MNodeType.DEVICE;
  }

  @Override
  public IDatabaseMNode<N> getAsDatabaseMNode() {
    return basicMNode.getAsDatabaseMNode();
  }

  @Override
  public IDeviceMNode<N> getAsDeviceMNode() {
    return this;
  }

  @Override
  public IInternalMNode<N> getAsInternalMNode() {
    return basicMNode;
  }

  @Override
  public IMeasurementMNode<N> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicMNode(this, context);
  }

  @Override
  public boolean addAlias(String alias, IMeasurementMNode<N> child) {
    return basicMNode.getDeviceInfo().addAlias(alias, child);
  }

  @Override
  public void deleteAliasChild(String alias) {
    basicMNode.getDeviceInfo().deleteAliasChild(alias);
  }

  @Override
  public Map<String, IMeasurementMNode<N>> getAliasChildren() {
    return basicMNode.getDeviceInfo().getAliasChildren();
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode<N>> aliasChildren) {
    basicMNode.getDeviceInfo().setAliasChildren(aliasChildren);
  }

  @Override
  public boolean isUseTemplate() {
    return basicMNode.getDeviceInfo().isUseTemplate();
  }

  @Override
  public void setUseTemplate(final boolean useTemplate) {
    basicMNode.getDeviceInfo().setUseTemplate(useTemplate);
  }

  @Override
  public void setSchemaTemplateId(final int schemaTemplateId) {
    basicMNode.getDeviceInfo().setSchemaTemplateId(schemaTemplateId);
  }

  @Override
  public int getSchemaTemplateId() {
    return basicMNode.getDeviceInfo().getSchemaTemplateId();
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    return basicMNode.getDeviceInfo().getSchemaTemplateIdWithState();
  }

  @Override
  public boolean isPreDeactivateSelfOrTemplate() {
    return basicMNode.getDeviceInfo().isPreDeactivateSelfOrTemplate();
  }

  @Override
  public void preDeactivateSelfOrTemplate() {
    basicMNode.getDeviceInfo().preDeactivateSelfOrTemplate();
  }

  @Override
  public void rollbackPreDeactivateSelfOrTemplate() {
    basicMNode.getDeviceInfo().rollbackPreDeactivateSelfOrTemplate();
  }

  @Override
  public void deactivateTemplate() {
    basicMNode.getDeviceInfo().deactivateTemplate();
  }

  @Override
  public boolean isAligned() {
    Boolean align = basicMNode.getDeviceInfo().isAligned();
    if (align == null) {
      return false;
    }
    return align;
  }

  @Override
  public Boolean isAlignedNullable() {
    return basicMNode.getDeviceInfo().isAligned();
  }

  @Override
  public void setAligned(Boolean isAligned) {
    basicMNode.getDeviceInfo().setAligned(isAligned);
  }

  @Override
  public int estimateSize() {
    return basicMNode.estimateSize();
  }

  @Override
  public N getAsMNode() {
    return basicMNode.getAsMNode();
  }
}
