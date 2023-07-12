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
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;

public abstract class AbstractAboveDatabaseMNode<N extends IMNode<N>, BasicNode extends IMNode<N>>
    implements IMNode<N> {

  protected final BasicNode basicMNode;

  protected AbstractAboveDatabaseMNode(BasicNode basicMNode) {
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
    if (basicMNode.getParent() == null) {
      return null;
    }
    return basicMNode.getParent();
  }

  @Override
  public void setParent(N parent) {
    basicMNode.setParent(parent);
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    throw new UnsupportedOperationException("Wrong MNode Type");
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
    return true;
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
    return MNodeType.INTERNAL;
  }

  @Override
  public IDatabaseMNode<N> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<N> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IMeasurementMNode<N> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  /**
   * The basic memory occupied by any AbstractDatabaseDeviceMNode object.
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>basicMNode reference, 8B
   *       </ol>
   *   <li>MapEntry in parent
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + basicMNode.estimateSize();
  }
}
