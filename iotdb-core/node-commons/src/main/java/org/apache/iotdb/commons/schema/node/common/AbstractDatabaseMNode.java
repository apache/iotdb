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
import org.apache.iotdb.commons.schema.node.info.IDatabaseInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;

public abstract class AbstractDatabaseMNode<N extends IMNode<N>, BasicNode extends IMNode<N>>
    implements IDatabaseMNode<N> {

  private static final long serialVersionUID = 7999036474525817732L;

  private final IDatabaseInfo<N> databaseInfo;
  protected final BasicNode basicMNode;

  public AbstractDatabaseMNode(BasicNode basicMNode, IDatabaseInfo<N> databaseInfo) {
    this.basicMNode = basicMNode;
    this.databaseInfo = databaseInfo;
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
    if (newMNode.isDatabase()) {
      databaseInfo.moveDataToNewMNode(newMNode.getAsDatabaseMNode());
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
    return true;
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
    return MNodeType.STORAGE_GROUP;
  }

  @Override
  public IDatabaseMNode<N> getAsDatabaseMNode() {
    return this;
  }

  @Override
  public IDeviceMNode<N> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IMeasurementMNode<N> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDatabaseMNode(this, context);
  }

  @Override
  public long getDataTTL() {
    return databaseInfo.getDataTTL();
  }

  @Override
  public void setDataTTL(long dataTTL) {
    databaseInfo.setDataTTL(dataTTL);
  }

  /**
   * The basic memory occupied by any AbstractDatabaseMNode object
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>databaseInfo reference, 8B
   *         <li>basicMNode reference, 8B
   *       </ol>
   *   <li>MapEntry in parent
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + databaseInfo.estimateSize() + basicMNode.estimateSize();
  }

  protected IDatabaseInfo<N> getDatabaseInfo() {
    return databaseInfo;
  }
}
