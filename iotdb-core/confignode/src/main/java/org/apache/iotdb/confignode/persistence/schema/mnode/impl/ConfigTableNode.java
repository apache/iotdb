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

package org.apache.iotdb.confignode.persistence.schema.mnode.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.container.ConfigMNodeContainer;
import org.apache.iotdb.confignode.persistence.schema.mnode.info.ConfigTableInfo;

import java.util.ArrayList;
import java.util.List;

public class ConfigTableNode implements IConfigMNode {

  private IConfigMNode parent;

  private transient String fullPath;

  private ConfigTableInfo tableNodeInfo;

  public ConfigTableNode(IConfigMNode parent, String name) {
    this.parent = parent;
    this.tableNodeInfo = new ConfigTableInfo(name);
  }

  public TsTable getTable() {
    return tableNodeInfo.getTable();
  }

  public void setTable(TsTable table) {
    tableNodeInfo.setTable(table);
  }

  public TableNodeStatus getStatus() {
    return tableNodeInfo.getStatus();
  }

  public void setStatus(TableNodeStatus status) {
    tableNodeInfo.setStatus(status);
  }

  @Override
  public String getName() {
    return tableNodeInfo.getName();
  }

  @Override
  public void setName(String name) {
    tableNodeInfo.setName(name);
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

  @Override
  public MNodeType getMNodeType() {
    return MNodeType.SG_INTERNAL;
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicMNode(this, context);
  }

  @Override
  public int estimateSize() {
    return 8 + 8 + 8 + 8 + 8 + 8 + 28 + tableNodeInfo.estimateSize();
  }

  @Override
  public IConfigMNode getAsMNode() {
    return this;
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

  @Override
  public IConfigMNode addChild(String name, IConfigMNode child) {
    return null;
  }

  @Override
  public IConfigMNode addChild(IConfigMNode child) {
    return null;
  }

  @Override
  public IConfigMNode deleteChild(String name) {
    return null;
  }

  @Override
  public IMNodeContainer<IConfigMNode> getChildren() {
    return ConfigMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public void setChildren(IMNodeContainer<IConfigMNode> children) {
    // do nothing
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
  public IDatabaseMNode<IConfigMNode> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public void setSchemaTemplateId(int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getSchemaTemplateId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void preUnsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsetSchemaTemplate() {
    throw new UnsupportedOperationException();
  }
}
