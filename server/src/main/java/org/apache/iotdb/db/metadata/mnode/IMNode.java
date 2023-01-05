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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.tree.ITreeNode;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheEntry;

/** This interface defines a MNode's operation interfaces. */
public interface IMNode extends ITreeNode {

  String getName();

  void setName(String name);

  IMNode getParent();

  void setParent(IMNode parent);

  String getFullPath();

  void setFullPath(String fullPath);

  PartialPath getPartialPath();

  boolean hasChild(String name);

  IMNode getChild(String name);

  IMNode addChild(String name, IMNode child);

  IMNode addChild(IMNode child);

  IMNode deleteChild(String name);

  // this method will replace the oldChild with the newChild, the data of oldChild will be moved to
  // newChild
  void replaceChild(String oldChildName, IMNode newChildNode);

  // this method will move all the reference or value of current node's attributes to newMNode
  void moveDataToNewMNode(IMNode newMNode);

  IMNodeContainer getChildren();

  void setChildren(IMNodeContainer children);

  boolean isUseTemplate();

  void setUseTemplate(boolean useTemplate);

  /** @return the logic id of template set or activated on this node, id>=-1 */
  int getSchemaTemplateId();

  /** @return the template id with current state, may be negative since unset or deactivation */
  int getSchemaTemplateIdWithState();

  void setSchemaTemplateId(int schemaTemplateId);

  void preUnsetSchemaTemplate();

  void rollbackUnsetSchemaTemplate();

  boolean isSchemaTemplatePreUnset();

  void unsetSchemaTemplate();

  boolean isAboveDatabase();

  boolean isStorageGroup();

  boolean isEntity();

  boolean isMeasurement();

  MNodeType getMNodeType(Boolean isConfig);

  IStorageGroupMNode getAsStorageGroupMNode();

  IEntityMNode getAsEntityMNode();

  IMeasurementMNode getAsMeasurementMNode();

  CacheEntry getCacheEntry();

  void setCacheEntry(CacheEntry cacheEntry);

  <R, C> R accept(MNodeVisitor<R, C> visitor, C context);
}
