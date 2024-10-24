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

package org.apache.iotdb.commons.schema.node;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.commons.schema.tree.ITreeNode;

/** This interface defines a MNode's operation interfaces. */
public interface IMNode<N extends IMNode<N>> extends ITreeNode {

  String getName();

  void setName(String name);

  N getParent();

  void setParent(N parent);

  String getFullPath();

  void setFullPath(String fullPath);

  PartialPath getPartialPath();

  boolean hasChild(String name);

  N getChild(String name);

  N addChild(String name, N child);

  N addChild(N child);

  N deleteChild(final String name);

  IMNodeContainer<N> getChildren();

  void setChildren(IMNodeContainer<N> children);

  boolean isAboveDatabase();

  boolean isDatabase();

  boolean isDevice();

  boolean isMeasurement();

  MNodeType getMNodeType();

  IDatabaseMNode<N> getAsDatabaseMNode();

  IDeviceMNode<N> getAsDeviceMNode();

  IInternalMNode<N> getAsInternalMNode();

  IMeasurementMNode<N> getAsMeasurementMNode();

  <R, C> R accept(MNodeVisitor<R, C> visitor, C context);

  int estimateSize();

  N getAsMNode();
}
