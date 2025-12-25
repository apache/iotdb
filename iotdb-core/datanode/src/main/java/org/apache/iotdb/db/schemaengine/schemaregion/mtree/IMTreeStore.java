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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;

/**
 * This interface defines the basic access methods of an MTreeStore.
 *
 * <p>MTreeStore could be implemented as memory-based or disk-based for different scenarios.
 */
public interface IMTreeStore<N extends IMNode<N>> {

  /**
   * Generate the ancestor nodes of storageGroupNode
   *
   * @return root node
   */
  N generatePrefix(PartialPath storageGroupPath);

  N getRoot();

  /**
   * Check if parent has child
   *
   * @param parent parent node
   * @param name name or alias
   * @return true if parent has a child whose name or alias matches the condition
   */
  boolean hasChild(N parent, String name) throws MetadataException;

  /**
   * Get child by name or alias
   *
   * @param parent parent node
   * @param name name or alias
   * @return child node
   */
  N getChild(N parent, String name) throws MetadataException;

  IMNodeIterator<N> getChildrenIterator(N parent) throws MetadataException;

  IMNodeIterator<N> getTraverserIterator(
      N parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException;

  N addChild(N parent, String childName, N child);

  void deleteChild(N parent, String childName) throws MetadataException;

  /**
   * Update the mnode under the guarantee of latch.
   *
   * @param node node to be updated
   * @param operation operation
   */
  void updateMNode(N node, Consumer<N> operation) throws MetadataException;

  IDeviceMNode<N> setToEntity(N node) throws MetadataException;

  N setToInternal(IDeviceMNode<N> entityMNode) throws MetadataException;

  void setAlias(IMeasurementMNode<N> measurementMNode, String alias) throws MetadataException;

  void pin(N node) throws MetadataException;

  void unPin(N node);

  void unPinPath(N node);

  IMTreeStore<N> getWithReentrantReadLock();

  void clear();

  boolean createSnapshot(File snapshotDir);

  ReleaseFlushMonitor.RecordNode recordTraverserStatistics();

  void recordTraverserMetric(long costTime);
}
