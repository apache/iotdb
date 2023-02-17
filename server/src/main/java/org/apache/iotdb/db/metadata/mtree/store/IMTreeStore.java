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
package org.apache.iotdb.db.metadata.mtree.store;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.File;
import java.util.Map;

/**
 * This interface defines the basic access methods of an MTreeStore.
 *
 * <p>MTreeStore could be implemented as memory-based or disk-based for different scenarios.
 */
public interface IMTreeStore {

  /**
   * Generate the ancestor nodes of storageGroupNode
   *
   * @return root node
   */
  IMNode generatePrefix(PartialPath storageGroupPath);

  IMNode getRoot();

  /**
   * Check if parent has child
   *
   * @param parent parent node
   * @param name name or alias
   * @return true if parent has a child whose name or alias matches the condition
   */
  boolean hasChild(IMNode parent, String name) throws MetadataException;

  /**
   * Get child by name or alias
   *
   * @param parent parent node
   * @param name name or alias
   * @return child node
   */
  IMNode getChild(IMNode parent, String name) throws MetadataException;

  IMNodeIterator getChildrenIterator(IMNode parent) throws MetadataException;

  IMNodeIterator getTraverserIterator(
      IMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException;

  IMNode addChild(IMNode parent, String childName, IMNode child);

  void deleteChild(IMNode parent, String childName) throws MetadataException;

  void updateMNode(IMNode node) throws MetadataException;

  IEntityMNode setToEntity(IMNode node) throws MetadataException;

  IMNode setToInternal(IEntityMNode entityMNode) throws MetadataException;

  void setAlias(IMeasurementMNode measurementMNode, String alias) throws MetadataException;

  void pin(IMNode node) throws MetadataException;

  void unPin(IMNode node);

  void unPinPath(IMNode node);

  IMTreeStore getWithReentrantReadLock();

  void clear();

  boolean createSnapshot(File snapshotDir);
}
