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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.util.Iterator;

public interface ISchemaFileManager {

  /**
   * Delete all existed schema files under target directory
   *
   * @return a bare root IMNode
   */
  IMNode init() throws MetadataException, IOException;

  /**
   * Load all schema files under target directory, return the recovered tree.
   *
   * @return a deep copy tree corresponding to files.
   */
  IMNode initWithLoad() throws MetadataException, IOException;

  IMNode getChildNode(IMNode parent, String childName) throws MetadataException, IOException;

  Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException;

  /**
   * If parameter node is above storage group, update upper tree directly. NOTICE, it is not
   * reliable if it had no descendant of storage group.
   *
   * <p>Get storage group name of the parameter node, write the node with non-negative segment
   * address into corresponding file.
   *
   * @param parent cannot be a MeasurementMNode
   */
  void writeMNode(IMNode parent) throws MetadataException, IOException;

  /**
   * If a node above storage group, prune upperTree directly, remove children in cascade.
   *
   * <p>If a storage group node, remove corresponding file and prune upper tree, otherwise remove
   * record of the node as well as segment of it if not measurement.
   *
   * @param targetNode arbitrary instance implements IMNode
   */
  void deleteMNode(IMNode targetNode) throws MetadataException, IOException;

  void sync() throws MetadataException, IOException;

  /** Close all files and get a new upper tree. */
  void close() throws MetadataException, IOException;

  void clear() throws MetadataException, IOException;
}
