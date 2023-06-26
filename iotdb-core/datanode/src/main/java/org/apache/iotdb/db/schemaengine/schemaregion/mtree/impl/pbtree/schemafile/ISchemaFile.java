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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public interface ISchemaFile {

  /**
   * Get the database node, with its segment address of 0.
   *
   * @return node instance, <b>template name as hash code</b>
   */
  ICachedMNode init() throws MetadataException;

  /**
   * Modify header of pbtree file corresponding to the database node synchronously
   *
   * @param sgNode node to be updated
   * @return true if success
   */
  boolean updateDatabaseNode(IDatabaseMNode<ICachedMNode> sgNode) throws IOException;

  /**
   * Only database node along with its descendents could be flushed into pbtree file.
   *
   * @param node
   */
  void writeMNode(ICachedMNode node) throws MetadataException, IOException;

  void delete(ICachedMNode node) throws IOException, MetadataException;

  void close() throws IOException;

  void clear() throws IOException, MetadataException;

  void sync() throws IOException;

  ICachedMNode getChildNode(ICachedMNode parent, String childName)
      throws MetadataException, IOException;

  Iterator<ICachedMNode> getChildren(ICachedMNode parent) throws MetadataException, IOException;

  boolean createSnapshot(File snapshotDir);
}
