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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public interface ISchemaFile {

  /**
   * Get the database node, with its segment address of 0.
   *
   * @return node instance, <b>template name as hash code</b>
   */
  IMNode init() throws MetadataException;

  /**
   * Modify header of schema file corresponding to the database node synchronously
   *
   * @param sgNode node to be updated
   * @return true if success
   */
  boolean updateStorageGroupNode(IStorageGroupMNode sgNode) throws IOException;

  /**
   * Only database node along with its descendents could be flushed into schema file.
   *
   * @param node
   */
  void writeMNode(IMNode node) throws MetadataException, IOException;

  void delete(IMNode node) throws IOException, MetadataException;

  void close() throws IOException;

  void clear() throws IOException, MetadataException;

  void sync() throws IOException;

  IMNode getChildNode(IMNode parent, String childName) throws MetadataException, IOException;

  Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException;

  boolean createSnapshot(File snapshotDir);
}
