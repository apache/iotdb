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
package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.util.Map;

/** this interface provides operations on mtree */
public interface IMetadataAccess {

  /** get root mnode of the mtree */
  IMNode getRoot() throws MetadataException;

  /** get child of the parent */
  IMNode getChild(IMNode parent, String name) throws MetadataException;

  IMNode getChild(IMNode parent, String name, boolean lockChild) throws MetadataException;

  /** get a cloned children map instance from the parent */
  Map<String, IMNode> getChildren(IMNode parent) throws MetadataException;

  /** add a child to the parent */
  void addChild(IMNode parent, String childName, IMNode child) throws MetadataException;

  void addChild(IMNode parent, String childName, IMNode child, boolean lockChild)
      throws MetadataException;

  /** add a alias child to the parent */
  void addAlias(IMNode parent, String alias, IMNode child) throws MetadataException;

  /** replace a child of the parent with the newChild */
  void replaceChild(IMNode parent, String measurement, IMNode newChild) throws MetadataException;

  void replaceChild(IMNode parent, String measurement, IMNode newChild, boolean lockChild)
      throws MetadataException;

  /**
   * delete a child of the parent. Collect all the MNode in subtree of this child into memory
   * Attention!!!! must unlock child Node before delete
   */
  IMNode deleteChild(IMNode parent, String childName) throws MetadataException;

  /** delete a alias child of the parent */
  void deleteAliasChild(IMNode parent, String alias) throws MetadataException;

  void updateMNode(IMNode mNode) throws MetadataException;

  void lockMNodeInMemory(IMNode mNode) throws MetadataException;

  void releaseMNodeMemoryLock(IMNode mNode) throws MetadataException;

  void sync() throws IOException;

  void createSnapshot() throws IOException;

  void clear() throws IOException;
}
