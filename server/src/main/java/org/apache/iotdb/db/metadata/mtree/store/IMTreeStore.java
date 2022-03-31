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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;

/**
 * This interface defines the basic access methods of an MTreeStore.
 *
 * <p>MTreeStore could be implemented as memory-based or disk-based for different scenarios.
 */
public interface IMTreeStore {

  IMNode getRoot();

  boolean hasChild(IMNode parent, String name) throws MetadataException;

  IMNode getChild(IMNode parent, String name) throws MetadataException;

  IMNodeIterator getChildrenIterator(IMNode parent) throws MetadataException;

  IMNode addChild(IMNode parent, String childName, IMNode child);

  void deleteChild(IMNode parent, String childName) throws MetadataException;

  void updateStorageGroupMNode(IStorageGroupMNode node) throws MetadataException;

  void updateMNode(IMNode node);

  void pin(IMNode node) throws MetadataException;

  void unPin(IMNode node);

  void unPinPath(IMNode node);

  void clear();
}
