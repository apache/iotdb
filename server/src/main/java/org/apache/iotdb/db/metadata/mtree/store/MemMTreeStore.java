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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MemMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore {

  private IMNode root;

  public MemMTreeStore(IMNode node) {
    root = node;
  }

  public MemMTreeStore(PartialPath rootPath, boolean isStorageGroup) {
    if (isStorageGroup) {
      this.root =
          new StorageGroupMNode(
              null,
              rootPath.getTailNode(),
              IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
    } else {
      this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    }
  }

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) {
    return parent.hasChild(name);
  }

  @Override
  public IMNode getChild(IMNode parent, String name) {
    return parent.getChild(name);
  }

  @Override
  public IMNodeIterator getChildrenIterator(IMNode parent) {
    return new MemMNodeIterator(parent.getChildren().values().iterator());
  }

  @Override
  public IMNode addChild(IMNode parent, String childName, IMNode child) {
    return parent.addChild(childName, child);
  }

  @Override
  public void deleteChild(IMNode parent, String childName) {
    parent.deleteChild(childName);
  }

  @Override
  public void updateStorageGroupMNode(IStorageGroupMNode node) {}

  @Override
  public void updateMNode(IMNode node) {}

  @Override
  public void pin(IMNode node) {}

  @Override
  public void unPin(IMNode node) {}

  @Override
  public void clear() {
    root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
  }
}
