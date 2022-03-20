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
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MemMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore {

  private IMNode root;

  public MemMTreeStore() {}

  public void init(PartialPath rootPath, boolean isStorageGroup) throws IOException {
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
  public void addChild(IMNode parent, String childName, IMNode child) {
    parent.addChild(childName, child);
  }

  @Override
  public void addAlias(IEntityMNode parent, String alias, IMeasurementMNode child) {
    parent.addAlias(alias, child);
  }

  @Override
  public List<IMeasurementMNode> deleteChild(IMNode parent, String childName) {
    IMNode cur = parent.getChild(childName);
    parent.deleteChild(childName);
    // collect all the LeafMNode in this storage group
    List<IMeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<IMNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      IMNode node = queue.poll();
      IMNodeIterator iterator = getChildrenIterator(node);
      IMNode child;
      while (iterator.hasNext()) {
        child = iterator.next();
        if (child.isMeasurement()) {
          leafMNodes.add(child.getAsMeasurementMNode());
        } else {
          queue.add(child);
        }
      }
    }
    return leafMNodes;
  }

  @Override
  public void deleteAliasChild(IEntityMNode parent, String alias) {
    parent.deleteAliasChild(alias);
  }

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
