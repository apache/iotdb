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

import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeContainers;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.MockSchemaFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CachedMTreeStore implements IMTreeStore {

  private IMemManager memManager = new MemManager();

  private ICacheStrategy cacheStrategy = new CacheStrategy();

  private ISchemaFile file;

  private IMNode root;

  @Override
  public void init() throws IOException {
    MNodeContainers.IS_DISK_MODE = true;
    file = new MockSchemaFile();
    root = file.init();
    cacheStrategy.pinMNode(root);
  }

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) {
    return getChild(parent, name) != null;
  }

  @Override
  public IMNode getChild(IMNode parent, String name) {
    IMNode node = parent.getChild(name);
    if (node == null) {
      if (!getCachedMNodeContainer(parent).isVolatile()) {
        node = file.getChildNode(parent, name);
        if (node != null && cacheStrategy.isCached(parent)) {
          if (cacheMNodeInMemory(node)) {
            cacheStrategy.updateCacheStatusAfterRead(node);
          }
        }
      }
    } else {
      if (cacheStrategy.isCached(node)) {
        cacheStrategy.updateCacheStatusAfterRead(node);
      }
    }
    return node;
  }

  // must pin parent first
  @Override
  public IMNode getPinnedChild(IMNode parent, String name) {
    IMNode node = parent.getChild(name);
    if (node == null) {
      if (!getCachedMNodeContainer(parent).isVolatile()) {
        node = file.getChildNode(parent, name);
        if (node != null) {
          pinMNodeInMemory(node);
          cacheStrategy.updateCacheStatusAfterRead(node);
        }
      }
    } else {
      pinMNodeInMemory(node);
      cacheStrategy.updateCacheStatusAfterRead(node);
    }
    return node;
  }

  @Override
  public Iterator<IMNode> getChildrenIterator(IMNode parent) {
    return new CachedMNodeIterator(parent);
  }

  // must pin parent first
  @Override
  public void addChild(IMNode parent, String childName, IMNode child) {
    child.setParent(parent);
    pinMNodeInMemory(child);
    cacheStrategy.updateCacheStatusAfterAppend(child);
  }

  @Override
  public void addAlias(IEntityMNode parent, String alias, IMeasurementMNode child) {
    parent.addAlias(alias, child);
  }

  @Override
  public void deleteChild(IMNode parent, String childName) {
    IMNode node = parent.getChild(childName);
    parent.deleteChild(childName);
    if (cacheStrategy.isCached(node)) {
      List<IMNode> removedMNodes = cacheStrategy.remove(node);
      for (IMNode removedMNode : removedMNodes) {
        if (cacheStrategy.isPinned(removedMNode)) {
          memManager.releasePinnedMemResource(removedMNode);
        }
        memManager.releaseMemResource(removedMNode);
      }
    }
    if (!getCachedMNodeContainer(parent).isVolatile()) {
      file.deleteMNode(node);
    }
  }

  @Override
  public void deleteAliasChild(IEntityMNode parent, String alias) {
    parent.deleteAliasChild(alias);
  }

  @Override
  public void updateMNode(IMNode node) {
    if (cacheStrategy.isCached(node)) {
      cacheStrategy.updateCacheStatusAfterUpdate(node);
    } else {
      file.writeMNode(node.getParent());
    }
  }

  @Override
  public void unPin(IMNode node) {
    List<IMNode> releasedMNodes = cacheStrategy.unPinMNode(node);
    for (IMNode releasedMNode : releasedMNodes) {
      memManager.releasePinnedMemResource(releasedMNode);
    }
    if (!memManager.isUnderThreshold()) {
      executeMemoryRelease();
    }
  }

  @Override
  public void createSnapshot() throws IOException {}

  @Override
  public void clear() {
    root = null;
    cacheStrategy.clear();
    memManager.clear();
    if (file != null) {
      file.close();
    }
    file = null;
  }

  private boolean cacheMNodeInMemory(IMNode node) {
    if (!memManager.requestMemResource(node)) {
      executeMemoryRelease();
      if (cacheStrategy.isCached(node.getParent())) {
        memManager.requestMemResource(node);
        cacheStrategy.cacheMNode(node);
        return true;
      }
    }
    return false;
  }

  private void executeMemoryRelease() {
    flushVolatileNodes();
    List<IMNode> evictedMNodes;
    while (!memManager.isUnderThreshold()) {
      evictedMNodes = cacheStrategy.evict();
      for (IMNode evictedMNode : evictedMNodes) {
        memManager.releaseMemResource(evictedMNode);
      }
    }
  }

  private void flushVolatileNodes() {
    List<IMNode> nodesToPersist = cacheStrategy.collectVolatileMNodes(root);
    for (IMNode volatileNode : nodesToPersist) {
      file.writeMNode(volatileNode);
      if (cacheStrategy.isCached(volatileNode)) {
        cacheStrategy.updateCacheStatusAfterPersist(volatileNode);
      }
    }
  }

  private void pinMNodeInMemory(IMNode node) {
    if (!cacheStrategy.isPinned(node)) {
      if (cacheStrategy.isCached(node)) {
        memManager.upgradeMemResource(node);
      } else {
        memManager.requestPinnedMemResource(node);
      }
    }
    cacheStrategy.pinMNode(node);
    if (!memManager.isUnderThreshold()) {
      executeMemoryRelease();
    }
  }

  private class CachedMNodeIterator implements Iterator<IMNode> {

    IMNode parent;
    Iterator<IMNode> iterator;
    boolean isIteratingDisk = false;
    IMNode nextNode;

    CachedMNodeIterator(IMNode parent) {
      this.parent = parent;
      this.iterator = getCachedMNodeContainer(parent).getChildrenIterator();
    }

    @Override
    public boolean hasNext() {
      readNext();
      return nextNode != null;
    }

    // must invoke hasNext() first
    @Override
    public IMNode next() {
      if (nextNode == null) {
        throw new NoSuchElementException();
      }
      if (!isIteratingDisk) {
        if (cacheStrategy.isCached(nextNode)) {
          cacheStrategy.updateCacheStatusAfterRead(nextNode);
        }
      } else {
        if (cacheStrategy.isCached(parent)) {
          if (cacheMNodeInMemory(nextNode)) {
            cacheStrategy.updateCacheStatusAfterRead(nextNode);
          }
        }
      }
      IMNode result = nextNode;
      nextNode = null;
      return result;
    }

    private void readNext() {
      if (!isIteratingDisk) {
        if (iterator.hasNext()) {
          nextNode = iterator.next();
          return;
        } else {
          startIteratingDisk();
        }
      }

      while (iterator.hasNext()) {
        nextNode = iterator.next();
        if (!parent.hasChild(nextNode.getName())) {
          break;
        }
      }
    }

    private void startIteratingDisk() {
      iterator = file.getChildren(parent);
      isIteratingDisk = true;
    }
  }
}
