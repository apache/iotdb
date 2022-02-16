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
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeContainers;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.MockSchemaFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CachedMTreeStore implements IMTreeStore {

  private IMemManager memManager = new MemManager();

  private ICacheStrategy cacheStrategy = new CacheStrategy();

  private ISchemaFile file;

  private IMNode root;

  private ExecutorService flushTask = Executors.newFixedThreadPool(1);
  private boolean hasFlushTask = false;

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); // default writer preferential
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

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
    readLock.lock();
    try {
      IMNode child = getChild(parent, name);
      if (child == null) {
        return false;
      } else {
        unPin(child);
        return true;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public IMNode getChild(IMNode parent, String name) {
    readLock.lock();
    try {
      IMNode node = parent.getChild(name);
      if (node == null) {
        if (!getCachedMNodeContainer(parent).isVolatile()) {
          node = file.getChildNode(parent, name);
          if (node != null) {
            node.setParent(parent);
            pinMNodeInMemory(node);
            cacheStrategy.updateCacheStatusAfterDiskRead(node);
          }
        }
      } else {
        pinMNodeInMemory(node);
        cacheStrategy.updateCacheStatusAfterMemoryRead(node);
      }

      if (node != null && node.isMeasurement()) {
        processAlias(parent.getAsEntityMNode(), node.getAsMeasurementMNode());
      }

      return node;
    } finally {
      readLock.unlock();
    }
  }

  private void processAlias(IEntityMNode parent, IMeasurementMNode node) {
    String alias = node.getAlias();
    if (alias != null) {
      parent.addAlias(alias, node);
    }
  }

  // get iterator will take readLock, must call iterator.close after usage
  @Override
  public IMNodeIterator getChildrenIterator(IMNode parent) {
    return new CachedMNodeIterator(parent);
  }

  // must pin parent first
  @Override
  public void addChild(IMNode parent, String childName, IMNode child) {
    readLock.lock();
    try {
      child.setParent(parent);
      pinMNodeInMemory(child);
      parent.addChild(childName, child);
      cacheStrategy.updateCacheStatusAfterAppend(child);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void addAlias(IEntityMNode parent, String alias, IMeasurementMNode child) {
    parent.addAlias(alias, child);
  }

  @Override
  public List<IMeasurementMNode> deleteChild(IMNode parent, String childName) {
    readLock.lock();
    try {
      IMNode deletedMNode = getChild(parent, childName);
      // collect all the LeafMNode in this storage group
      List<IMeasurementMNode> leafMNodes = new LinkedList<>();
      Queue<IMNode> queue = new LinkedList<>();
      queue.add(deletedMNode);
      while (!queue.isEmpty()) {
        IMNode node = queue.poll();
        IMNodeIterator iterator = getChildrenIterator(node);
        try {
          IMNode child;
          while (iterator.hasNext()) {
            child = iterator.next();
            if (child.isMeasurement()) {
              leafMNodes.add(child.getAsMeasurementMNode());
            } else {
              queue.add(child);
            }
          }
        } finally {
          iterator.close();
        }
      }

      parent.deleteChild(childName);
      if (cacheStrategy.isCached(deletedMNode)) {
        List<IMNode> removedMNodes = cacheStrategy.remove(deletedMNode);
        for (IMNode removedMNode : removedMNodes) {
          if (cacheStrategy.isPinned(removedMNode)) {
            memManager.releasePinnedMemResource(removedMNode);
          }
          memManager.releaseMemResource(removedMNode);
        }
      }
      if (!getCachedMNodeContainer(parent).isVolatile()) {
        file.deleteMNode(deletedMNode);
      }

      return leafMNodes;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void deleteAliasChild(IEntityMNode parent, String alias) {
    parent.deleteAliasChild(alias);
  }

  // must pin first
  @Override
  public void updateMNode(IMNode node) {
    readLock.lock();
    try {
      cacheStrategy.updateCacheStatusAfterUpdate(node);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void pin(IMNode node) {
    pinMNodeInMemory(node);
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
    if (memManager.isExceedCapacity()) {
      tryExecuteMemoryRelease();
    }
  }

  @Override
  public void unPin(IMNode node) {
    if (!cacheStrategy.isCached(node)) {
      return;
    }
    readLock.lock();
    try {
      if (!cacheStrategy.isPinned(node)) {
        return;
      }
      List<IMNode> releasedMNodes = cacheStrategy.unPinMNode(node);
      for (IMNode releasedMNode : releasedMNodes) {
        memManager.releasePinnedMemResource(releasedMNode);
      }
      if (memManager.isExceedCapacity()) {
        tryExecuteMemoryRelease();
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void createSnapshot() throws IOException {}

  @Override
  public void clear() {
    writeLock.lock();
    try {
      root = null;
      cacheStrategy.clear();
      memManager.clear();
      if (file != null) {
        file.close();
      }
      file = null;
    } finally {
      writeLock.unlock();
    }
  }

  private void tryExecuteMemoryRelease() {
    executeMemoryRelease();
    if (memManager.isExceedCapacity()) {
      if (!hasFlushTask) {
        registerFlushTask();
      }
    }
  }

  private void executeMemoryRelease() {
    List<IMNode> evictedMNodes;
    while (memManager.isExceedThreshold()) {
      evictedMNodes = cacheStrategy.evict();
      if (evictedMNodes.isEmpty()) {
        break;
      }
      for (IMNode evictedMNode : evictedMNodes) {
        memManager.releaseMemResource(evictedMNode);
      }
    }
  }

  private synchronized void registerFlushTask() {
    if (hasFlushTask) {
      return;
    }
    hasFlushTask = true;
    flushTask.submit(this::flushVolatileNodes);
  }

  private void flushVolatileNodes() {
    writeLock.lock();
    try {
      List<IMNode> nodesToPersist = cacheStrategy.collectVolatileMNodes();
      for (IMNode volatileNode : nodesToPersist) {
        file.writeMNode(volatileNode);
        if (cacheStrategy.isCached(volatileNode)) {
          cacheStrategy.updateCacheStatusAfterPersist(volatileNode);
        }
      }
      if (memManager.isExceedCapacity()) {
        executeMemoryRelease();
      }
      hasFlushTask = false;
    } finally {
      writeLock.unlock();
    }
  }

  private class CachedMNodeIterator implements IMNodeIterator {

    IMNode parent;
    Iterator<IMNode> iterator;
    Iterator<IMNode> bufferIterator;
    boolean isIteratingDisk = true;
    boolean loadedFromDisk = true;
    IMNode nextNode;

    CachedMNodeIterator(IMNode parent) {
      readLock.lock();
      try {
        this.parent = parent;
        bufferIterator = getCachedMNodeContainer(parent).getChildrenBufferIterator();
        this.iterator = file.getChildren(parent);
        readNext();
      } catch (Throwable e) {
        readLock.unlock();
        throw e;
      }
    }

    @Override
    public boolean hasNext() {
      if (nextNode != null) {
        return true;
      } else {
        readNext();
        return nextNode != null;
      }
    }

    // must invoke hasNext() first
    @Override
    public IMNode next() {
      if (nextNode == null) {
        throw new NoSuchElementException();
      }
      if (loadedFromDisk) {
        nextNode.setParent(parent);
      }
      pinMNodeInMemory(nextNode);
      if (loadedFromDisk) {
        cacheStrategy.updateCacheStatusAfterDiskRead(nextNode);
      } else {
        cacheStrategy.updateCacheStatusAfterMemoryRead(nextNode);
      }
      IMNode result = nextNode;
      nextNode = null;
      return result;
    }

    private void readNext() {
      IMNode node = null;
      if (isIteratingDisk) {
        ICachedMNodeContainer container = getCachedMNodeContainer(parent);
        if (iterator.hasNext()) {
          node = iterator.next();
          while (container.hasChildInBuffer(node.getName())) {
            if (iterator.hasNext()) {
              node = iterator.next();
            } else {
              node = null;
              break;
            }
          }
        }
        if (node != null) {
          if (parent.hasChild(node.getName())) {
            // this branch means the node load from disk is in cache, thus use the instance in cache
            node = parent.getChild(node.getName());
            loadedFromDisk = false;
          } else {
            loadedFromDisk = true;
          }
          nextNode = node;
          return;
        } else {
          startIteratingBuffer();
        }
      }

      if (iterator.hasNext()) {
        node = iterator.next();
      }
      nextNode = node;
    }

    private void startIteratingBuffer() {
      iterator = bufferIterator;
      isIteratingDisk = false;
      loadedFromDisk = false;
    }

    @Override
    public void close() {
      readLock.unlock();
    }
  }
}
