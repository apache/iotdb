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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeContainers;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.LRUCacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFileManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SFManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CachedMTreeStore implements IMTreeStore {

  private static final Logger logger = LoggerFactory.getLogger(CachedMTreeStore.class);

  private IMemManager memManager = new MemManager();

  private ICacheStrategy cacheStrategy = new LRUCacheStrategy();

  private ISchemaFileManager file;

  private IMNode root;

  private ExecutorService flushTask;
  private boolean hasFlushTask = false;

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); // default writer preferential
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  @Override
  public void init() throws MetadataException, IOException {
    MNodeContainers.IS_DISK_MODE = true;
    file = SFManager.getInstance();
    root = file.init();
    cacheStrategy.pinMNode(root);
    flushTask = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("MTreeFlushThread");
  }

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) throws MetadataException {
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

  /**
   * Get the target child node from parent. The parent must be pinned before invoking this method.
   * The method will try to get child node from cache. If there's no matched node in cache or the
   * node is not cached, which means it has been evicted, then this method will retrieve child node
   * from schemaFile The returned child node will be pinned. If there's no matched child with the
   * given name, this method will return null.
   *
   * @param parent parent node
   * @param name the name or alias of the target child
   * @return the pinned child node
   * @throws MetadataException
   */
  @Override
  public IMNode getChild(IMNode parent, String name) throws MetadataException {
    readLock.lock();
    try {
      IMNode node = parent.getChild(name);
      if (node == null || !cacheStrategy.isCached(node)) {
        node = loadChildFromDisk(parent, name);
        if (node == null) {
          logger.info(
              "There's no child in memory or disk. The child is {}",
              parent.getFullPath() + "." + name);
        }
      } else {
        // the operation that changes the node's cache status should be synchronized
        synchronized (node) {
          if (cacheStrategy.isCached(node)) {
            pinMNodeInMemory(node);
            cacheStrategy.updateCacheStatusAfterMemoryRead(node);
          }
        }
        if (!cacheStrategy.isPinned(node)) {
          node = loadChildFromDisk(parent, name);
          logger.info(
              "The child has been concurrently evicted, thus try load from disk. The child is {}. {}",
              parent.getFullPath() + "." + name,
              node == null);
        }
      }
      if (node != null && node.isMeasurement()) {
        processAlias(parent.getAsEntityMNode(), node.getAsMeasurementMNode());
      }

      return node;
    } finally {
      readLock.unlock();
    }
  }

  private IMNode loadChildFromDisk(IMNode parent, String name) throws MetadataException {
    synchronized (parent) {
      IMNode node = null;
      if (!getCachedMNodeContainer(parent).isVolatile()) {
        try {
          node = file.getChildNode(parent, name);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
        if (node != null) {
          node.setParent(parent);
          pinMNodeInMemory(node);
          cacheStrategy.updateCacheStatusAfterDiskRead(node);
        }
      }
      return node;
    }
  }

  private void processAlias(IEntityMNode parent, IMeasurementMNode node) {
    String alias = node.getAlias();
    if (alias != null) {
      parent.addAlias(alias, node);
    }
  }

  // getChildrenIterator will take readLock, must call iterator.close() after usage
  @Override
  public IMNodeIterator getChildrenIterator(IMNode parent) throws MetadataException {
    try {
      return new CachedMNodeIterator(parent);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
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

  /**
   * This method will delete a node from MTree, which means the corresponding subTree will be
   * deleted. Before deletion, the measurementMNode in this subtree should be collected for updating
   * statistics in MManager. The deletion will delete subtree in schemaFile first and then delete
   * the node from memory. The target node and its ancestors should be pinned before invoking this
   * problem.
   *
   * @param parent the parent node of the target node
   * @param childName the name of the target node
   * @return the collected MeasurementMNode in the deleted subtree
   * @throws MetadataException
   */
  @Override
  public List<IMeasurementMNode> deleteChild(IMNode parent, String childName)
      throws MetadataException {
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

      ICachedMNodeContainer container = getCachedMNodeContainer(parent);
      if (!container.isVolatile() && !container.hasChildInNewChildBuffer(childName)) {
        // the container has been persisted and this child is not a new child, which means the child
        // has been persisted and should be deleted from disk
        try {
          file.deleteMNode(deletedMNode);
        } catch (IOException e) {
          throw new MetadataException(e);
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

      return leafMNodes;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void deleteAliasChild(IEntityMNode parent, String alias) {
    parent.deleteAliasChild(alias);
  }

  /**
   * The upside modification on node in MTree or MManager should be sync to MTreeStore explicitly.
   * Must pin the node first before update
   *
   * @param node the modified node
   */
  @Override
  public void updateMNode(IMNode node) {
    readLock.lock();
    try {
      cacheStrategy.updateCacheStatusAfterUpdate(node);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Pin MNode in memory makes the pinned node and its ancestors not be evicted during cache
   * eviction. The pinned MNode will occupy memory resource, thus this method will check the memory
   * status which may trigger cache eviction or flushing.
   *
   * @param node
   */
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

  /**
   * UnPin MNode release the node from this thread/task's usage. If none thread/task is using this
   * node or pinning this node, it will be able to evict this node from memory. Since unpin changes
   * the node's status, the memory status will be checked and cache eviction and flushing may be
   * tirgger.
   *
   * @param node
   */
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

  /** clear all the data of MTreeStore in memory and disk. */
  @Override
  public void clear() {
    if (flushTask != null) {
      flushTask.shutdown();
      flushTask = null;
    }
    root = null;
    cacheStrategy.clear();
    memManager.clear();
    if (file != null) {
      try {
        file.clear();
        file.close();
      } catch (MetadataException | IOException e) {
        logger.error(String.format("Error occurred during SchemaFile clear, %s", e.getMessage()));
      }
    }
    file = null;
  }

  /**
   * Execute cache eviction until the memory status is under safe mode or no node could be evicted.
   * If the memory status is still full, which means the nodes in memory are all volatile nodes, new
   * added or updated, fire flush task.
   */
  private void tryExecuteMemoryRelease() {
    executeMemoryRelease();
    if (memManager.isExceedCapacity()) {
      if (!hasFlushTask) {
        registerFlushTask();
      }
    }
  }

  /**
   * Keep fetching evictable nodes from cacheStrategy until the memory status is under safe mode or
   * no node could be evicted. Update the memory status after evicting each node.
   */
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

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  private void flushVolatileNodes() {
    writeLock.lock();
    try {
      List<IMNode> nodesToPersist = cacheStrategy.collectVolatileMNodes();
      for (IMNode volatileNode : nodesToPersist) {
        try {
          file.writeMNode(volatileNode);
        } catch (MetadataException | IOException e) {
          logger.error(String.format("Error occurred during MTree flush, %s", e.getMessage()));
          return;
        }
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

  /**
   * Since any node R/W operation may change the memory status, thus it should be controlled during
   * iterating child nodes.
   */
  private class CachedMNodeIterator implements IMNodeIterator {

    IMNode parent;
    Iterator<IMNode> iterator;
    Iterator<IMNode> bufferIterator;
    boolean isIteratingDisk;
    IMNode nextNode;

    CachedMNodeIterator(IMNode parent) throws MetadataException, IOException {
      readLock.lock();
      try {
        this.parent = parent;
        ICachedMNodeContainer container = getCachedMNodeContainer(parent);
        bufferIterator = container.getChildrenBufferIterator();
        if (!container.isVolatile()) {
          this.iterator = file.getChildren(parent);
          isIteratingDisk = true;
        } else {
          iterator = bufferIterator;
          isIteratingDisk = false;
        }

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
        try {
          readNext();
        } catch (MetadataException e) {
          logger.error(String.format("Error occurred during readNext, %s", e.getMessage()));
          return false;
        }
        return nextNode != null;
      }
    }

    // must invoke hasNext() first
    @Override
    public IMNode next() {
      if (nextNode == null) {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
      }
      IMNode result = nextNode;
      nextNode = null;
      return result;
    }

    private void readNext() throws MetadataException {
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
            // this branch means the node load from disk is in cache, thus use the instance in
            // cache
            IMNode nodeInMem = parent.getChild(node.getName());
            synchronized (nodeInMem) {
              if (cacheStrategy.isCached(nodeInMem)) {
                pinMNodeInMemory(nodeInMem);
                cacheStrategy.updateCacheStatusAfterMemoryRead(nodeInMem);
              }
            }
            if (cacheStrategy.isPinned(nodeInMem)) {
              node = nodeInMem;
            } else {
              synchronized (parent) {
                node.setParent(parent);
                pinMNodeInMemory(node);
                cacheStrategy.updateCacheStatusAfterDiskRead(node);
              }
            }
          } else {
            synchronized (parent) {
              node.setParent(parent);
              pinMNodeInMemory(node);
              cacheStrategy.updateCacheStatusAfterDiskRead(node);
            }
          }
          nextNode = node;
          return;
        } else {
          startIteratingBuffer();
        }
      }

      if (iterator.hasNext()) {
        node = iterator.next();
        // node in buffer won't be evicted during Iteration
        pinMNodeInMemory(node);
        cacheStrategy.updateCacheStatusAfterMemoryRead(node);
      }
      nextNode = node;
    }

    private void startIteratingBuffer() {
      iterator = bufferIterator;
      isIteratingDisk = false;
    }

    @Override
    public void close() {
      try {
        if (nextNode != null) {
          unPin(nextNode);
          nextNode = null;
        }
      } finally {
        readLock.unlock();
      }
    }
  }
}
