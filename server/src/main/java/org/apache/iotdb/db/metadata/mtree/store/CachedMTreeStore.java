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
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeContainers;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.MTreeFlushTaskManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.LRUCacheManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManagerHolder;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.path.PartialPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CachedMTreeStore implements IMTreeStore {

  private static final Logger logger = LoggerFactory.getLogger(CachedMTreeStore.class);

  private IMemManager memManager = MemManagerHolder.getMemManagerInstance();

  private ICacheManager cacheManager = new LRUCacheManager();

  private ISchemaFile file;

  private IMNode root;

  private MTreeFlushTaskManager flushTaskManager = MTreeFlushTaskManager.getInstance();
  private int flushCount = 0;
  private volatile boolean hasFlushTask = false;

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); // default writer preferential
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  @Override
  public void init(PartialPath rootPath, boolean isStorageGroup)
      throws MetadataException, IOException {
    if (!isStorageGroup) {
      throw new MetadataException("CachedMTreeStore only support subTree below storage group");
    }
    MNodeContainers.IS_DISK_MODE = true;
    memManager.init();
    file = SchemaFile.initSchemaFile(rootPath.getFullPath());
    root = file.init();
    cacheManager.initRootStatus(root);
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
      if (node == null) {
        node = loadChildFromDisk(parent, name);
      } else {
        try {
          cacheManager.updateCacheStatusAfterMemoryRead(node);
          ensureMemoryStatus();
        } catch (MNodeNotCachedException e) {
          node = loadChildFromDisk(parent, name);
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
    IMNode node = null;
    if (!getCachedMNodeContainer(parent).isVolatile()) {
      try {
        node = file.getChildNode(parent, name);
      } catch (MetadataException e) {
        // todo fix this exception handler
      } catch (IOException e) {
        throw new MetadataException(e);
      }
      if (node != null) {
        loadChildFromDiskToParent(parent, node);
      }
    }
    return node;
  }

  private IMNode loadChildFromDiskToParent(IMNode parent, IMNode node) {
    synchronized (parent) {
      IMNode nodeAlreadyLoaded = parent.getChild(node.getName());
      if (nodeAlreadyLoaded != null) {
        try {
          cacheManager.updateCacheStatusAfterMemoryRead(nodeAlreadyLoaded);
          ensureMemoryStatus();
          return nodeAlreadyLoaded;
        } catch (MNodeNotCachedException ignored) {
          // the nodeAlreadyLoaded is evicted and use the node read from disk
        }
      }
      node.setParent(parent);
      cacheManager.updateCacheStatusAfterDiskRead(node);
      ensureMemoryStatus();
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
  public IMNode addChild(IMNode parent, String childName, IMNode child) {
    readLock.lock();
    try {
      child.setParent(parent);
      cacheManager.updateCacheStatusAfterAppend(child);
      ensureMemoryStatus();
      return parent.getChild(childName);
    } finally {
      readLock.unlock();
    }
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
   * @throws MetadataException
   */
  @Override
  public void deleteChild(IMNode parent, String childName) throws MetadataException {
    writeLock.lock();
    try {
      IMNode deletedMNode = getChild(parent, childName);
      ICachedMNodeContainer container = getCachedMNodeContainer(parent);
      if (!container.isVolatile() && !container.hasChildInNewChildBuffer(childName)) {
        // the container has been persisted and this child is not a new child, which means the child
        // has been persisted and should be deleted from disk
        try {
          file.delete(deletedMNode);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }

      parent.deleteChild(childName);
      cacheManager.remove(deletedMNode);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void updateStorageGroupMNode(IStorageGroupMNode node) throws MetadataException {
    this.root = node;
    writeLock.lock();
    try {
      file.updateStorageGroupNode(node);
    } catch (IOException e) {
      logger.error("IOException occurred during updating StorageGroupMNode {}", node.getFullPath());
      throw new MetadataException(e);
    } finally {
      writeLock.unlock();
    }
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
      cacheManager.updateCacheStatusAfterUpdate(node);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Currently, this method is only used for pin node get from mNodeCache. Pin MNode in memory makes
   * the pinned node and its ancestors not be evicted during cache eviction. The pinned MNode will
   * occupy memory resource, thus this method will check the memory status which may trigger cache
   * eviction or flushing.
   *
   * @param node
   */
  @Override
  public void pin(IMNode node) throws MetadataException {
    readLock.lock();
    try {
      cacheManager.pinMNode(node);
      ensureMemoryStatus();
    } finally {
      readLock.unlock();
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
    readLock.lock();
    try {
      cacheManager.unPinMNode(node);
      ensureMemoryStatus();
    } finally {
      readLock.unlock();
    }
  }

  /** clear all the data of MTreeStore in memory and disk. */
  @Override
  public void clear() {
    root = null;
    cacheManager.clear();
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

  private void ensureMemoryStatus() {
    if (memManager.isExceedCapacity()) {
      tryExecuteMemoryRelease();
    }
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
   * Keep fetching evictable nodes from cacheManager until the memory status is under safe mode or
   * no node could be evicted. Update the memory status after evicting each node.
   */
  private void executeMemoryRelease() {
    while (memManager.isExceedThreshold()) {
      if (!cacheManager.evict()) {
        break;
      }
    }
  }

  private synchronized void registerFlushTask() {
    if (hasFlushTask) {
      return;
    }
    hasFlushTask = true;
    flushTaskManager.submit(this::flushVolatileNodes);
  }

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  private void flushVolatileNodes() {
    writeLock.lock();
    try {
      List<IMNode> nodesToPersist = cacheManager.collectVolatileMNodes();
      for (IMNode volatileNode : nodesToPersist) {
        try {
          file.writeMNode(volatileNode);
        } catch (MetadataException | IOException e) {
          logger.error(
              "Error occurred during MTree flush, current node is {}",
              volatileNode.getFullPath(),
              e);
          return;
        }
        cacheManager.updateCacheStatusAfterPersist(volatileNode);
      }
      ensureMemoryStatus();
      hasFlushTask = false;
      flushCount++;
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
            try {
              cacheManager.updateCacheStatusAfterMemoryRead(nodeInMem);
              ensureMemoryStatus();
              node = nodeInMem;
            } catch (MNodeNotCachedException e) {
              node = loadChildFromDiskToParent(parent, node);
            }
          } else {
            node = loadChildFromDiskToParent(parent, node);
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
        cacheManager.updateCacheStatusAfterMemoryRead(node);
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
