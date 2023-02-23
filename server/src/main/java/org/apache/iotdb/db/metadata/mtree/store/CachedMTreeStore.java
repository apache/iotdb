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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.metadata.mnode.AboveDatabaseMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.estimator.IMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.CachedTraverserIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheMemoryManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.template.Template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CachedMTreeStore implements IMTreeStore {

  private static final Logger logger = LoggerFactory.getLogger(CachedMTreeStore.class);

  private final MemManager memManager;

  private final ICacheManager cacheManager;

  private ISchemaFile file;

  private IMNode root;

  private final Runnable flushCallback;

  private final CachedSchemaRegionStatistics regionStatistics;

  private final StampedWriterPreferredLock lock = new StampedWriterPreferredLock();

  public CachedMTreeStore(
      PartialPath storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Runnable flushCallback)
      throws MetadataException, IOException {
    file = SchemaFile.initSchemaFile(storageGroup.getFullPath(), schemaRegionId);
    root = file.init();
    this.regionStatistics = regionStatistics;
    this.memManager = new MemManager(regionStatistics);
    this.flushCallback = flushCallback;
    this.cacheManager = CacheMemoryManager.getInstance().createLRUCacheManager(this, memManager);
    cacheManager.initRootStatus(root);
    regionStatistics.setCacheManager(cacheManager);
    ensureMemoryStatus();
  }

  @Override
  public IMNode generatePrefix(PartialPath storageGroupPath) {
    String[] nodes = storageGroupPath.getNodes();
    // nodes[0] must be root
    IMNode res = new AboveDatabaseMNode(null, nodes[0]);
    IMNode cur = res;
    IMNode child;
    for (int i = 1; i < nodes.length - 1; i++) {
      child = new AboveDatabaseMNode(cur, nodes[i]);
      cur.addChild(nodes[i], child);
      cur = child;
    }
    root.setParent(cur);
    cur.addChild(root);
    return res;
  }

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) throws MetadataException {
    return hasChild(parent, name, true);
  }

  protected final boolean hasChild(IMNode parent, String name, boolean needLock)
      throws MetadataException {
    if (needLock) {
      lock.threadReadLock();
    }
    try {
      IMNode child = getChild(parent, name, needLock);
      if (child == null) {
        return false;
      } else {
        unPin(child);
        return true;
      }
    } finally {
      if (needLock) {
        lock.threadReadUnlock();
      }
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
    return getChild(parent, name, true);
  }

  protected final IMNode getChild(IMNode parent, String name, boolean needLock)
      throws MetadataException {
    if (needLock) {
      lock.threadReadLock();
    }
    try {
      IMNode node = parent.getChild(name);
      if (node == null) {
        node = loadChildFromDisk(parent, name);
      } else {
        try {
          cacheManager.updateCacheStatusAfterMemoryRead(node);
        } catch (MNodeNotCachedException e) {
          node = loadChildFromDisk(parent, name);
        }
      }
      if (node != null && node.isMeasurement()) {
        processAlias(parent.getAsEntityMNode(), node.getAsMeasurementMNode());
      }

      return node;
    } finally {
      if (needLock) {
        lock.threadReadUnlock();
      }
    }
  }

  private IMNode loadChildFromDisk(IMNode parent, String name) throws MetadataException {
    IMNode node = null;
    if (!getCachedMNodeContainer(parent).isVolatile()) {
      try {
        node = file.getChildNode(parent, name);
      } catch (IOException e) {
        throw new MetadataException(e);
      }
      if (node != null) {
        node = loadChildFromDiskToParent(parent, node);
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
    return getChildrenIterator(parent, true);
  }

  final IMNodeIterator getChildrenIterator(IMNode parent, boolean needLock)
      throws MetadataException {
    try {
      return new CachedMNodeIterator(parent, needLock);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public IMNodeIterator getTraverserIterator(
      IMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    return getTraverserIterator(this, parent, templateMap, skipPreDeletedSchema);
  }

  final IMNodeIterator getTraverserIterator(
      IMTreeStore store,
      IMNode parent,
      Map<Integer, Template> templateMap,
      boolean skipPreDeletedSchema)
      throws MetadataException {
    if (parent.isEntity()) {
      AbstractTraverserIterator iterator =
          new CachedTraverserIterator(store, parent.getAsEntityMNode(), templateMap);
      iterator.setSkipPreDeletedSchema(skipPreDeletedSchema);
      return iterator;
    } else {
      return store.getChildrenIterator(parent);
    }
  }

  // must pin parent first
  @Override
  public IMNode addChild(IMNode parent, String childName, IMNode child) {
    lock.threadReadLock();
    try {
      child.setParent(parent);
      cacheManager.updateCacheStatusAfterAppend(child);
      ensureMemoryStatus();
      return parent.getChild(childName);
    } finally {
      lock.threadReadUnlock();
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
    lock.writeLock();
    try {
      IMNode deletedMNode = getChild(parent, childName, false);
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
      lock.unlockWrite();
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
    updateMNode(node, true);
  }

  final void updateMNode(IMNode node, boolean needLock) {
    if (needLock) {
      lock.threadReadLock();
    }
    try {
      cacheManager.updateCacheStatusAfterUpdate(node);
    } finally {
      if (needLock) {
        lock.threadReadUnlock();
      }
    }
  }

  @Override
  public IEntityMNode setToEntity(IMNode node) {
    IEntityMNode result = MNodeUtils.setToEntity(node);
    if (result != node) {
      memManager.updatePinnedSize(IMNodeSizeEstimator.getEntityNodeBaseSize());
    }
    updateMNode(result);
    return result;
  }

  @Override
  public IMNode setToInternal(IEntityMNode entityMNode) {
    IMNode result = MNodeUtils.setToInternal(entityMNode);
    if (result != entityMNode) {
      memManager.updatePinnedSize(-IMNodeSizeEstimator.getEntityNodeBaseSize());
    }
    updateMNode(result);
    return result;
  }

  @Override
  public void setAlias(IMeasurementMNode measurementMNode, String alias) throws MetadataException {
    String existingAlias = measurementMNode.getAlias();
    if (existingAlias == null && alias == null) {
      return;
    }

    measurementMNode.setAlias(alias);
    updateMNode(measurementMNode);

    if (existingAlias != null && alias != null) {
      memManager.updatePinnedSize(alias.length() - existingAlias.length());
    } else if (alias == null) {
      memManager.updatePinnedSize(
          -(IMNodeSizeEstimator.getAliasBaseSize() + existingAlias.length()));
    } else {
      memManager.updatePinnedSize(IMNodeSizeEstimator.getAliasBaseSize() + alias.length());
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
    pin(node, true);
  }

  final void pin(IMNode node, boolean needLock) throws MetadataException {
    if (needLock) {
      lock.threadReadLock();
    }
    try {
      cacheManager.pinMNode(node);
    } finally {
      if (needLock) {
        lock.threadReadUnlock();
      }
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
    unPin(node, true);
  }

  final void unPin(IMNode node, boolean needLock) {
    if (needLock) {
      lock.threadReadLock();
    }
    try {
      if (cacheManager.unPinMNode(node)) {
        ensureMemoryStatus();
      }
    } finally {
      if (needLock) {
        lock.threadReadUnlock();
      }
    }
  }

  @Override
  public void unPinPath(IMNode node) {
    unPinPath(node, true);
  }

  public void unPinPath(IMNode node, boolean needLock) {
    while (!node.isStorageGroup()) {
      unPin(node, needLock);
      node = node.getParent();
    }
  }

  final long stampedReadLock() {
    return lock.stampedReadLock();
  }

  final void stampedReadUnlock(long stamp) {
    lock.stampedReadUnlock(stamp);
  }

  @Override
  public IMTreeStore getWithReentrantReadLock() {
    return new ReentrantReadOnlyCachedMTreeStore(this);
  }

  /** clear all the data of MTreeStore in memory and disk. */
  @Override
  public void clear() {
    lock.writeLock();
    try {
      regionStatistics.setCacheManager(null);
      cacheManager.clear(root);
      root = null;
      if (file != null) {
        try {
          file.clear();
          file.close();
        } catch (MetadataException | IOException e) {
          logger.error(String.format("Error occurred during SchemaFile clear, %s", e.getMessage()));
        }
      }
      file = null;
    } finally {
      lock.unlockWrite();
    }
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    lock.writeLock();
    try {
      flushVolatileNodes();
      ensureMemoryStatus();
      return file.createSnapshot(snapshotDir);
    } finally {
      lock.unlockWrite();
    }
  }

  public static CachedMTreeStore loadFromSnapshot(
      File snapshotDir,
      String storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Runnable flushCallback)
      throws IOException, MetadataException {
    return new CachedMTreeStore(
        snapshotDir, storageGroup, schemaRegionId, regionStatistics, flushCallback);
  }

  private CachedMTreeStore(
      File snapshotDir,
      String storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Runnable flushCallback)
      throws IOException, MetadataException {
    file = SchemaFile.loadSnapshot(snapshotDir, storageGroup, schemaRegionId);
    root = file.init();
    this.regionStatistics = regionStatistics;
    this.memManager = new MemManager(regionStatistics);
    this.flushCallback = flushCallback;
    this.cacheManager = CacheMemoryManager.getInstance().createLRUCacheManager(this, memManager);
    cacheManager.initRootStatus(root);
    regionStatistics.setCacheManager(cacheManager);
    ensureMemoryStatus();
  }

  private void ensureMemoryStatus() {
    CacheMemoryManager.getInstance().ensureMemoryStatus();
  }

  public StampedWriterPreferredLock getLock() {
    return lock;
  }

  public CachedSchemaRegionStatistics getRegionStatistics() {
    return regionStatistics;
  }

  /**
   * Fetching evictable nodes from cacheManager. Update the memory status after evicting each node.
   *
   * @return should not continue releasing
   */
  public boolean executeMemoryRelease() {
    if (regionStatistics.getUnpinnedMemorySize() != 0) {
      return !cacheManager.evict();
    } else {
      return true;
    }
  }

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  public void flushVolatileNodes() {
    try {
      IStorageGroupMNode updatedStorageGroupMNode = cacheManager.collectUpdatedStorageGroupMNodes();
      if (updatedStorageGroupMNode != null) {
        try {
          file.updateStorageGroupNode(updatedStorageGroupMNode);
        } catch (IOException e) {
          logger.error(
              "IOException occurred during updating StorageGroupMNode {}",
              updatedStorageGroupMNode.getFullPath(),
              e);
          return;
        }
      }
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
      if (updatedStorageGroupMNode != null || !nodesToPersist.isEmpty()) {
        flushCallback.run();
      }
    } catch (Throwable e) {
      logger.error(
          "Error occurred during MTree flush, current SchemaRegionId is {}",
          regionStatistics.getSchemaRegionId(),
          e);
      e.printStackTrace();
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
    boolean isLocked;

    CachedMNodeIterator(IMNode parent, boolean needLock) throws MetadataException, IOException {
      if (needLock) {
        lock.threadReadLock();
      }
      isLocked = true;
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
        if (needLock) {
          lock.threadReadUnlock();
        }
        isLocked = false;
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
      if (nextNode == null && !hasNext()) {
        throw new NoSuchElementException();
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
          IMNode nodeInMem = parent.getChild(node.getName());
          if (nodeInMem != null) {
            // this branch means the node load from disk is in cache, thus use the instance in
            // cache
            try {
              cacheManager.updateCacheStatusAfterMemoryRead(nodeInMem);
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
        if (isLocked) {
          lock.threadReadUnlock();
        }
      }
    }
  }
}
