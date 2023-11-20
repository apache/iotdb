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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.estimator.MNodeSizeEstimator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.CacheMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.ICacheManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush.PBTreeFlushExecutor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.MemManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.iterator.CachedTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MNodeUtils;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CachedMTreeStore implements IMTreeStore<ICachedMNode> {

  private static final Logger logger = LoggerFactory.getLogger(CachedMTreeStore.class);

  private final int schemaRegionId;

  private final MemManager memManager;
  private final ICacheManager cacheManager;
  private ISchemaFile file;
  private ICachedMNode root;
  private final Runnable flushCallback;
  private final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
  private final CachedSchemaRegionStatistics regionStatistics;
  private final LockManager lockManager = new LockManager();

  public CachedMTreeStore(
      PartialPath storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Runnable flushCallback)
      throws MetadataException, IOException {
    this.schemaRegionId = schemaRegionId;
    file = SchemaFile.initSchemaFile(storageGroup.getFullPath(), schemaRegionId);
    root = file.init();
    this.regionStatistics = regionStatistics;
    this.memManager = new MemManager(regionStatistics);
    this.flushCallback = flushCallback;
    this.cacheManager =
        CacheMemoryManager.getInstance().createLRUCacheManager(this, memManager, lockManager);
    cacheManager.initRootStatus(root);
    regionStatistics.setCacheManager(cacheManager);
    ensureMemoryStatus();
  }

  @Override
  public ICachedMNode generatePrefix(PartialPath storageGroupPath) {
    String[] nodes = storageGroupPath.getNodes();
    // nodes[0] must be root
    ICachedMNode res = nodeFactory.createAboveDatabaseMNode(null, nodes[0]);
    ICachedMNode cur = res;
    ICachedMNode child;
    for (int i = 1; i < nodes.length - 1; i++) {
      child = nodeFactory.createAboveDatabaseMNode(cur, nodes[i]);
      cur.addChild(nodes[i], child);
      cur = child;
    }
    root.setParent(cur);
    cur.addChild(root);
    return res;
  }

  @Override
  public ICachedMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(ICachedMNode parent, String name) throws MetadataException {
    return hasChild(parent, name, true);
  }

  protected final boolean hasChild(ICachedMNode parent, String name, boolean needLock)
      throws MetadataException {
    if (needLock) {
      lockManager.threadReadLock(parent);
    }
    try {
      ICachedMNode child = getChild(parent, name, needLock);
      if (child == null) {
        return false;
      } else {
        unPin(child);
        return true;
      }
    } finally {
      if (needLock) {
        lockManager.threadReadUnlock(parent);
      }
    }
  }

  /**
   * Get the target child node from parent. The parent must be pinned before invoking this method.
   * The method will try to get child node from cache. If there's no matched node in cache or the
   * node is not cached, which means it has been evicted, then this method will retrieve child node
   * from PBTree The returned child node will be pinned. If there's no matched child with the given
   * name, this method will return null.
   *
   * @param parent parent node
   * @param name the name or alias of the target child
   * @return the pinned child node
   * @throws MetadataException
   */
  @Override
  public ICachedMNode getChild(ICachedMNode parent, String name) throws MetadataException {
    return getChild(parent, name, true);
  }

  protected final ICachedMNode getChild(ICachedMNode parent, String name, boolean needLock)
      throws MetadataException {
    if (needLock) {
      lockManager.threadReadLock(parent);
    }
    try {
      ICachedMNode node = parent.getChild(name);
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
        processAlias(parent.getAsDeviceMNode(), node.getAsMeasurementMNode());
      }

      return node;
    } finally {
      if (needLock) {
        lockManager.threadReadUnlock(parent);
      }
    }
  }

  private ICachedMNode loadChildFromDisk(ICachedMNode parent, String name)
      throws MetadataException {
    ICachedMNode node = null;
    if (!ICachedMNodeContainer.getCachedMNodeContainer(parent).isVolatile()) {
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

  @SuppressWarnings("java:S2445")
  private ICachedMNode loadChildFromDiskToParent(ICachedMNode parent, ICachedMNode node) {
    synchronized (parent) {
      ICachedMNode nodeAlreadyLoaded = parent.getChild(node.getName());
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

  private void processAlias(
      IDeviceMNode<ICachedMNode> parent, IMeasurementMNode<ICachedMNode> node) {
    String alias = node.getAlias();
    if (alias != null) {
      parent.addAlias(alias, node);
    }
  }

  // getChildrenIterator will take readLock, must call iterator.close() after usage
  @Override
  public IMNodeIterator<ICachedMNode> getChildrenIterator(ICachedMNode parent)
      throws MetadataException {
    return getChildrenIterator(parent, true);
  }

  final IMNodeIterator<ICachedMNode> getChildrenIterator(ICachedMNode parent, boolean needLock)
      throws MetadataException {
    try {
      return new CachedMNodeIterator(parent, needLock);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public IMNodeIterator<ICachedMNode> getTraverserIterator(
      ICachedMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    return getTraverserIterator(this, parent, templateMap, skipPreDeletedSchema);
  }

  final IMNodeIterator<ICachedMNode> getTraverserIterator(
      IMTreeStore<ICachedMNode> store,
      ICachedMNode parent,
      Map<Integer, Template> templateMap,
      boolean skipPreDeletedSchema)
      throws MetadataException {
    if (parent.isDevice()) {
      AbstractTraverserIterator<ICachedMNode> iterator =
          new CachedTraverserIterator(store, parent.getAsDeviceMNode(), templateMap, nodeFactory);
      iterator.setSkipPreDeletedSchema(skipPreDeletedSchema);
      return iterator;
    } else {
      return store.getChildrenIterator(parent);
    }
  }

  // must pin parent first
  @Override
  public ICachedMNode addChild(ICachedMNode parent, String childName, ICachedMNode child) {
    lockManager.threadReadLock(parent);
    try {
      child.setParent(parent);
      cacheManager.updateCacheStatusAfterAppend(child);
      ensureMemoryStatus();
      return parent.getChild(childName);
    } finally {
      lockManager.threadReadUnlock(parent);
    }
  }

  /**
   * This method will delete a node from MTree, which means the corresponding subTree will be
   * deleted. Before deletion, the measurementMNode in this subtree should be collected for updating
   * statistics in MManager. The deletion will delete subtree in PBTree first and then delete the
   * node from memory. The target node and its ancestors should be pinned before invoking this
   * problem.
   *
   * @param parent the parent node of the target node
   * @param childName the name of the target node
   * @throws MetadataException
   */
  @Override
  public void deleteChild(ICachedMNode parent, String childName) throws MetadataException {
    lockManager.writeLock(parent);
    try {
      ICachedMNode deletedMNode = getChild(parent, childName, false);
      ICachedMNodeContainer container = ICachedMNodeContainer.getCachedMNodeContainer(parent);
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
      lockManager.writeUnlock(parent);
    }
  }

  /**
   * The upside modification on node in MTree or MManager should be sync to MTreeStore explicitly.
   * Must pin the node first before update
   *
   * @param node the modified node
   */
  @Override
  public void updateMNode(ICachedMNode node, Consumer<ICachedMNode> operation) {
    updateMNode(node, operation, true);
  }

  final void updateMNode(ICachedMNode node, Consumer<ICachedMNode> operation, boolean needLock) {
    if (needLock && !node.isDatabase()) {
      lockManager.threadReadLock(node.getParent());
    }
    try {
      operation.accept(node);
      cacheManager.updateCacheStatusAfterUpdate(node);
    } finally {
      if (needLock && !node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
      }
    }
  }

  @Override
  public IDeviceMNode<ICachedMNode> setToEntity(ICachedMNode node) {
    AtomicReference<IDeviceMNode<ICachedMNode>> resultReference = new AtomicReference<>(null);
    updateMNode(
        node,
        o -> {
          IDeviceMNode<ICachedMNode> result = MNodeUtils.setToEntity(node, nodeFactory);
          resultReference.getAndSet(result);
        });

    IDeviceMNode<ICachedMNode> result = resultReference.get();
    if (result != node) {
      regionStatistics.addDevice();
      memManager.updatePinnedSize(result.estimateSize() - node.estimateSize());
    }

    return result;
  }

  @Override
  public ICachedMNode setToInternal(IDeviceMNode<ICachedMNode> entityMNode) {
    AtomicReference<ICachedMNode> resultReference = new AtomicReference<>(null);
    updateMNode(
        entityMNode.getAsMNode(),
        o -> {
          ICachedMNode result = MNodeUtils.setToInternal(entityMNode, nodeFactory);
          resultReference.getAndSet(result);
        });

    ICachedMNode result = resultReference.get();
    if (result != entityMNode) {
      regionStatistics.deleteDevice();
      memManager.updatePinnedSize(result.estimateSize() - entityMNode.estimateSize());
    }

    return result;
  }

  @Override
  public void setAlias(IMeasurementMNode<ICachedMNode> measurementMNode, String alias)
      throws MetadataException {
    String existingAlias = measurementMNode.getAlias();
    if (existingAlias == null && alias == null) {
      return;
    }

    updateMNode(measurementMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setAlias(alias));

    if (existingAlias != null && alias != null) {
      memManager.updatePinnedSize(alias.length() - existingAlias.length());
    } else if (alias == null) {
      memManager.updatePinnedSize(
          -(MNodeSizeEstimator.getAliasBaseSize() + existingAlias.length()));
    } else {
      memManager.updatePinnedSize(MNodeSizeEstimator.getAliasBaseSize() + alias.length());
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
  public void pin(ICachedMNode node) throws MetadataException {
    pin(node, true);
  }

  final void pin(ICachedMNode node, boolean needLock) throws MetadataException {
    if (needLock && !node.isDatabase()) {
      lockManager.threadReadLock(node.getParent());
    }
    try {
      cacheManager.pinMNode(node);
    } finally {
      if (needLock && !node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
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
  public void unPin(ICachedMNode node) {
    unPin(node, true);
  }

  final void unPin(ICachedMNode node, boolean needLock) {
    if (needLock && !node.isDatabase()) {
      lockManager.threadReadLock(node.getParent());
    }
    try {
      if (cacheManager.unPinMNode(node)) {
        ensureMemoryStatus();
      }
    } finally {
      if (needLock && !node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
      }
    }
  }

  @Override
  public void unPinPath(ICachedMNode node) {
    unPinPath(node, true);
  }

  public void unPinPath(ICachedMNode node, boolean needLock) {
    while (!node.isDatabase()) {
      unPin(node, needLock);
      node = node.getParent();
    }
  }

  @Override
  public IMTreeStore<ICachedMNode> getWithReentrantReadLock() {
    return new ReentrantReadOnlyCachedMTreeStore(this);
  }

  /** clear all the data of MTreeStore in memory and disk. */
  @Override
  public void clear() {
    lockManager.globalWriteLock();
    try {
      CacheMemoryManager.getInstance().clearCachedMTreeStore(this);
      regionStatistics.setCacheManager(null);
      cacheManager.clear(root);
      root = null;
      if (file != null) {
        try {
          file.clear();
          file.close();
        } catch (MetadataException | IOException e) {
          logger.error(String.format("Error occurred during PBTree clear, %s", e.getMessage()));
        }
      }
      file = null;
    } finally {
      lockManager.globalWriteUnlock();
    }
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    lockManager.globalWriteLock();
    try {
      flushVolatileNodes(false);
      return file.createSnapshot(snapshotDir);
    } finally {
      lockManager.globalWriteUnlock();
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
    this.schemaRegionId = schemaRegionId;
    file = SchemaFile.loadSnapshot(snapshotDir, storageGroup, schemaRegionId);
    root = file.init();
    this.regionStatistics = regionStatistics;
    this.memManager = new MemManager(regionStatistics);
    this.flushCallback = flushCallback;
    this.cacheManager =
        CacheMemoryManager.getInstance().createLRUCacheManager(this, memManager, lockManager);
    cacheManager.initRootStatus(root);
    regionStatistics.setCacheManager(cacheManager);
    ensureMemoryStatus();
  }

  private void ensureMemoryStatus() {
    CacheMemoryManager.getInstance().ensureMemoryStatus();
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

  /** Sync all volatile nodes to PBTree and execute memory release after flush. */
  public void flushVolatileNodes(boolean needLock) {
    try {
      boolean hasVolatileNodes = flushVolatileDBNode();

      Iterator<ICachedMNode> volatileSubtrees = cacheManager.collectVolatileSubtrees();
      if (volatileSubtrees.hasNext()) {
        hasVolatileNodes = true;

        long startTime = System.currentTimeMillis();

        ICachedMNode subtreeRoot;
        PBTreeFlushExecutor flushExecutor;
        while (volatileSubtrees.hasNext()) {
          subtreeRoot = volatileSubtrees.next();
          flushExecutor =
              new PBTreeFlushExecutor(subtreeRoot, needLock, cacheManager, file, lockManager);
          flushExecutor.flushVolatileNodes();
        }

        long time = System.currentTimeMillis() - startTime;
        if (time > 10_000) {
          logger.info("It takes {}ms to flush MTree in SchemaRegion {}", time, schemaRegionId);
        } else {
          logger.debug("It takes {}ms to flush MTree in SchemaRegion {}", time, schemaRegionId);
        }
      }

      if (hasVolatileNodes) {
        flushCallback.run();
      }
    } catch (MetadataException | IOException e) {
      logger.warn(
          "Exception occurred during MTree flush, current SchemaRegionId is {}", schemaRegionId, e);
    } catch (Throwable e) {
      logger.error(
          "Error occurred during MTree flush, current SchemaRegionId is {}", schemaRegionId, e);
      e.printStackTrace();
    }
  }

  private boolean flushVolatileDBNode() throws IOException {
    IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode =
        cacheManager.collectUpdatedStorageGroupMNodes();
    if (updatedStorageGroupMNode == null) {
      return false;
    }

    try {
      file.updateDatabaseNode(updatedStorageGroupMNode);
      return true;
    } catch (IOException e) {
      logger.warn(
          "IOException occurred during updating StorageGroupMNode {}",
          updatedStorageGroupMNode.getFullPath(),
          e);
      throw e;
    }
  }

  /**
   * Since any node R/W operation may change the memory status, thus it should be controlled during
   * iterating child nodes.
   */
  private class CachedMNodeIterator implements IMNodeIterator<ICachedMNode> {
    ICachedMNode parent;
    Iterator<ICachedMNode> iterator;
    Iterator<ICachedMNode> bufferIterator;
    boolean isIteratingDisk;
    ICachedMNode nextNode;
    boolean isLocked;

    long readLockStamp;

    CachedMNodeIterator(ICachedMNode parent, boolean needLock)
        throws MetadataException, IOException {
      if (needLock) {
        readLockStamp = lockManager.stampedReadLock(parent);
      }
      isLocked = true;
      try {
        this.parent = parent;
        ICachedMNodeContainer container = ICachedMNodeContainer.getCachedMNodeContainer(parent);
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
          lockManager.stampedReadUnlock(parent, readLockStamp);
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
    public ICachedMNode next() {
      if (nextNode == null && !hasNext()) {
        throw new NoSuchElementException();
      }
      ICachedMNode result = nextNode;
      nextNode = null;
      return result;
    }

    private void readNext() throws MetadataException {
      ICachedMNode node = null;
      if (isIteratingDisk) {
        ICachedMNodeContainer container = ICachedMNodeContainer.getCachedMNodeContainer(parent);
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
          ICachedMNode nodeInMem = parent.getChild(node.getName());
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
          lockManager.stampedReadUnlock(parent, readLockStamp);
        }
      }
    }
  }
}
