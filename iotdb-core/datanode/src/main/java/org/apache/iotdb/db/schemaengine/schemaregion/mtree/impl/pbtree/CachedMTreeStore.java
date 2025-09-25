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
import org.apache.iotdb.commons.schema.MergeSortIterator;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.estimator.MNodeSizeEstimator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush.PBTreeFlushExecutor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.MemoryStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.IMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.iterator.CachedTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MNodeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CachedMTreeStore implements IMTreeStore<ICachedMNode> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachedMTreeStore.class);

  private final int schemaRegionId;

  private final MemoryStatistics memoryStatistics;
  private final IMemoryManager memoryManager;
  private ISchemaFile file;
  private ICachedMNode root;
  // TODO: delete it
  private final Runnable flushCallback;
  private final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
  private final CachedSchemaRegionStatistics regionStatistics;
  private final SchemaRegionCachedMetric metric;
  private final ReleaseFlushMonitor releaseFlushMonitor = ReleaseFlushMonitor.getInstance();
  private final LockManager lockManager;

  public CachedMTreeStore(
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      SchemaRegionCachedMetric metric,
      Runnable flushCallback,
      ISchemaFile schemaFile,
      IMemoryManager memoryManager,
      MemoryStatistics memoryStatistics,
      LockManager lockManager)
      throws MetadataException {
    this.schemaRegionId = schemaRegionId;
    this.regionStatistics = regionStatistics;
    this.flushCallback = flushCallback;

    this.lockManager = lockManager;

    file = schemaFile;
    root = file.init();

    this.memoryStatistics = memoryStatistics;

    this.memoryManager = memoryManager;
    this.metric = metric;
    memoryManager.initRootStatus(root);

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

  public IMemoryManager getMemoryManager() {
    return memoryManager;
  }

  public ISchemaFile getSchemaFile() {
    return file;
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  @Override
  public ICachedMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(ICachedMNode parent, String name) throws MetadataException {
    return hasChild(parent, name, true, true);
  }

  protected final boolean hasChild(
      ICachedMNode parent, String name, boolean needGlobalLock, boolean needNodeLock)
      throws MetadataException {
    if (needGlobalLock) {
      lockManager.globalReadLock();
    }
    if (needNodeLock) {
      lockManager.threadReadLock(parent);
    }
    try {
      ICachedMNode child = getChild(parent, name, false, false);
      if (child == null) {
        return false;
      } else {
        unPin(child, false);
        return true;
      }
    } finally {
      if (needNodeLock) {
        lockManager.threadReadUnlock(parent);
      }
      if (needGlobalLock) {
        lockManager.globalReadUnlock();
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
    return getChild(parent, name, true, true);
  }

  protected final ICachedMNode getChild(
      ICachedMNode parent, String name, boolean needGlobalLock, boolean needNodeLock)
      throws MetadataException {
    if (needGlobalLock) {
      lockManager.globalReadLock();
    }
    if (needNodeLock) {
      lockManager.threadReadLock(parent);
    }
    try {
      ICachedMNode node = parent.getChild(name);
      if (node == null) {
        node = loadChildFromDisk(parent, name);
      } else {
        try {
          memoryManager.updateCacheStatusAfterMemoryRead(node);
        } catch (MNodeNotCachedException e) {
          node = loadChildFromDisk(parent, name);
        }
      }
      if (node != null && node.isMeasurement()) {
        processAlias(parent.getAsDeviceMNode(), node.getAsMeasurementMNode());
      }

      return node;
    } finally {
      if (needNodeLock) {
        lockManager.threadReadUnlock(parent);
      }
      if (needGlobalLock) {
        lockManager.globalReadUnlock();
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
          memoryManager.updateCacheStatusAfterMemoryRead(nodeAlreadyLoaded);
          return nodeAlreadyLoaded;
        } catch (MNodeNotCachedException ignored) {
          // the nodeAlreadyLoaded is evicted and use the node read from disk
        }
      }
      node.setParent(parent);
      metric.recordLoadFromDisk(node.estimateSize(), 1L);
      memoryManager.updateCacheStatusAfterDiskRead(node);
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
    lockManager.globalReadLock();
    lockManager.threadReadLock(parent);
    try {
      child.setParent(parent);
      memoryManager.updateCacheStatusAfterAppend(child);
      ensureMemoryStatus();
      return parent.getChild(childName);
    } finally {
      lockManager.threadReadUnlock(parent);
      lockManager.globalReadUnlock();
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
    lockManager.globalWriteLock();
    try {
      ICachedMNode deletedMNode = getChild(parent, childName, false, false);
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
      memoryManager.remove(deletedMNode);
    } finally {
      lockManager.globalWriteUnlock();
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
    if (needLock) {
      lockManager.globalReadLock();
    }
    if (!node.isDatabase()) {
      lockManager.threadReadLock(node.getParent(), true);
    }
    try {
      operation.accept(node);
      memoryManager.updateCacheStatusAfterUpdate(node);
    } finally {
      if (!node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
      }
      if (needLock) {
        lockManager.globalReadUnlock();
      }
    }
  }

  @Override
  public IDeviceMNode<ICachedMNode> setToEntity(ICachedMNode node) {
    int rawSize = node.estimateSize();
    AtomicReference<Boolean> resultReference = new AtomicReference<>(false);
    updateMNode(node, o -> resultReference.getAndSet(MNodeUtils.setToEntity(node)));

    boolean isSuccess = resultReference.get();
    if (isSuccess) {
      regionStatistics.addDevice();
      memoryStatistics.updatePinnedSize(node.estimateSize() - rawSize);
    }

    return node.getAsDeviceMNode();
  }

  @Override
  public ICachedMNode setToInternal(IDeviceMNode<ICachedMNode> entityMNode) {
    int rawSize = entityMNode.estimateSize();
    AtomicReference<Boolean> resultReference = new AtomicReference<>(false);
    // the entityMNode is just a wrapper, the actual CachedMNode instance shall be the same before
    // and after
    // setToInternal
    ICachedMNode internalMNode = entityMNode.getAsMNode();
    updateMNode(
        internalMNode, o -> resultReference.getAndSet(MNodeUtils.setToInternal(entityMNode)));

    boolean isSuccess = resultReference.get();
    if (isSuccess) {
      regionStatistics.deleteDevice();
      memoryStatistics.updatePinnedSize(internalMNode.estimateSize() - rawSize);
    }

    return internalMNode;
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
      memoryStatistics.updatePinnedSize(alias.length() - existingAlias.length());
    } else if (alias == null) {
      memoryStatistics.updatePinnedSize(
          -(MNodeSizeEstimator.getAliasBaseSize() + existingAlias.length()));
    } else {
      memoryStatistics.updatePinnedSize(MNodeSizeEstimator.getAliasBaseSize() + alias.length());
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
    if (node.getParent() == null) {
      // ignore node represented by template
      return;
    }
    if (needLock) {
      lockManager.globalReadLock();
    }
    if (!node.isDatabase()) {
      lockManager.threadReadLock(node.getParent());
    }
    try {
      memoryManager.pinMNode(node);
    } finally {
      if (!node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
      }
      if (needLock) {
        lockManager.globalReadUnlock();
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
    if (node.getParent() == null) {
      // ignore node represented by template
      return;
    }
    if (needLock) {
      lockManager.globalReadLock();
    }
    if (!node.isDatabase()) {
      lockManager.threadReadLock(node.getParent(), true);
    }
    try {
      if (memoryManager.unPinMNode(node)) {
        ensureMemoryStatus();
      }
    } finally {
      if (!node.isDatabase()) {
        lockManager.threadReadUnlock(node.getParent());
      }
      if (needLock) {
        lockManager.globalReadUnlock();
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

  final long stampedReadLock() {
    return lockManager.globalStampedReadLock();
  }

  final void stampedReadUnlock(long stamp) {
    lockManager.globalStampedReadUnlock(stamp);
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
      releaseFlushMonitor.clearCachedMTreeStore(this);
      regionStatistics.setMemoryManager(null);
      memoryManager.clear(root);
      root = null;
      if (file != null) {
        try {
          file.clear();
          file.close();
        } catch (MetadataException | IOException e) {
          LOGGER.error("Error occurred during PBTree clear, {}", e.getMessage(), e);
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

  @Override
  public ReleaseFlushMonitor.RecordNode recordTraverserStatistics() {
    return releaseFlushMonitor.recordTraverserTime(schemaRegionId);
  }

  @Override
  public void recordTraverserMetric(long costTime) {
    metric.recordTraverser(costTime);
  }

  public void recordReleaseMetrics(long costTime, long releaseNodeNum, long releaseMemorySize) {
    metric.recordRelease(costTime, releaseNodeNum, releaseMemorySize);
  }

  public void recordFlushMetrics(long costTime, long releaseNodeNum, long releaseMemorySize) {
    metric.recordFlush(costTime, releaseNodeNum, releaseMemorySize);
  }

  private void ensureMemoryStatus() {
    releaseFlushMonitor.ensureMemoryStatus();
  }

  public CachedSchemaRegionStatistics getRegionStatistics() {
    return regionStatistics;
  }

  /**
   * Fetching evictable nodes from memoryManager. Update the memory status after evicting each node.
   *
   * @return should not continue releasing
   */
  public boolean executeMemoryRelease(AtomicLong releaseNodeNum, AtomicLong releaseMemorySize) {
    if (regionStatistics.getUnpinnedMemorySize() != 0) {
      return !memoryManager.evict(releaseNodeNum, releaseMemorySize);
    } else {
      return true;
    }
  }

  /** Sync all volatile nodes to PBTree and execute memory release after flush. */
  public void flushVolatileNodes(boolean needLock) {
    if (needLock) {
      lockManager.globalReadLock();
    }
    long startTime = System.currentTimeMillis();
    AtomicLong flushNodeNum = new AtomicLong(0);
    AtomicLong flushMemSize = new AtomicLong(0);
    try {
      PBTreeFlushExecutor flushExecutor = new PBTreeFlushExecutor(memoryManager, file, lockManager);

      flushExecutor.flushVolatileNodes(flushNodeNum, flushMemSize);

    } catch (Throwable e) {
      LOGGER.error(
          "Error occurred during MTree flush, current SchemaRegionId is {}", schemaRegionId, e);
    } finally {
      long time = System.currentTimeMillis() - startTime;
      if (time > 10_000) {
        LOGGER.info("It takes {}ms to flush MTree in SchemaRegion {}", time, schemaRegionId);
      } else {
        LOGGER.debug("It takes {}ms to flush MTree in SchemaRegion {}", time, schemaRegionId);
      }
      recordFlushMetrics(time, flushNodeNum.get(), flushMemSize.get());
      if (needLock) {
        lockManager.globalReadUnlock();
      }
    }
  }

  /**
   * Since any node R/W operation may change the memory status, thus it should be controlled during
   * iterating child nodes.
   */
  private class CachedMNodeIterator implements IMNodeIterator<ICachedMNode> {
    ICachedMNode parent;
    CachedMNodeMergeIterator mergeIterator;
    Iterator<ICachedMNode> bufferIterator;
    Iterator<ICachedMNode> diskIterator;

    boolean needLock;
    boolean isLocked;
    long readLockStamp;

    CachedMNodeIterator(ICachedMNode parent, boolean needLock)
        throws MetadataException, IOException {
      this.needLock = needLock;
      if (needLock) {
        lockManager.globalReadLock();
      }
      readLockStamp = lockManager.stampedReadLock(parent);
      isLocked = true;
      try {
        this.parent = parent;
        ICachedMNodeContainer container = ICachedMNodeContainer.getCachedMNodeContainer(parent);
        bufferIterator = container.getChildrenBufferIterator();
        diskIterator =
            !container.isVolatile() ? file.getChildren(parent) : Collections.emptyIterator();
        mergeIterator = new CachedMNodeMergeIterator(diskIterator, bufferIterator);
      } catch (Throwable e) {
        lockManager.stampedReadUnlock(parent, readLockStamp);
        if (needLock) {
          lockManager.globalReadUnlock();
        }
        isLocked = false;
        throw e;
      }
    }

    @Override
    public boolean hasNext() {
      return mergeIterator.hasNext();
    }

    @Override
    public ICachedMNode next() {
      return mergeIterator.next();
    }

    @Override
    public void skipTemplateChildren() {
      // do nothing
    }

    @Override
    public void close() {
      if (isLocked) {
        lockManager.stampedReadUnlock(parent, readLockStamp);
        if (needLock) {
          lockManager.globalReadUnlock();
        }
        isLocked = false;
      }
    }

    private class CachedMNodeMergeIterator extends MergeSortIterator<ICachedMNode> {
      public CachedMNodeMergeIterator(
          Iterator<ICachedMNode> diskIterator, Iterator<ICachedMNode> bufferIterator) {
        super(diskIterator, bufferIterator);
      }

      protected ICachedMNode onReturnLeft(ICachedMNode ansMNode) {
        ICachedMNode nodeInMem = parent.getChild(ansMNode.getName());
        if (nodeInMem != null) {
          try {
            memoryManager.updateCacheStatusAfterMemoryRead(nodeInMem);
            ansMNode = nodeInMem;
          } catch (MNodeNotCachedException e) {
            ansMNode = loadChildFromDiskToParent(parent, ansMNode);
          }
        } else {
          ansMNode = loadChildFromDiskToParent(parent, ansMNode);
        }
        return ansMNode;
      }

      protected ICachedMNode onReturnRight(ICachedMNode ansMNode) {
        try {
          memoryManager.updateCacheStatusAfterMemoryRead(ansMNode);
        } catch (MNodeNotCachedException e) {
          throw new RuntimeException(e);
        }
        return ansMNode;
      }

      protected int decide() {
        return 1;
      }

      protected int compare(ICachedMNode left, ICachedMNode right) {
        return left.getName().compareTo(right.getName());
      }
    }
  }
}
