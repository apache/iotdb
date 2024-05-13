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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISegmentedPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.RecordUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SegmentedPage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getNodeAddress;
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getPageIndex;

/**
 * Abstraction for various implementation of structure of pages. But multi-level index is hard-coded
 * within the framework since only alias works like secondary index now.
 *
 * <p>Hierarchy of index may be decoupled from this framework in further improvement.
 */
public abstract class PageManager implements IPageManager {
  protected static final Logger logger = LoggerFactory.getLogger(PageManager.class);

  protected final PagePool pagePool;
  protected final PageIOChannel pageIOChannel;

  protected final Map<Long, SchemaPageContext> threadContexts;

  protected final AtomicInteger lastPageIndex;

  private SchemaRegionCachedMetric metric = null;

  PageManager(FileChannel channel, File pmtFile, int lastPageIndex, String logPath)
      throws IOException, MetadataException {
    this.pagePool = new PagePool();
    this.threadContexts = new ConcurrentHashMap<>();
    this.lastPageIndex =
        lastPageIndex >= 0 ? new AtomicInteger(lastPageIndex) : new AtomicInteger(0);

    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      pageIOChannel = new PageIOChannel(channel, pmtFile, false, logPath);
    } else {
      pageIOChannel = new PageIOChannel(channel, pmtFile, true, logPath);
    }

    // construct first page if file to init
    if (lastPageIndex < 0) {
      ISegmentedPage rootPage =
          ISchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
      pagePool.put(rootPage);
      rootPage.syncPageBuffer();
      rootPage.flushPageToChannel(channel);
    }
  }

  @Override
  public void writeMNode(ICachedMNode node) throws MetadataException, IOException {
    SchemaPageContext cxt = new SchemaPageContext();
    threadContexts.put(Thread.currentThread().getId(), cxt);
    pagePool.cacheGuardian();
    entrantLock(node, cxt);
    try {
      writeNewChildren(node, cxt);
      writeUpdatedChildren(node, cxt);
      flushDirtyPages(cxt);
    } finally {
      releaseLocks(cxt);
      pagePool.releaseReferent(cxt);
      threadContexts.remove(Thread.currentThread().getId(), cxt);
    }
  }

  /** Context only tracks write locks, and so it shall be released. */
  protected void releaseLocks(SchemaPageContext cxt) {
    for (int i : cxt.lockTraces) {
      cxt.referredPages.get(i).getLock().writeLock().unlock();
    }
  }

  /** locking in the order of page index to avoid deadlock. */
  protected void entrantLock(ICachedMNode node, SchemaPageContext cxt)
      throws IOException, MetadataException {
    int initPageIndex = getPageIndex(getNodeAddress(node));
    ISchemaPage page;

    if (node.isDatabase()) {
      if (initPageIndex < 0) {
        // node is using template
        return;
      }

      page = getPageInstance(initPageIndex, cxt);
      page.getLock().writeLock().lock();
      cxt.traceLock(page);
      cxt.indexBuckets.sortIntoBucket(page, (short) -1);
    } else {
      int parIndex = getPageIndex(getNodeAddress(node.getParent()));

      int minPageIndex = Math.min(initPageIndex, parIndex);
      int maxPageIndex = Math.max(initPageIndex, parIndex);

      // as InternalPage will not transplant, its parent pointer needs no lock
      page = getPageInstance(initPageIndex, cxt);
      if (page.getAsInternalPage() != null) {
        page.getLock().writeLock().lock();
        cxt.traceLock(page);
        cxt.indexBuckets.sortIntoBucket(page, (short) -1);
        return;
      }

      if (minPageIndex >= 0) {
        page = getPageInstance(minPageIndex, cxt);
        page.getLock().writeLock().lock();
        cxt.traceLock(page);
        cxt.indexBuckets.sortIntoBucket(page, (short) -1);
      }

      if (minPageIndex != maxPageIndex && maxPageIndex >= 0) {
        page = getPageInstance(maxPageIndex, cxt);
        page.getLock().writeLock().lock();
        cxt.traceLock(page);
        cxt.indexBuckets.sortIntoBucket(page, (short) -1);
      }
    }
  }

  // region Framework Methods
  private void writeNewChildren(ICachedMNode node, SchemaPageContext cxt)
      throws MetadataException, IOException {
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    long res; // result of write
    ICachedMNode child;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    String alias;
    // TODO: reserve order of insert in container may be better
    for (Map.Entry<String, ICachedMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node)
            .getNewChildFlushingBuffer()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      // check and pre-allocate
      child = entry.getValue();
      if (!child.isMeasurement()) {
        alias = null;

        // TODO optimization if many device only using template but has no child
        if (getNodeAddress(child) < 0) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize, cxt);
          SchemaFile.setNodeAddress(child, glbIndex);
        } else {
          // new child with a valid segment address could be maliciously modified
          throw new MetadataException("A child in newChildBuffer shall not have segmentAddress.");
        }
      } else {
        alias =
            child.getAsMeasurementMNode().getAlias() == null
                ? null
                : child.getAsMeasurementMNode().getAlias();
      }

      // prepare buffer to write
      childBuffer = RecordUtils.node2Buffer(child);
      actualAddress = getTargetSegmentAddress(curSegAddr, entry.getKey(), cxt);
      curPage = getPageInstance(SchemaFile.getPageIndex(actualAddress), cxt);

      res =
          curPage
              .getAsSegmentedPage()
              .write(SchemaFile.getSegIndex(actualAddress), entry.getKey(), childBuffer);
      if (res >= 0) {
        if (res != 0) {
          // spare size increased, buckets need to update
          cxt.indexBuckets.sortIntoBucket(curPage, (short) -1);
        }

        interleavedFlush(curPage, cxt);
        cxt.markDirty(curPage);

        subIndex = subIndexRootPage(curSegAddr, cxt);
        if (alias != null && subIndex >= 0) {
          insertSubIndexEntry(subIndex, alias, entry.getKey(), cxt);
        }
      } else {
        // page overflow
        // current page is not enough for coming record
        if (curPage.getAsSegmentedPage().getSegmentSize(SchemaFile.getSegIndex(actualAddress))
            == SchemaFileConfig.SEG_MAX_SIZ) {
          // curPage might be replaced so unnecessary to mark it here
          multiPageInsertOverflowOperation(curPage, entry.getKey(), childBuffer, cxt);

          subIndex = subIndexRootPage(curSegAddr, cxt);
          if (node.isDevice() && subIndex < 0) {
            // the record occurred overflow had been inserted already
            buildSubIndex(node, cxt);
          } else if (alias != null) {
            // implied node is entity, so sub index must exist
            insertSubIndexEntry(subIndex, alias, entry.getKey(), cxt);
          }
        } else {
          // transplant and delete former segment
          short actSegId = SchemaFile.getSegIndex(actualAddress);
          short newSegSize =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity(),
                  ICachedMNodeContainer.getCachedMNodeContainer(node)
                      .getNewChildFlushingBuffer()
                      .entrySet()
                      .size());
          ISegmentedPage newPage = getMinApplSegmentedPageInMem(newSegSize, cxt);

          // with single segment, curSegAddr equals actualAddress
          curSegAddr =
              newPage.transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSize);
          newPage.write(SchemaFile.getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);
          cxt.indexBuckets.sortIntoBucket(curPage, (short) -1);

          // entrant lock guarantees thread-safe
          SchemaFile.setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr, cxt);

          // newPage is marked and referred within getMinAppl method
          cxt.markDirty(curPage);
        }
      }
    }
  }

  private void writeUpdatedChildren(ICachedMNode node, SchemaPageContext cxt)
      throws MetadataException, IOException {
    boolean removeOldSubEntry, insertNewSubEntry;
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    String alias, oldAlias; // key of the sub-index entry now
    ICachedMNode child, oldChild;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    long res; // result of update
    for (Map.Entry<String, ICachedMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node)
            .getUpdatedChildFlushingBuffer()
            .entrySet()) {
      child = entry.getValue();
      actualAddress = getTargetSegmentAddress(curSegAddr, entry.getKey(), cxt);
      childBuffer = RecordUtils.node2Buffer(child);

      curPage = getPageInstance(SchemaFile.getPageIndex(actualAddress), cxt);
      if (curPage.getAsSegmentedPage().read(SchemaFile.getSegIndex(actualAddress), entry.getKey())
          == null) {
        throw new MetadataException(
            String.format(
                "Node[%s] has no child[%s] in pbtree file.", node.getName(), entry.getKey()));
      }

      // prepare alias comparison
      if (child.isMeasurement() && child.getAsMeasurementMNode().getAlias() != null) {
        alias = child.getAsMeasurementMNode().getAlias();
      } else {
        alias = null;
      }
      if (node.isDevice()) {
        oldChild =
            curPage
                .getAsSegmentedPage()
                .read(SchemaFile.getSegIndex(actualAddress), entry.getKey());
        oldAlias = oldChild.isMeasurement() ? oldChild.getAsMeasurementMNode().getAlias() : null;
      } else {
        oldAlias = null;
      }

      if (alias == null && oldAlias != null) {
        // remove old alias index: not exists anymore
        removeOldSubEntry = true;
        insertNewSubEntry = false;
      } else if (alias != null && oldAlias == null) {
        // insert alias index: no longer null
        removeOldSubEntry = false;
        insertNewSubEntry = true;
      } else if (alias != null && alias.compareTo(oldAlias) != 0) {
        // remove and (re)insert: both not null, but not equals
        insertNewSubEntry = removeOldSubEntry = true;
      } else {
        // do nothing: both null, or both not null but equals
        insertNewSubEntry = removeOldSubEntry = false;
      }

      res =
          curPage
              .getAsSegmentedPage()
              .update(SchemaFile.getSegIndex(actualAddress), entry.getKey(), childBuffer);
      if (res >= 0) {
        if (res != 0) {
          // spare size increased, buckets need to update
          cxt.indexBuckets.sortIntoBucket(curPage, (short) -1);
        }
        interleavedFlush(curPage, cxt);
        cxt.markDirty(curPage);

        subIndex = subIndexRootPage(curSegAddr, cxt);
        if (subIndex >= 0) {
          if (removeOldSubEntry) {
            removeSubIndexEntry(subIndex, oldAlias, cxt);
          }

          if (insertNewSubEntry) {
            insertSubIndexEntry(subIndex, alias, entry.getKey(), cxt);
          }
        }
      } else {
        // page overflow
        if (curPage.getAsSegmentedPage().getSegmentSize(SchemaFile.getSegIndex(actualAddress))
            == SchemaFileConfig.SEG_MAX_SIZ) {
          multiPageUpdateOverflowOperation(curPage, entry.getKey(), childBuffer, cxt);

          subIndex = subIndexRootPage(curSegAddr, cxt);
          if (node.isDevice() && subIndex < 0) {
            buildSubIndex(node, cxt);
          } else if (insertNewSubEntry || removeOldSubEntry) {
            if (removeOldSubEntry) {
              removeSubIndexEntry(subIndex, oldAlias, cxt);
            }

            if (insertNewSubEntry) {
              insertSubIndexEntry(subIndex, alias, entry.getKey(), cxt);
            }
          }
        } else {
          // transplant into another bigger segment
          short actSegId = SchemaFile.getSegIndex(actualAddress);
          short newSegSiz =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity());
          ISegmentedPage newPage = getMinApplSegmentedPageInMem(newSegSiz, cxt);

          // assign new segment address
          curSegAddr = newPage.transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSiz);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);
          cxt.indexBuckets.sortIntoBucket(curPage, (short) -1);

          newPage.update(SchemaFile.getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          SchemaFile.setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr, cxt);
          cxt.markDirty(curPage);
        }
      }
    }
  }

  // endregion

  // region Abstract and Overridable Methods
  protected abstract long getTargetSegmentAddress(
      long curAddr, String recKey, SchemaPageContext cxt) throws IOException, MetadataException;

  /**
   * Deal with split, transplant and index update about the overflow occurred by insert.
   *
   * @param curPage the page encounters overflow, shall be a {@linkplain SegmentedPage}.
   * @param key the key to insert
   * @param childBuffer content of the key
   */
  protected abstract void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageContext cxt)
      throws MetadataException, IOException;

  /** Same as {@linkplain #multiPageInsertOverflowOperation} */
  protected abstract void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageContext cxt)
      throws MetadataException, IOException;

  /**
   * Occurs only when an Entity Node encounters a multipart overflow.
   *
   * @param parNode node needs to build subordinate index.
   */
  protected abstract void buildSubIndex(ICachedMNode parNode, SchemaPageContext cxt)
      throws MetadataException, IOException;

  /**
   * Insert an entry of subordinate index of the target node.
   *
   * @param base index of the top page contains sub-index entries.
   * @param key key of the sub-index entry.
   * @param rec value of the sub-index entry.
   */
  protected abstract void insertSubIndexEntry(
      int base, String key, String rec, SchemaPageContext cxt)
      throws MetadataException, IOException;

  protected abstract void removeSubIndexEntry(int base, String oldAlias, SchemaPageContext cxt)
      throws MetadataException, IOException;

  protected abstract String searchSubIndexAlias(int base, String alias, SchemaPageContext cxt)
      throws MetadataException, IOException;

  // endregion

  // region Flush Strategy

  public synchronized void flushDirtyPages(SchemaPageContext cxt) throws IOException {
    if (cxt.dirtyCnt == 0) {
      return;
    }
    pageIOChannel.flushMultiPages(cxt);
    pagePool.appendBucketIndex(cxt);
    if (metric != null) {
      metric.recordFlushPageNum(cxt.referredPages.size());
    }
  }

  /**
   * when page prepares to insert, compare it with lastLeaf from context. if not same, then flush
   * the lastLeaf, unlock, deref, and remove it from dirtyPages. lastLeafPage only initiated at
   * overflowOperation
   */
  @Deprecated
  private void interleavedFlush(ISchemaPage page, SchemaPageContext cxt) throws IOException {
    if (cxt.lastLeafPage == null || cxt.lastLeafPage.getPageIndex() == page.getPageIndex()) {
      return;
    }
    cxt.interleavedFlushCnt++;
    if (metric != null) {
      metric.recordFlushPageNum(1);
    }
    pageIOChannel.flushSinglePage(cxt.lastLeafPage);
    // this lastLeaf shall only be lock once
    cxt.dirtyCnt--;

    // unlock and deref the page from context
    if (cxt.lockTraces.contains(cxt.lastLeafPage.getPageIndex())) {
      cxt.lastLeafPage.getLock().writeLock().unlock();
      cxt.lockTraces.remove(cxt.lastLeafPage.getPageIndex());
    }
    cxt.lastLeafPage.decrementAndGetRefCnt();

    // can be reclaimed since the page only referred by pageInstCache
    cxt.referredPages.remove(cxt.lastLeafPage.getPageIndex());
    // alleviate eviction pressure
    pagePool.remove(cxt.lastLeafPage.getPageIndex());

    cxt.lastLeafPage = page.getAsSegmentedPage();
  }

  // endregion

  // region General Interfaces
  @Override
  public int getLastPageIndex() {
    return lastPageIndex.get();
  }

  @Override
  public void clear() throws IOException, MetadataException {
    pagePool.clear();
    lastPageIndex.set(0);
    pageIOChannel.renewLogWriter();
  }

  @Override
  public void inspect(PrintWriter pw) throws IOException, MetadataException {
    SchemaPageContext cxt = new SchemaPageContext();
    String pageContent;
    for (int i = 0; i <= lastPageIndex.get(); i++) {
      pageContent = getPageInstance(i, cxt).inspect();
      pw.print("---------------------\n");
      pw.print(pageContent);
      pw.print("\n");
    }
  }

  @Override
  public void close() throws IOException {
    pageIOChannel.closeLogWriter();
  }

  // endregion

  // region Page Access Management

  /** Any page returned will be pinned/referred by the cxt. * */
  public ISchemaPage getPageInstance(int pageIdx, SchemaPageContext cxt)
      throws IOException, MetadataException {
    if (pageIdx > lastPageIndex.get()) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    // just return from (thread local) context
    if (cxt != null && cxt.referredPages.containsKey(pageIdx)) {
      return cxt.referredPages.get(pageIdx);
    }

    // lock for no duplicate page with same index from disk, and guarantees page will not be evicted
    //  by other thread before referred by current thread
    pagePool.lock();
    try {
      ISchemaPage page = pagePool.get(pageIdx);
      if (page != null) {
        cxt.refer(page);
        return page;
      }

      ByteBuffer newBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
      if (metric != null) {
        metric.recordLoadPageNum(1);
      }
      pageIOChannel.loadFromFileToBuffer(newBuf, pageIdx);
      page = ISchemaPage.loadSchemaPage(newBuf);
      cxt.refer(page);
      pagePool.put(page);
      return page;
    } finally {
      pagePool.unlock();
    }
  }

  private long preAllocateSegment(short size, SchemaPageContext cxt)
      throws IOException, MetadataException {
    ISegmentedPage page = getMinApplSegmentedPageInMem(size, cxt);
    short sparePrev = page.getSpareSize();
    long res = SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
    if (sparePrev < page.getSpareSize()) {
      // a compaction trigger by allocNewSegment had increased spare size
      cxt.indexBuckets.sortIntoBucket(page, (short) -1);
    }
    return res;
  }

  protected ISchemaPage replacePageInCache(ISchemaPage page, SchemaPageContext cxt) {
    // no need to lock since the root of B+Tree is locked
    cxt.markDirty(page, true);
    pagePool.put(page);
    return page;
  }

  /**
   * Only accessed during write operation and every page returned is due to be modified thus they
   * are all marked dirty.
   *
   * @param size size of the expected segment
   * @return always write locked
   */
  protected ISegmentedPage getMinApplSegmentedPageInMem(short size, SchemaPageContext cxt)
      throws SegmentNotFoundException {
    // pages retrieved from context is unnecessary and inefficient to lock
    ISchemaPage targetPage = cxt.indexBuckets.getNearestFitPage(size, false);
    if (targetPage != null) {
      cxt.indexBuckets.sortIntoBucket(targetPage, size);
      return targetPage.getAsSegmentedPage();
    }

    pagePool.lock();
    try {
      // pageIndexBuckets sorts pages within pageInstCache into buckets to expedite access
      targetPage = pagePool.getNearestFitPage(size);
      if (targetPage != null) {
        cxt.markDirty(targetPage);
        cxt.traceLock(targetPage);

        // transfer the page from pageIndexBuckets to cxt.buckets thus not be accessed by other
        //  WRITE thread
        cxt.indexBuckets.sortIntoBucket(targetPage, size);
        return targetPage.getAsSegmentedPage();
      }

      // due to be dirty thus its index only sorted into local buckets
      targetPage = allocNewSegmentedPage(cxt);
      cxt.indexBuckets.sortIntoBucket(targetPage, size);
      return targetPage.getAsSegmentedPage();
    } finally {
      pagePool.unlock();
    }
  }

  protected ISchemaPage allocNewSegmentedPage(SchemaPageContext cxt)
      throws SegmentNotFoundException {
    ISchemaPage newPage =
        ISchemaPage.initSegmentedPage(
            ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), lastPageIndex.incrementAndGet());
    // lock and mark to be consistent with # getMinApplSegmentedPageInMem
    cxt.markDirty(newPage);
    newPage.getLock().writeLock().lock();
    cxt.traceLock(newPage);
    pagePool.put(newPage);
    return newPage;
  }

  protected ISchemaPage registerAsNewPage(ISchemaPage page, SchemaPageContext cxt) {
    // new page will be LOCAL until the creating thread finished
    //  thus not added to sparsePageIndex now
    page.setPageIndex(lastPageIndex.incrementAndGet());
    pagePool.put(page);
    cxt.markDirty(page);
    return page;
  }

  private void updateParentalRecord(
      ICachedMNode parent, String key, long newSegAddr, SchemaPageContext cxt)
      throws IOException, MetadataException {
    if (parent == null || parent.getChild(key).isDatabase()) {
      throw new MetadataException("Root page shall not be migrated.");
    }
    long parSegAddr = parent.getParent() == null ? 0L : getNodeAddress(parent);
    parSegAddr = getTargetSegmentAddress(parSegAddr, key, cxt);
    ISchemaPage page = getPageInstance(SchemaFile.getPageIndex(parSegAddr), cxt);
    ((SegmentedPage) page).updateRecordSegAddr(SchemaFile.getSegIndex(parSegAddr), key, newSegAddr);
    cxt.markDirty(page);
  }

  // endregion

  // region Inner Utilities

  private int subIndexRootPage(long addr, SchemaPageContext cxt)
      throws IOException, MetadataException {
    return getPageInstance(SchemaFile.getPageIndex(addr), cxt).getSubIndex();
  }

  private static long getPageAddress(int pageIndex) {
    return (SchemaFileConfig.PAGE_INDEX_MASK & pageIndex) * SchemaFileConfig.PAGE_LENGTH
        + SchemaFileConfig.FILE_HEADER_SIZE;
  }

  /** Estimate segment size for pre-allocate */
  private static short estimateSegmentSize(ICachedMNode node) {
    int childNum = node.getChildren().size();
    if (childNum < SchemaFileConfig.SEG_SIZE_METRIC[0]) {
      // for record offset, length of string key
      int totalSize = SchemaFileConfig.SEG_HEADER_SIZE + 6 * childNum;
      for (ICachedMNode child : node.getChildren().values()) {
        totalSize += child.getName().getBytes().length + RecordUtils.getRecordLength(child);
      }
      return (short) totalSize > SchemaFileConfig.SEG_MIN_SIZ
          ? (short) totalSize
          : SchemaFileConfig.SEG_MIN_SIZ;
    }

    int tier = SchemaFileConfig.SEG_SIZE_LST.length - 1;
    while (tier > 0) {
      if (childNum > SchemaFileConfig.SEG_SIZE_METRIC[tier]) {
        return SchemaFileConfig.SEG_SIZE_LST[tier];
      }
      tier--;
    }
    return SchemaFileConfig.SEG_SIZE_LST[0];
  }

  /**
   * This method {@linkplain #reEstimateSegSize} is called when {@linkplain
   * SchemaPageOverflowException} occurs. It is designed to accelerate when there is lots of new
   * children nodes, avoiding segments extend several times.
   *
   * <p>Notice that SegmentOverflowException inside a page with sufficient space will not reach
   * here. Supposed to merge with SchemaFile#reEstimateSegSize.
   *
   * @param expSize expected size calculated from next new record
   * @param batchSize size of children within one #writeNewChildren(ICachedMNode)
   * @return estimated size
   */
  private static short reEstimateSegSize(int expSize, int batchSize) throws MetadataException {
    if (batchSize < SchemaFileConfig.SEG_SIZE_METRIC[0]) {
      return reEstimateSegSize(expSize);
    }
    int base_tier = 0;
    for (int i = 0; i < SchemaFileConfig.SEG_SIZE_LST.length; i++) {
      if (SchemaFileConfig.SEG_SIZE_LST[i] >= expSize) {
        base_tier = i;
        break;
      }
    }
    int tier = SchemaFileConfig.SEG_SIZE_LST.length - 1;
    while (tier >= base_tier) {
      if (batchSize > SchemaFileConfig.SEG_SIZE_METRIC[tier]) {
        return SchemaFileConfig.SEG_SIZE_LST[tier];
      }
      tier--;
    }
    return SchemaFileConfig.SEG_SIZE_LST[base_tier];
  }

  private static short reEstimateSegSize(int expSize) throws MetadataException {
    if (expSize > SchemaFileConfig.SEG_MAX_SIZ) {
      // TODO: to support extreme large MNode
      throw new MetadataException(
          "Single record larger than half page is not supported in SchemaFile now.");
    }
    for (short size : SchemaFileConfig.SEG_SIZE_LST) {
      if (expSize < size) {
        return size;
      }
    }
    return SchemaFileConfig.SEG_MAX_SIZ;
  }

  // endregion

  // region TestOnly Methods

  @TestOnly
  public String checkAllContexts() {
    StringBuilder builder = new StringBuilder();

    for (SchemaPageContext cxt : threadContexts.values()) {
      builder.append(
          String.format(
              "cxt[%d] has %d pages, %d lock trace\n",
              cxt.threadID, cxt.referredPages.size(), cxt.lockTraces.size()));
    }

    return builder.toString();
  }

  @TestOnly
  public static String checkContextLock(SchemaPageContext cxt) {
    StringBuilder builder = new StringBuilder();

    for (ISchemaPage page : cxt.referredPages.values()) {
      builder.append(
          String.format(
              "page:%d, wl:%d, rl:%d, trace:%b, dirty:%b;\n",
              page.getPageIndex(),
              ((ReentrantReadWriteLock) page.getLock()).getWriteHoldCount(),
              ((ReentrantReadWriteLock) page.getLock()).getReadLockCount(),
              cxt.lockTraces.contains(page.getPageIndex()),
              page.isDirtyPage()));
    }
    return builder.toString();
  }

  @TestOnly
  public long getTargetSegmentAddressOnTest(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    return getTargetSegmentAddress(curSegAddr, recKey, new SchemaPageContext());
  }

  @TestOnly
  public ISchemaPage getPageInstanceOnTest(int pageIdx) throws IOException, MetadataException {
    SchemaPageContext cxt = new SchemaPageContext();
    return getPageInstance(pageIdx, cxt);
  }

  // endregion

  @Override
  public void setMetric(SchemaRegionCachedMetric metric) {
    this.metric = metric;
  }
}
