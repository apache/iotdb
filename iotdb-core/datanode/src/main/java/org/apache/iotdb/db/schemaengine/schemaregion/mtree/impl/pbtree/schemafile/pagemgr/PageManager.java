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
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISegmentedPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.RecordUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SegmentedPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.log.SchemaFileLogReader;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.log.SchemaFileLogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

  protected final Map<Integer, ISchemaPage> pageInstCache;
  // bucket for quick retrieval, only append when write operation finished
  protected final PageIndexSortBuckets pageIndexBuckets;

  protected final Lock cacheLock;
  protected final Condition cacheFull;

  protected final Map<Long, SchemaPageContext> threadContexts;

  protected final AtomicInteger lastPageIndex;

  private final FileChannel channel;

  // handle timeout interruption during reading
  private final File pmtFile;
  private FileChannel readChannel;

  private final AtomicInteger logCounter;
  private SchemaFileLogWriter logWriter;
  private SchemaRegionCachedMetric metric = null;

  // flush strategy is dependent on consensus protocol, only check protocol on init
  protected FlushPageStrategy flushDirtyPagesStrategy;
  protected SinglePageFlushStrategy singlePageFlushStrategy;

  PageManager(FileChannel channel, File pmtFile, int lastPageIndex, String logPath)
      throws IOException, MetadataException {
    this.pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaFileConfig.PAGE_CACHE_SIZE, 1, true));
    this.pageIndexBuckets = new PageIndexSortBuckets(SchemaFileConfig.SEG_SIZE_LST, pageInstCache);
    this.threadContexts = new ConcurrentHashMap<>();

    this.cacheLock = new ReentrantLock();
    this.cacheFull = this.cacheLock.newCondition();

    this.lastPageIndex =
        lastPageIndex >= 0 ? new AtomicInteger(lastPageIndex) : new AtomicInteger(0);
    this.channel = channel;
    this.pmtFile = pmtFile;
    this.readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);

    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      // with RATIS enabled, integrity is guaranteed by consensus protocol
      logCounter = new AtomicInteger();
      logWriter = null;
      flushDirtyPagesStrategy = this::flushDirtyPagesWithoutLogging;
      singlePageFlushStrategy = this::flushSinglePageWithoutLogging;
    } else {
      // without RATIS, utilize physical logging for integrity
      int pageAcc = (int) recoverFromLog(logPath) / SchemaFileConfig.PAGE_LENGTH;
      this.logWriter = new SchemaFileLogWriter(logPath);
      logCounter = new AtomicInteger(pageAcc);
      flushDirtyPagesStrategy = this::flushDirtyPagesWithLogging;
      singlePageFlushStrategy = this::flushSinglePageWithLogging;
    }

    // construct first page if file to init
    if (lastPageIndex < 0) {
      ISegmentedPage rootPage =
          ISchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
      pageInstCache.put(rootPage.getPageIndex(), rootPage);
      rootPage.syncPageBuffer();
      rootPage.flushPageToChannel(channel);
    }
  }

  /** Load bytes from log, deserialize and flush directly into channel, return current length */
  private long recoverFromLog(String logPath) throws IOException, MetadataException {
    SchemaFileLogReader reader = new SchemaFileLogReader(logPath);
    ISchemaPage page;
    List<byte[]> res = reader.collectUpdatedEntries();
    for (byte[] entry : res) {
      // TODO check bytes semantic correctness with CRC32 or other way
      page = ISchemaPage.loadSchemaPage(ByteBuffer.wrap(entry));
      page.flushPageToChannel(this.channel);
    }
    reader.close();

    // complete log file
    if (!res.isEmpty()) {
      try (FileOutputStream outputStream = new FileOutputStream(logPath, true)) {
        outputStream.write(new byte[] {SchemaFileConfig.SF_COMMIT_MARK});
        return outputStream.getChannel().size();
      }
    }
    return 0L;
  }

  /**
   * A rough cache size guardian, all threads passed this entrant check will not be limited with
   * cache size anymore. TODO A better guardian is based on constraint per thread.
   */
  protected void cacheGuardian() {
    cacheLock.lock();
    try {
      while (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
        try {
          // try to evict by LRU
          Iterator<ISchemaPage> iterator = pageInstCache.values().iterator();
          int pageSizeLimit = SchemaFileConfig.PAGE_CACHE_SIZE, size = pageInstCache.size();

          ISchemaPage p;
          while (iterator.hasNext()) {
            p = iterator.next();

            if (size <= pageSizeLimit) {
              break;
            }

            if (p.getRefCnt().get() == 0) {
              iterator.remove();
              size--;
            }
          }

          if (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
            // wait until another operation finished and released pages
            cacheFull.await();
          }
        } catch (InterruptedException e) {
          logger.warn(
              "Interrupted during page cache eviction. Consider increasing cache size, "
                  + "reducing concurrency, or extending timeout");
        }
      }
    } finally {
      cacheLock.unlock();
    }
  }

  @Override
  public void writeMNode(ICachedMNode node) throws MetadataException, IOException {
    SchemaPageContext cxt = new SchemaPageContext();
    threadContexts.put(Thread.currentThread().getId(), cxt);
    cacheGuardian();
    entrantLock(node, cxt);
    try {
      writeNewChildren(node, cxt);
      writeUpdatedChildren(node, cxt);
      flushDirtyPages(cxt);
    } finally {
      releaseLocks(cxt);
      releaseReferent(cxt);
      threadContexts.remove(Thread.currentThread().getId(), cxt);
    }
  }

  /** Context only tracks write locks, and so it shall be released. */
  protected void releaseLocks(SchemaPageContext cxt) {
    for (int i : cxt.lockTraces) {
      cxt.referredPages.get(i).getLock().writeLock().unlock();
    }
  }

  /** release referents and evict likely useless page if necessary */
  protected void releaseReferent(SchemaPageContext cxt) {
    for (ISchemaPage p : cxt.referredPages.values()) {
      p.decrementAndGetRefCnt();
    }

    if (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
      cacheLock.lock();
      try {
        for (ISchemaPage p : cxt.referredPages.values()) {
          // unnecessary to evict the page object in context by 2 case:
          //  1. it is held by another thread, e.g., RefCnt != 0
          //  2. it had already been evicted, e.g., pageCache.get(id) != page
          if (p.getRefCnt().get() == 0 && pageInstCache.get(p.getPageIndex()) == p) {
            pageInstCache.remove(p.getPageIndex());
          }
        }

        if (pageInstCache.size() <= SchemaFileConfig.PAGE_CACHE_SIZE) {
          cacheFull.signal();
        }
      } finally {
        cacheLock.unlock();
      }
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
    ICachedMNode child;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    String alias;
    // TODO: reserve order of insert in container may be better
    for (Map.Entry<String, ICachedMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().entrySet().stream()
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

      try {
        curPage
            .getAsSegmentedPage()
            .write(SchemaFile.getSegIndex(actualAddress), entry.getKey(), childBuffer);
        interleavedFlush(curPage, cxt);
        cxt.markDirty(curPage);

        subIndex = subIndexRootPage(curSegAddr, cxt);
        if (alias != null && subIndex >= 0) {
          insertSubIndexEntry(subIndex, alias, entry.getKey(), cxt);
        }

      } catch (SchemaPageOverflowException e) {
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
                      .getNewChildBuffer()
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
    for (Map.Entry<String, ICachedMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getUpdatedChildBuffer().entrySet()) {
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

      try {
        curPage
            .getAsSegmentedPage()
            .update(SchemaFile.getSegIndex(actualAddress), entry.getKey(), childBuffer);
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
      } catch (SchemaPageOverflowException e) {
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
  @FunctionalInterface
  interface FlushPageStrategy {
    void apply(List<ISchemaPage> dirtyPages) throws IOException;
  }

  @FunctionalInterface
  interface SinglePageFlushStrategy {
    void apply(ISchemaPage page) throws IOException;
  }

  private void flushDirtyPagesWithLogging(List<ISchemaPage> dirtyPages) throws IOException {
    if (dirtyPages.size() == 0) {
      return;
    }

    if (logCounter.get() > SchemaFileConfig.SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(dirtyPages.size());
    for (ISchemaPage page : dirtyPages) {
      page.syncPageBuffer();
      logWriter.write(page);
    }
    logWriter.prepare();

    for (ISchemaPage page : dirtyPages) {
      page.flushPageToChannel(channel);
    }
    logWriter.commit();
  }

  private void flushSinglePageWithLogging(ISchemaPage page) throws IOException {
    if (logCounter.get() > SchemaFileConfig.SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(1);
    page.syncPageBuffer();
    logWriter.write(page);
    logWriter.prepare();
    page.flushPageToChannel(channel);
    logWriter.commit();
  }

  private void flushDirtyPagesWithoutLogging(List<ISchemaPage> dirtyPages) throws IOException {
    for (ISchemaPage page : dirtyPages) {
      page.syncPageBuffer();
      page.flushPageToChannel(channel);
    }
  }

  private void flushSinglePageWithoutLogging(ISchemaPage page) throws IOException {
    page.syncPageBuffer();
    page.flushPageToChannel(channel);
  }

  public synchronized void flushDirtyPages(SchemaPageContext cxt) throws IOException {
    if (cxt.dirtyCnt == 0) {
      return;
    }
    flushDirtyPagesStrategy.apply(
        cxt.referredPages.values().stream()
            .filter(ISchemaPage::isDirtyPage)
            .collect(Collectors.toList()));
    cxt.appendBucketIndex(pageIndexBuckets);
    if (metric != null) {
      metric.recordFlushPageNum(cxt.referredPages.size());
    }
  }

  /**
   * when page prepares to insert, compare it with lastLeaf from context. if not same, then flush
   * the lastLeaf, unlock, deref, and remove it from dirtyPages. lastLeafPage only initiated at
   * overflowOperation
   */
  private void interleavedFlush(ISchemaPage page, SchemaPageContext cxt) throws IOException {
    if (cxt.lastLeafPage == null || cxt.lastLeafPage.getPageIndex() == page.getPageIndex()) {
      return;
    }
    cxt.interleavedFlushCnt++;
    if (metric != null) {
      metric.recordFlushPageNum(1);
    }
    singlePageFlushStrategy.apply(cxt.lastLeafPage);
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
    pageInstCache.remove(cxt.lastLeafPage.getPageIndex());

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
    pageInstCache.clear();
    lastPageIndex.set(0);
    logWriter = logWriter == null ? null : logWriter.renew();
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
    if (logWriter != null) {
      logWriter.close();
    }
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
    cacheLock.lock();
    try {
      ISchemaPage page = pageInstCache.get(pageIdx);
      if (page != null) {
        cxt.refer(page);
        return page;
      }

      ByteBuffer newBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
      if (metric != null) {
        metric.recordLoadPageNum(1);
      }
      loadFromFile(newBuf, pageIdx);
      page = ISchemaPage.loadSchemaPage(newBuf);
      cxt.refer(page);
      addPageToCache(page);
      return page;
    } finally {
      cacheLock.unlock();
    }
  }

  private long preAllocateSegment(short size, SchemaPageContext cxt)
      throws IOException, MetadataException {
    ISegmentedPage page = getMinApplSegmentedPageInMem(size, cxt);
    return SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  protected ISchemaPage replacePageInCache(ISchemaPage page, SchemaPageContext cxt) {
    // no need to lock since the root of B+Tree is locked
    cxt.markDirty(page, true);
    addPageToCache(page);
    return page;
  }

  /**
   * Only accessed during write operation and every page returned is due to be modified thus they
   * are all marked dirty.
   *
   * @param size size of the expected segment
   * @return always write locked
   */
  protected ISegmentedPage getMinApplSegmentedPageInMem(short size, SchemaPageContext cxt) {
    // pages retrieved from context is unnecessary and inefficient to lock
    ISchemaPage targetPage = cxt.indexBuckets.getNearestFitPage(size, false);
    if (targetPage != null) {
      cxt.indexBuckets.sortIntoBucket(targetPage, size);
      return targetPage.getAsSegmentedPage();
    }

    cacheLock.lock();
    try {
      // pageIndexBuckets sorts pages within pageInstCache into buckets to expedite access
      targetPage = pageIndexBuckets.getNearestFitPage(size, true);
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
      cacheLock.unlock();
    }
  }

  protected ISchemaPage allocNewSegmentedPage(SchemaPageContext cxt) {
    ISchemaPage newPage =
        ISchemaPage.initSegmentedPage(
            ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), lastPageIndex.incrementAndGet());
    // lock and mark to be consistent with # getMinApplSegmentedPageInMem
    cxt.markDirty(newPage);
    newPage.getLock().writeLock().lock();
    cxt.traceLock(newPage);
    addPageToCache(newPage);
    return newPage;
  }

  protected ISchemaPage registerAsNewPage(ISchemaPage page, SchemaPageContext cxt) {
    // new page will be LOCAL until the creating thread finished
    //  thus not added to sparsePageIndex now
    page.setPageIndex(lastPageIndex.incrementAndGet());
    addPageToCache(page);
    cxt.markDirty(page);
    return page;
  }

  protected ISchemaPage addPageToCache(ISchemaPage page) {
    // size control is left to operation entrance
    // return value could use to assess whether key already existed
    return this.pageInstCache.put(page.getPageIndex(), page);
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    if (!readChannel.isOpen()) {
      readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);
    }
    return readChannel.read(dst, getPageAddress(pageIndex));
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

  /** Thread local variables about write/update process. */
  protected static class SchemaPageContext {
    final long threadID;
    final PageIndexSortBuckets indexBuckets;
    // locked and dirty pages are all referred pages, they all reside in page cache
    final Map<Integer, ISchemaPage> referredPages;
    final Set<Integer> lockTraces;
    // track B+Tree traversal trace
    final int[] treeTrace;
    int dirtyCnt;
    int interleavedFlushCnt;

    // flush B+Tree leaf before operation finished since all records are ordered
    ISegmentedPage lastLeafPage;

    public SchemaPageContext() {
      threadID = Thread.currentThread().getId();
      referredPages = new HashMap<>();
      indexBuckets = new PageIndexSortBuckets(SchemaFileConfig.SEG_SIZE_LST, referredPages);
      treeTrace = new int[16];
      lockTraces = new HashSet<>();
      lastLeafPage = null;
      dirtyCnt = 0;
      interleavedFlushCnt = 0;
    }

    protected void markDirty(ISchemaPage page) {
      markDirty(page, false);
    }

    protected void markDirty(ISchemaPage page, boolean forceReplace) {
      if (!page.isDirtyPage()) {
        dirtyCnt++;
      }
      page.setDirtyFlag();
      refer(page);
      if (forceReplace && referredPages.containsKey(page.getPageIndex())) {

        // previous page is dirty, so it's not a new dirty page
        if (referredPages.get(page.getPageIndex()).isDirtyPage()) {
          dirtyCnt--;
        }
        // force to replace
        referredPages.put(page.getPageIndex(), page);
      }
    }

    protected void traceLock(ISchemaPage page) {
      refer(page);
      lockTraces.add(page.getPageIndex());
    }

    // referred pages will not be evicted until operation finished
    private void refer(ISchemaPage page) {
      if (!referredPages.containsKey(page.getPageIndex())) {
        page.incrementAndGetRefCnt();
        referredPages.put(page.getPageIndex(), page);
      }
    }

    /**
     * Since records are ordered for write operation, it is reasonable to flush those left siblings
     * of the active leaf page. The target page would be initiated at the first split within the
     * operation.
     *
     * @param page left leaf of the split
     */
    public void invokeLastLeaf(ISchemaPage page) {
      // only record at the first split
      if (lastLeafPage == null) {
        lastLeafPage = page.getAsSegmentedPage();
      }
    }

    private void appendBucketIndex(PageIndexSortBuckets pisb) {
      for (int i = 0; i < indexBuckets.buckets.length; i++) {
        pisb.buckets[i].addAll(indexBuckets.buckets[i]);
      }
    }
  }

  /**
   * * Index buckets affiliated to a page collection. Indexes are sort into buckets according to
   * spare space of its corresponding page instance.
   */
  protected static class PageIndexSortBuckets {
    private final short[] bounds;
    private final LinkedList<Integer>[] buckets;
    private final Map<Integer, ISchemaPage> pageContainer;

    public PageIndexSortBuckets(short[] borders, Map<Integer, ISchemaPage> container) {
      bounds = Arrays.copyOf(borders, borders.length);
      buckets = (LinkedList<Integer>[]) new LinkedList[borders.length];
      pageContainer = container;
      for (int i = 0; i < borders.length; i++) {
        buckets[i] = new LinkedList<>();
      }
    }

    public void clear() {
      for (Queue<Integer> q : buckets) {
        q.clear();
      }
    }

    public void sortIntoBucket(ISchemaPage page, short newSegSize) {
      if (page.getAsSegmentedPage() == null) {
        return;
      }

      // actual space occupied by a segment includes both its own length and the length of its
      // offset. so available length for a segment is the spareSize minus the offset bytes
      short availableSize =
          newSegSize < 0
              ? (short) (page.getAsSegmentedPage().getSpareSize() - SchemaFileConfig.SEG_OFF_DIG)
              : (short)
                  (page.getAsSegmentedPage().getSpareSize()
                      - newSegSize
                      - SchemaFileConfig.SEG_OFF_DIG);

      // too small to index
      if (availableSize <= SchemaFileConfig.SEG_HEADER_SIZE) {
        return;
      }

      // be like: SEG_HEADER < buckets[0] <= bounds[0] < buckets[1] <= ...
      for (int i = 0; i < bounds.length; i++) {
        // the last of SEG_SIZE_LST is the maximum page size, definitely larger than others
        if (availableSize <= bounds[i]) {
          buckets[i].add(page.getPageIndex());
          return;
        }
      }
    }

    /**
     * @param withLock set if page container is a global/shared object
     * @return the page index will be removed from the bucket.
     */
    public synchronized ISchemaPage getNearestFitPage(short size, boolean withLock) {
      ISchemaPage targetPage;
      int elemToCheck;
      for (int i = 0; i < buckets.length && pageContainer.size() > 0; i++) {
        // buckets[i] stores pages with spare space less than bounds[i]
        elemToCheck = buckets[i].size();
        while (size < bounds[i] && elemToCheck > 0) {
          // find roughly fit page
          targetPage = pageContainer.getOrDefault(buckets[i].poll(), null);
          elemToCheck--;

          if (targetPage == null || targetPage.getAsSegmentedPage() == null) {
            // act as lazy remove on buckets
            continue;
          }

          // seek in global container thus other page could be read locked
          if (withLock
              && targetPage.getAsSegmentedPage().isCapableForSegSize(size)
              && targetPage.getLock().writeLock().tryLock()) {
            if (targetPage.getAsSegmentedPage().isCapableForSegSize(size)) {
              return targetPage.getAsSegmentedPage();
            }
            targetPage.getLock().writeLock().unlock();
          }

          // only in local dirty pages which are always write locked by itself
          if (!withLock && targetPage.getAsSegmentedPage().isCapableForSegSize(size)) {
            return targetPage;
          }

          // not large as expected, fit into suitable bucket
          if (i > 0 && targetPage.getAsSegmentedPage().isCapableForSegSize(bounds[0])) {
            sortIntoBucket(targetPage, (short) -1);
          }
        }
      }
      return null;
    }
  }

  @Override
  public void setMetric(SchemaRegionCachedMetric metric) {
    this.metric = metric;
  }
}
