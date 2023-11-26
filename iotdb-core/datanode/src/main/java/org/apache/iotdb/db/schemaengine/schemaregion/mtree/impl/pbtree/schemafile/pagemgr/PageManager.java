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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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

  protected final Map<Integer, ISchemaPage> pageInstCache;

  protected final PageLocks pageLocks;

  protected final AtomicInteger lastPageIndex;

  private final FileChannel channel;

  // ALL are thread-locals, TODO remove after fixed
  // protected final Map<Integer, ISchemaPage> dirtyPages;
  // optimize retrieval of the smallest applicable DIRTY segmented page
  // tiered by: MIN_SEG_SIZE, PAGE/16, PAGE/8, PAGE/4, PAGE/2, PAGE_SIZE
  // protected final LinkedList<Integer>[] tieredDirtyPageIndex = new LinkedList[SchemaFileConfig.SEG_SIZE_LST.length];
  // shift to ThreadLocal if concurrent write expected
  // protected int[] treeTrace;

  // handle timeout interruption during reading
  private File pmtFile;
  private FileChannel readChannel;

  private final AtomicInteger logCounter;
  private SchemaFileLogWriter logWriter;

  // flush strategy is dependent on consensus protocol, only check protocol on init
  protected FlushPageStrategy flushDirtyPagesStrategy;

  PageManager(FileChannel channel, File pmtFile, int lastPageIndex, String logPath)
      throws IOException, MetadataException {
    this.pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaFileConfig.PAGE_CACHE_SIZE, 1, true));

    this.pageLocks = new PageLocks();
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
    } else {
      // without RATIS, utilize physical logging for integrity
      int pageAcc = (int) recoverFromLog(logPath) / SchemaFileConfig.PAGE_LENGTH;
      this.logWriter = new SchemaFileLogWriter(logPath);
      logCounter = new AtomicInteger(pageAcc);
      flushDirtyPagesStrategy = this::flushDirtyPagesWithLogging;
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
        long length = outputStream.getChannel().size();
        return length;
      }
    }
    return 0L;
  }

  // region Framework Methods
  @Override
  public void writeNewChildren(ICachedMNode node) throws MetadataException, IOException {
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    ICachedMNode child;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    String alias;
    SchemaPageUpdateContext cxt = new SchemaPageUpdateContext();
    // TODO: reserve order of insert in container may be better
    for (Map.Entry<String, ICachedMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      // check and pre-allocate
      child = entry.getValue();
      if (!child.isMeasurement()) {
        alias = null;

        if (getNodeAddress(child) >= 0) {
          // new child with a valid segment address, weird
          throw new MetadataException(
              String.format(
                  "A child [%s] in newChildBuffer shall not have segmentAddress.",
                  child.getFullPath()));
        }

        // pre-allocate except that child is a device node using template
        if (!(child.isDevice() && child.getAsDeviceMNode().isUseTemplate())) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize, cxt);
          SchemaFile.setNodeAddress(child, glbIndex);
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
        markDirty(curPage, cxt);
        addPageToCache(curPage.getPageIndex(), curPage);

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
          SchemaFile.setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr, cxt);
          markDirty(curPage, cxt);
          addPageToCache(curPage.getPageIndex(), curPage);
        }
      }
    }
    // TODO context could be saved by ThreadID, and flush under orchestration
    flushDirtyPages(cxt);
  }

  @Override
  public void writeUpdatedChildren(ICachedMNode node) throws MetadataException, IOException {
    boolean removeOldSubEntry = false, insertNewSubEntry = false;
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    String alias, oldAlias; // key of the sub-index entry now
    ICachedMNode child, oldChild;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    SchemaPageUpdateContext cxt = new SchemaPageUpdateContext();
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
        markDirty(curPage, cxt);
        addPageToCache(curPage.getPageIndex(), curPage);

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

          newPage.update(SchemaFile.getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          SchemaFile.setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr, cxt);
          markDirty(curPage, cxt);
          addPageToCache(curPage.getPageIndex(), curPage);
        }
      }
    }
    // TODO context could be saved by ThreadID, and flush under orchestration
    flushDirtyPages(cxt);
  }

  // endregion

  // region Abstract and Overridable Methods
  protected abstract long getTargetSegmentAddress(long curAddr, String recKey, SchemaPageUpdateContext cxt)
      throws IOException, MetadataException;

  /**
   * Deal with split, transplant and index update about the overflow occurred by insert.
   *
   * @param curPage the page encounters overflow, shall be a {@linkplain SegmentedPage}.
   * @param key the key to insert
   * @param childBuffer content of the key
   */
  protected abstract void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageUpdateContext cxt)
      throws MetadataException, IOException;

  /**
   * Same as {@linkplain #multiPageInsertOverflowOperation}
   *
   * @return the top internal page
   */
  protected abstract void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageUpdateContext cxt)
      throws MetadataException, IOException;

  /**
   * Occurs only when an Entity Node encounters a multipart overflow.
   *
   * @param parNode node needs to build subordinate index.
   */
  protected abstract void buildSubIndex(ICachedMNode parNode, SchemaPageUpdateContext cxt) throws MetadataException, IOException;

  /**
   * Insert an entry of subordinate index of the target node.
   *
   * @param base index of the top page contains sub-index entries.
   * @param key key of the sub-index entry.
   * @param rec value of the sub-index entry.
   */
  protected abstract void insertSubIndexEntry(int base, String key, String rec, SchemaPageUpdateContext cxt)
      throws MetadataException, IOException;

  protected abstract void removeSubIndexEntry(int base, String oldAlias, SchemaPageUpdateContext cxt)
      throws MetadataException, IOException;

  protected abstract String searchSubIndexAlias(int base, String alias, SchemaPageUpdateContext cxt)
      throws MetadataException, IOException;

  // endregion

  // region General Interfaces
  @Override
  public int getLastPageIndex() {
    return lastPageIndex.get();
  }

  @FunctionalInterface
  interface FlushPageStrategy {
    void apply(SchemaPageUpdateContext cxt) throws IOException;
  }

  private void flushDirtyPagesWithLogging(SchemaPageUpdateContext cxt) throws IOException {
    if (logCounter.get() > SchemaFileConfig.SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(cxt.dirtyPages.size());
    for (ISchemaPage page : cxt.dirtyPages.values()) {
      page.syncPageBuffer();
      logWriter.write(page);
    }
    logWriter.prepare();

    for (ISchemaPage page : cxt.dirtyPages.values()) {
      page.flushPageToChannel(channel);
    }
    logWriter.commit();
  }

  private void flushDirtyPagesWithoutLogging(SchemaPageUpdateContext cxt) throws IOException {
    for (ISchemaPage page : cxt.dirtyPages.values()) {
      page.syncPageBuffer();
      page.flushPageToChannel(channel);
    }
  }


  @Override
  public synchronized void flushDirtyPages() throws IOException {
    // FIXME associate flush with context

  }

  public synchronized void flushDirtyPages(SchemaPageUpdateContext cxt) throws IOException {
    if (cxt.dirtyPages.size() == 0) {
      return;
    }
    flushDirtyPagesStrategy.apply(cxt);
    cxt.dirtyPages.clear();
    Arrays.stream(cxt.tieredDirtyPageIndex).forEach(LinkedList::clear);
  }

  @Override
  public void clear() throws IOException, MetadataException {
    pageInstCache.clear();
    lastPageIndex.set(0);
    logWriter = logWriter == null ? null : logWriter.renew();
  }

  @Override
  public void inspect(PrintWriter pw) throws IOException, MetadataException {
    String pageContent;
    for (int i = 0; i <= lastPageIndex.get(); i++) {
      pageContent = getPageInstance(i, null).inspect();
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


  // region Page Lock Management
  @Override
  public void writeLockSegment(ICachedMNode node) {
    // pageLocks.getLock(getPageIndex(getNodeAddress(node))).writeLock().lock();
  }

  @Override
  public void entrantLock(ICachedMNode node) {
    // int pageIndex, parentPageIndex;
    // pageIndex = getPageIndex(getNodeAddress(node));
    // parentPageIndex = getPageIndex(getNodeAddress(node.getParent()));
    //
    // int minPageIndex = Math.min(pageIndex, parentPageIndex);
    // int maxPageIndex = Math.max(pageIndex, parentPageIndex);
    //
    // pageLocks.getLock(minPageIndex).writeLock().lock();
    //
    // if (minPageIndex != maxPageIndex) {
    //   pageLocks.getLock(maxPageIndex).writeLock().lock();
    // }
  }

  @Override
  public void releaseEntrantLock(ICachedMNode node) throws IOException, MetadataException {
    // pageLocks.getLock(getPageIndex(getNodeAddress(node))).writeLock().unlock();
    //
    // if (!node.isDatabase()
    //     && getPageIndex(getNodeAddress(node)) != getPageIndex(getNodeAddress(node.getParent()))) {
    //   pageLocks.getLock(getPageIndex(getNodeAddress(node.getParent()))).writeLock().unlock();
    // }
  }

  // endregion


  public synchronized ISchemaPage getPageInstance(int pageIdx, SchemaPageUpdateContext cxt) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex.get()) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    if (cxt != null && cxt.dirtyPages.containsKey(pageIdx)) {
      return cxt.dirtyPages.get(pageIdx);
    }

    if (pageInstCache.containsKey(pageIdx)) {
      return pageInstCache.get(pageIdx);
    }

    ByteBuffer newBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
    loadFromFile(newBuf, pageIdx);
    return addPageToCache(pageIdx, ISchemaPage.loadSchemaPage(newBuf));
  }

  private long preAllocateSegment(short size, SchemaPageUpdateContext cxt) throws IOException, MetadataException {
    ISegmentedPage page = getMinApplSegmentedPageInMem(size, cxt);
    return SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  protected ISchemaPage replacePageInCache(ISchemaPage page, SchemaPageUpdateContext cxt) {
    markDirty(page, cxt);
    return addPageToCache(page.getPageIndex(), page);
  }

  /**
   * Accessing dirtyPages will be expedited, while the retrieval from pageInstCache remains
   * unaffected due to its limited capacity.
   *
   * @param size size of the expected segment
   */
  protected ISegmentedPage getMinApplSegmentedPageInMem(short size, SchemaPageUpdateContext cxt) {
    ISchemaPage targetPage = null;
    int tierLoopCnt = 0;
    for (int i = 0; i < cxt.tieredDirtyPageIndex.length && cxt.dirtyPages.size() > 0; i++) {
      tierLoopCnt = cxt.tieredDirtyPageIndex[i].size();
      while (size < SchemaFileConfig.SEG_SIZE_LST[i] && tierLoopCnt > 0) {
        targetPage = cxt.dirtyPages.get(cxt.tieredDirtyPageIndex[i].pop());
        tierLoopCnt--;

        //  check validity of the retrieved targetPage, as the page type may have changed,
        //   e.g., from SegmentedPage to InternalPage, or index could be stale
        if (targetPage == null || targetPage.getAsSegmentedPage() == null) {
          // invalid index for SegmentedPage, drop the index and get next
          continue;
        }

        // suitable page for requested size
        if (targetPage.getAsSegmentedPage().isCapableForSegSize(size)) {
          sortSegmentedIntoIndex(targetPage, size, cxt);
          return targetPage.getAsSegmentedPage();
        }

        // not large enough but legal for another retrieval
        if (targetPage.getAsSegmentedPage().isCapableForSegSize(SchemaFileConfig.SEG_SIZE_LST[i])) {
          cxt.tieredDirtyPageIndex[i].add(targetPage.getPageIndex());
        }
      }
    }

    // TODO refactor design related to pageInstCache to index its pages in further development
    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().getAsSegmentedPage().isCapableForSegSize(size)) {
        markDirty(entry.getValue(), cxt);
        return pageInstCache.get(entry.getKey()).getAsSegmentedPage();
      }
    }
    return allocateNewSegmentedPage(cxt).getAsSegmentedPage();
  }

  /**
   * Index SegmentedPage inside dirtyPages into tiered list by {@linkplain
   * SchemaFileConfig#SEG_SIZE_LST}.
   *
   * <p>The level of its index depends on its AVAILABLE space.
   *
   * @param page SegmentedPage to be indexed, no guardian statements since all entrances are secured
   *     for now
   * @param newSegSize to re-integrate after a retrieval, the expected overhead shall be considered.
   *     -1 for common dirty mark.
   */
  protected void sortSegmentedIntoIndex(ISchemaPage page, short newSegSize, SchemaPageUpdateContext cxt) {
    // actual space occupied by a segment includes both its own length and the length of its offset.
    // so available length for a segment is the spareSize minus the offset length
    short availableSize =
        newSegSize < 0
            ? (short) (page.getAsSegmentedPage().getSpareSize() - SchemaFileConfig.SEG_OFF_DIG)
            : (short)
                (page.getAsSegmentedPage().getSpareSize()
                    - newSegSize
                    - SchemaFileConfig.SEG_OFF_DIG);

    // too small to index
    if (availableSize < SchemaFileConfig.SEG_HEADER_SIZE) {
      return;
    }

    // index range like: SEG_HEADER_SIZE <= [0] < SEG_SIZE_LST[0], ...
    for (int i = 0; i < SchemaFileConfig.SEG_SIZE_LST.length; i++) {
      // the last of SEG_SIZE_LST is the maximum page size, definitely larger than others
      if (availableSize < SchemaFileConfig.SEG_SIZE_LST[i]) {
        cxt.tieredDirtyPageIndex[i].add(page.getPageIndex());
        return;
      }
    }
  }

  protected ISchemaPage allocateNewSegmentedPage(SchemaPageUpdateContext cxt) {
    lastPageIndex.incrementAndGet();
    ISchemaPage newPage =
        ISchemaPage.initSegmentedPage(
            ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), lastPageIndex.get());
    markDirty(newPage, cxt);
    return addPageToCache(newPage.getPageIndex(), newPage);
  }

  protected ISchemaPage registerAsNewPage(ISchemaPage page, SchemaPageUpdateContext cxt) {
    page.setPageIndex(lastPageIndex.incrementAndGet());
    markDirty(page, cxt);
    return addPageToCache(page.getPageIndex(), page);
  }

  protected void markDirty(ISchemaPage page, SchemaPageUpdateContext cxt) {
    page.markDirty();
    cxt.dirtyPages.put(page.getPageIndex(), page);

    if (page.getAsSegmentedPage() != null) {
      sortSegmentedIntoIndex(page, (short) -1, cxt);
    }
  }

  protected synchronized ISchemaPage addPageToCache(int pageIndex, ISchemaPage page) {
    pageInstCache.put(pageIndex, page);
    // only one thread evicts and flushes pages
    if (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
      int removeCnt =
          (int) (0.2 * pageInstCache.size()) > 0 ? (int) (0.2 * pageInstCache.size()) : 1;
      List<Integer> rmvIds = new ArrayList<>(pageInstCache.keySet()).subList(0, removeCnt);

      // dirty pages only flushed from dirtyPages
      for (Integer id : rmvIds) {
        pageInstCache.remove(id);
      }
    }
    return page;
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    if (!readChannel.isOpen()) {
      readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);
    }
    return readChannel.read(dst, getPageAddress(pageIndex));
  }

  private void updateParentalRecord(ICachedMNode parent, String key, long newSegAddr, SchemaPageUpdateContext cxt)
      throws IOException, MetadataException {
    if (parent == null || parent.getChild(key).isDatabase()) {
      throw new MetadataException("Root page shall not be migrated.");
    }
    long parSegAddr = parent.getParent() == null ? 0L : getNodeAddress(parent);
    parSegAddr = getTargetSegmentAddress(parSegAddr, key, cxt);
    ISchemaPage page = getPageInstance(SchemaFile.getPageIndex(parSegAddr), cxt);
    ((SegmentedPage) page).updateRecordSegAddr(SchemaFile.getSegIndex(parSegAddr), key, newSegAddr);
    markDirty(page, cxt);
  }

  // endregion

  // region Inner Utilities

  private int subIndexRootPage(long addr, SchemaPageUpdateContext cxt) throws IOException, MetadataException {
    return getPageInstance(SchemaFile.getPageIndex(addr), cxt).getSubIndex();
  }

  private static long getPageAddress(int pageIndex) {
    return (SchemaFileConfig.PAGE_INDEX_MASK & pageIndex) * SchemaFileConfig.PAGE_LENGTH
        + SchemaFileConfig.FILE_HEADER_SIZE;
  }

  /**
   * Estimate segment size for pre-allocate
   *
   * @param node
   * @return
   */
  private static short estimateSegmentSize(ICachedMNode node) {
    int childNum = node.getChildren().size();
    if (childNum < SchemaFileConfig.SEG_SIZE_METRIC[0]) {
      // for record offset, length of string key
      int totalSize = SchemaFileConfig.SEG_HEADER_SIZE + 6 * childNum;
      for (ICachedMNode child : node.getChildren().values()) {
        totalSize += child.getName().getBytes().length;
        if (child.isMeasurement()) {
          totalSize +=
              child.getAsMeasurementMNode().getAlias() == null
                  ? 4
                  : child.getAsMeasurementMNode().getAlias().getBytes().length + 4;
          totalSize += 24; // slightly larger than actually HEADER size
        } else {
          totalSize += 16; // slightly larger
        }
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
   * @param batchSize size of children within one {@linkplain #writeNewChildren(ICachedMNode)}
   * @return estimated size
   * @throws MetadataException
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
  public long getTargetSegmentAddressOnTest(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    return getTargetSegmentAddress(curSegAddr, recKey, new SchemaPageUpdateContext());
  }

  @TestOnly
  public ISchemaPage getPageInstanceOnTest(int pageIdx) throws IOException, MetadataException {
    return getPageInstance(pageIdx, null);
  }

  // endregion

  /**
   * Concurrent read/write lock within one SchemaFile/PageManager.
   * Designed for concurrency of #pageInstCache previously, which is now synchronized.
   */
  private class PageLocks {
    // TODO pooling for better performance, i.e., remove/reuse locks rarely accessed
    private final ConcurrentHashMap<Integer, ReentrantReadWriteLock> locks;

    protected PageLocks() {
      locks = new ConcurrentHashMap<>();
    }

    private ReentrantReadWriteLock getLock(int page) {
      return locks.computeIfAbsent(page, k -> new ReentrantReadWriteLock());
    }
  }

  /** Thread local variables about write/update process. */
  public static class SchemaPageUpdateContext {
    final Map<Integer, ISchemaPage> dirtyPages;
    // optimize retrieval of the smallest applicable DIRTY segmented page
    // tiered by: MIN_SEG_SIZE, PAGE/16, PAGE/8, PAGE/4, PAGE/2, PAGE_SIZE
    final LinkedList<Integer>[] tieredDirtyPageIndex;
    // shift to ThreadLocal if concurrent write expected
    final int[] treeTrace;

    public SchemaPageUpdateContext() {
      dirtyPages = new HashMap<>();
      tieredDirtyPageIndex = new LinkedList[SchemaFileConfig.SEG_SIZE_LST.length];
      for (int i = 0; i < tieredDirtyPageIndex.length; i++) {
        tieredDirtyPageIndex[i] = new LinkedList<>();
      }
      treeTrace = new int[16];
    }
  }
}
