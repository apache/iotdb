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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISegmentedPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SegmentedPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.log.SchemaFileLogReader;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.log.SchemaFileLogWriter;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getNodeAddress;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getPageIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getSegIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.setNodeAddress;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.FILE_HEADER_SIZE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_CACHE_SIZE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_INDEX_MASK;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_LENGTH;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SCHEMA_FILE_LOG_SIZE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_HEADER_SIZE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_MAX_SIZ;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_MIN_SIZ;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_OFF_DIG;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_SIZE_LST;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_SIZE_METRIC;

/**
 * Abstraction for various implementation of structure of pages. But multi-level index is hard-coded
 * within the framework since only alias works like secondary index now.
 *
 * <p>Hierarchy of index may be decoupled from this framework in further improvement.
 */
public abstract class PageManager implements IPageManager {
  protected static final Logger logger = LoggerFactory.getLogger(PageManager.class);

  protected final Map<Integer, ISchemaPage> pageInstCache;
  protected final Map<Integer, ISchemaPage> dirtyPages;

  protected final ReentrantLock evictLock;
  protected final PageLocks pageLocks;

  protected final AtomicInteger lastPageIndex;

  // shift to ThreadLocal if concurrent write expected
  protected int[] treeTrace;

  private final FileChannel channel;

  // handle timeout interruption during reading
  private File pmtFile;
  private FileChannel readChannel;

  private final AtomicInteger logCounter;
  private SchemaFileLogWriter logWriter;

  PageManager(FileChannel channel, File pmtFile, int lastPageIndex, String logPath)
      throws IOException, MetadataException {
    this.pageInstCache = Collections.synchronizedMap(new LinkedHashMap<>(PAGE_CACHE_SIZE, 1, true));
    this.dirtyPages = new ConcurrentHashMap<>();
    this.evictLock = new ReentrantLock();
    this.pageLocks = new PageLocks();
    this.lastPageIndex =
        lastPageIndex >= 0 ? new AtomicInteger(lastPageIndex) : new AtomicInteger(0);
    this.treeTrace = new int[16];
    this.channel = channel;
    this.pmtFile = pmtFile;
    this.readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);

    // recover if log exists
    int pageAcc = (int) recoverFromLog(logPath) / PAGE_LENGTH;
    this.logWriter = new SchemaFileLogWriter(logPath);
    logCounter = new AtomicInteger(pageAcc);

    // construct first page if file to init
    if (lastPageIndex < 0) {
      ISegmentedPage rootPage = ISchemaPage.initSegmentedPage(ByteBuffer.allocate(PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SEG_MAX_SIZ);
      pageInstCache.put(rootPage.getPageIndex(), rootPage);
      markDirty(rootPage);
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
  public void writeNewChildren(IMNode node) throws MetadataException, IOException {
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    IMNode child;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    String alias;
    // TODO: reserve order of insert in container may be better
    for (Map.Entry<String, IMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      // check and pre-allocate
      child = entry.getValue();
      if (!child.isMeasurement()) {
        alias = null;
        if (getNodeAddress(child) < 0) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize);
          setNodeAddress(child, glbIndex);
        } else {
          // new child with a valid segment address, weird
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
      actualAddress = getTargetSegmentAddress(curSegAddr, entry.getKey());
      curPage = getPageInstance(getPageIndex(actualAddress));

      try {
        curPage.getAsSegmentedPage().write(getSegIndex(actualAddress), entry.getKey(), childBuffer);
        markDirty(curPage);
        addPageToCache(curPage.getPageIndex(), curPage);

        subIndex = subIndexRootPage(curSegAddr);
        if (alias != null && subIndex >= 0) {
          insertSubIndexEntry(subIndex, alias, entry.getKey());
        }

      } catch (SchemaPageOverflowException e) {
        if (curPage.getAsSegmentedPage().getSegmentSize(getSegIndex(actualAddress))
            == SEG_MAX_SIZ) {
          // curPage might be replaced so unnecessary to mark it here
          multiPageInsertOverflowOperation(curPage, entry.getKey(), childBuffer);

          subIndex = subIndexRootPage(curSegAddr);
          if (node.isEntity() && subIndex < 0) {
            // the record occurred overflow had been inserted already
            buildSubIndex(node);
          } else if (alias != null) {
            // implied node is entity, so sub index must exist
            insertSubIndexEntry(subIndex, alias, entry.getKey());
          }
        } else {
          // transplant and delete former segment
          short actSegId = getSegIndex(actualAddress);
          short newSegSize =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity(),
                  ICachedMNodeContainer.getCachedMNodeContainer(node)
                      .getNewChildBuffer()
                      .entrySet()
                      .size());
          ISegmentedPage newPage = getMinApplSegmentedPageInMem(newSegSize);

          // with single segment, curSegAddr equals actualAddress
          curSegAddr =
              newPage.transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSize);
          newPage.write(getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);
          setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
          markDirty(curPage);
          addPageToCache(curPage.getPageIndex(), curPage);
        }
      }
    }
  }

  @Override
  public void writeUpdatedChildren(IMNode node) throws MetadataException, IOException {
    boolean removeOldSubEntry = false, insertNewSubEntry = false;
    int subIndex;
    long curSegAddr = getNodeAddress(node);
    long actualAddress; // actual segment to write record
    String alias, oldAlias; // key of the sub-index entry now
    IMNode child, oldChild;
    ISchemaPage curPage;
    ByteBuffer childBuffer;
    for (Map.Entry<String, IMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getUpdatedChildBuffer().entrySet()) {
      child = entry.getValue();
      actualAddress = getTargetSegmentAddress(curSegAddr, entry.getKey());
      childBuffer = RecordUtils.node2Buffer(child);

      curPage = getPageInstance(getPageIndex(actualAddress));
      if (curPage.getAsSegmentedPage().read(getSegIndex(actualAddress), entry.getKey()) == null) {
        throw new MetadataException(
            String.format(
                "Node[%s] has no child[%s] in schema file.", node.getName(), entry.getKey()));
      }

      // prepare alias comparison
      if (child.isMeasurement() && child.getAsMeasurementMNode().getAlias() != null) {
        alias = child.getAsMeasurementMNode().getAlias();
      } else {
        alias = null;
      }
      if (node.isEntity()) {
        oldChild = curPage.getAsSegmentedPage().read(getSegIndex(actualAddress), entry.getKey());
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
            .update(getSegIndex(actualAddress), entry.getKey(), childBuffer);
        markDirty(curPage);
        addPageToCache(curPage.getPageIndex(), curPage);

        subIndex = subIndexRootPage(curSegAddr);
        if (subIndex >= 0) {
          if (removeOldSubEntry) {
            removeSubIndexEntry(subIndex, oldAlias);
          }

          if (insertNewSubEntry) {
            insertSubIndexEntry(subIndex, alias, entry.getKey());
          }
        }
      } catch (SchemaPageOverflowException e) {
        if (curPage.getAsSegmentedPage().getSegmentSize(getSegIndex(actualAddress))
            == SEG_MAX_SIZ) {
          multiPageUpdateOverflowOperation(curPage, entry.getKey(), childBuffer);

          subIndex = subIndexRootPage(curSegAddr);
          if (node.isEntity() && subIndex < 0) {
            buildSubIndex(node);
          } else if (insertNewSubEntry || removeOldSubEntry) {
            if (removeOldSubEntry) {
              removeSubIndexEntry(subIndex, oldAlias);
            }

            if (insertNewSubEntry) {
              insertSubIndexEntry(subIndex, alias, entry.getKey());
            }
          }
        } else {
          // transplant into another bigger segment
          short actSegId = getSegIndex(actualAddress);
          short newSegSiz =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity());
          ISegmentedPage newPage = getMinApplSegmentedPageInMem(newSegSiz);

          // assign new segment address
          curSegAddr = newPage.transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSiz);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);

          newPage.update(getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
          markDirty(curPage);
          addPageToCache(curPage.getPageIndex(), curPage);
        }
      }
    }
  }

  // endregion

  // region Abstract and Overridable Methods
  protected abstract long getTargetSegmentAddress(long curAddr, String recKey)
      throws IOException, MetadataException;

  /**
   * Deal with split, transplant and index update about the overflow occurred by insert.
   *
   * @param curPage the page encounters overflow, shall be a {@linkplain SegmentedPage}.
   * @param key the key to insert
   * @param childBuffer content of the key
   */
  protected abstract void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException;

  /**
   * Same as {@linkplain #multiPageInsertOverflowOperation}
   *
   * @return the top internal page
   */
  protected abstract void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException;

  /**
   * Occurs only when an Entity Node encounters a multipart overflow.
   *
   * @param parNode node needs to build subordinate index.
   */
  protected abstract void buildSubIndex(IMNode parNode) throws MetadataException, IOException;

  /**
   * Insert an entry of subordinate index of the target node.
   *
   * @param base index of the top page contains sub-index entries.
   * @param key key of the sub-index entry.
   * @param rec value of the sub-index entry.
   */
  protected abstract void insertSubIndexEntry(int base, String key, String rec)
      throws MetadataException, IOException;

  protected abstract void removeSubIndexEntry(int base, String oldAlias)
      throws MetadataException, IOException;

  protected abstract String searchSubIndexAlias(int base, String alias)
      throws MetadataException, IOException;

  // endregion

  // region General Interfaces
  @Override
  public int getLastPageIndex() {
    return lastPageIndex.get();
  }

  @Override
  public void flushDirtyPages() throws IOException {
    if (dirtyPages.size() == 0) {
      return;
    }

    // TODO: better performance expected while ensuring integrity when exception interrupts
    if (logCounter.get() > SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(dirtyPages.size());
    for (ISchemaPage page : dirtyPages.values()) {
      page.syncPageBuffer();
      logWriter.write(page);
    }
    logWriter.prepare();

    for (ISchemaPage page : dirtyPages.values()) {
      page.flushPageToChannel(channel);
    }
    logWriter.commit();
    dirtyPages.clear();
  }

  @Override
  public void clear() throws IOException, MetadataException {
    dirtyPages.clear();
    pageInstCache.clear();
    lastPageIndex.set(0);
    logWriter = logWriter.renew();
  }

  @Override
  public void inspect(PrintWriter pw) throws IOException, MetadataException {
    String pageContent;
    for (int i = 0; i <= lastPageIndex.get(); i++) {
      pageContent = getPageInstance(i).inspect();
      pw.print("---------------------\n");
      pw.print(pageContent);
      pw.print("\n");
    }
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
  }

  // endregion

  // region Page Access Management

  public ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex.get()) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    pageLocks.readLock(pageIdx);
    try {
      if (dirtyPages.containsKey(pageIdx)) {
        return dirtyPages.get(pageIdx);
      }

      if (pageInstCache.containsKey(pageIdx)) {
        return pageInstCache.get(pageIdx);
      }
    } finally {
      pageLocks.readUnlock(pageIdx);
    }

    try {
      pageLocks.writeLock(pageIdx);

      ByteBuffer newBuf = ByteBuffer.allocate(PAGE_LENGTH);

      loadFromFile(newBuf, pageIdx);
      return addPageToCache(pageIdx, ISchemaPage.loadSchemaPage(newBuf));
    } finally {
      pageLocks.writeUnlock(pageIdx);
    }
  }

  @Deprecated
  // TODO: improve to remove
  private long preAllocateSegment(short size) throws IOException, MetadataException {
    ISegmentedPage page = getMinApplSegmentedPageInMem((short) (size + SEG_OFF_DIG));
    return SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  protected ISchemaPage replacePageInCache(ISchemaPage page) {
    markDirty(page);
    return addPageToCache(page.getPageIndex(), page);
  }

  protected ISegmentedPage getMinApplSegmentedPageInMem(short size) {
    for (Map.Entry<Integer, ISchemaPage> entry : dirtyPages.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().getAsSegmentedPage().isCapableForSegSize(size)) {
        return dirtyPages.get(entry.getKey()).getAsSegmentedPage();
      }
    }

    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().getAsSegmentedPage().isCapableForSegSize(size)) {
        markDirty(entry.getValue());
        return pageInstCache.get(entry.getKey()).getAsSegmentedPage();
      }
    }
    return allocateNewSegmentedPage().getAsSegmentedPage();
  }

  protected synchronized ISchemaPage allocateNewSegmentedPage() {
    lastPageIndex.incrementAndGet();
    ISchemaPage newPage =
        ISchemaPage.initSegmentedPage(ByteBuffer.allocate(PAGE_LENGTH), lastPageIndex.get());
    markDirty(newPage);
    return addPageToCache(newPage.getPageIndex(), newPage);
  }

  protected synchronized ISchemaPage registerAsNewPage(ISchemaPage page) {
    page.setPageIndex(lastPageIndex.incrementAndGet());
    markDirty(page);
    return addPageToCache(page.getPageIndex(), page);
  }

  protected void markDirty(ISchemaPage page) {
    page.markDirty();
    dirtyPages.put(page.getPageIndex(), page);
  }

  protected ISchemaPage addPageToCache(int pageIndex, ISchemaPage page) {
    pageInstCache.put(pageIndex, page);
    // only one thread evicts and flushes pages
    if (evictLock.tryLock()) {
      try {
        if (pageInstCache.size() > PAGE_CACHE_SIZE) {
          int removeCnt =
              (int) (0.2 * pageInstCache.size()) > 0 ? (int) (0.2 * pageInstCache.size()) : 1;
          List<Integer> rmvIds = new ArrayList<>(pageInstCache.keySet()).subList(0, removeCnt);

          for (Integer id : rmvIds) {
            // dirty pages only flushed from dirtyPages
            if (pageLocks.findLock(id).writeLock().tryLock()) {
              try {
                pageInstCache.remove(id);
              } finally {
                pageLocks.findLock(id).writeLock().unlock();
              }
            }
          }
        }
      } finally {
        evictLock.unlock();
      }
    }
    return page;
  }

  private synchronized int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    if (!readChannel.isOpen()) {
      readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);
    }
    return readChannel.read(dst, getPageAddress(pageIndex));
  }

  private void updateParentalRecord(IMNode parent, String key, long newSegAddr)
      throws IOException, MetadataException {
    if (parent == null || parent.getChild(key).isStorageGroup()) {
      throw new MetadataException("Root page shall not be migrated.");
    }
    long parSegAddr = parent.getParent() == null ? 0L : getNodeAddress(parent);
    parSegAddr = getTargetSegmentAddress(parSegAddr, key);
    ISchemaPage page = getPageInstance(getPageIndex(parSegAddr));
    ((SegmentedPage) page).updateRecordSegAddr(getSegIndex(parSegAddr), key, newSegAddr);
    markDirty(page);
  }

  // endregion

  // region Inner Utilities

  private int subIndexRootPage(long addr) throws IOException, MetadataException {
    return getPageInstance(getPageIndex(addr)).getSubIndex();
  }

  private static long getPageAddress(int pageIndex) {
    return (PAGE_INDEX_MASK & pageIndex) * PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  /**
   * Estimate segment size for pre-allocate
   *
   * @param node
   * @return
   */
  private static short estimateSegmentSize(IMNode node) {
    int childNum = node.getChildren().size();
    if (childNum < SEG_SIZE_METRIC[0]) {
      // for record offset, length of string key
      int totalSize = SEG_HEADER_SIZE + 6 * childNum;
      for (IMNode child : node.getChildren().values()) {
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
      return (short) totalSize > SEG_MIN_SIZ ? (short) totalSize : SEG_MIN_SIZ;
    }

    int tier = SEG_SIZE_LST.length - 1;
    while (tier > 0) {
      if (childNum > SEG_SIZE_METRIC[tier]) {
        return SEG_SIZE_LST[tier];
      }
      tier--;
    }
    return SEG_SIZE_LST[0];
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
   * @param batchSize size of children within one {@linkplain #writeNewChildren(IMNode)}
   * @return estimated size
   * @throws MetadataException
   */
  private static short reEstimateSegSize(int expSize, int batchSize) throws MetadataException {
    if (batchSize < SEG_SIZE_METRIC[0]) {
      return reEstimateSegSize(expSize);
    }
    int base_tier = 0;
    for (int i = 0; i < SEG_SIZE_LST.length; i++) {
      if (SEG_SIZE_LST[i] >= expSize) {
        base_tier = i;
        break;
      }
    }
    int tier = SEG_SIZE_LST.length - 1;
    while (tier >= base_tier) {
      if (batchSize > SEG_SIZE_METRIC[tier]) {
        return SEG_SIZE_LST[tier];
      }
      tier--;
    }
    return SEG_SIZE_LST[base_tier];
  }

  private static short reEstimateSegSize(int expSize) throws MetadataException {
    if (expSize > SEG_MAX_SIZ) {
      // TODO: to support extreme large MNode
      throw new MetadataException(
          "Single record larger than half page is not supported in SchemaFile now.");
    }
    for (short size : SEG_SIZE_LST) {
      if (expSize < size) {
        return size;
      }
    }
    return SEG_MAX_SIZ;
  }

  // endregion

  // region TestOnly Methods

  @TestOnly
  public long getTargetSegmentAddressOnTest(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    return getTargetSegmentAddress(curSegAddr, recKey);
  }

  @TestOnly
  public ISchemaPage getPageInstanceOnTest(int pageIdx) throws IOException, MetadataException {
    return getPageInstance(pageIdx);
  }

  // endregion

  private static class PageLocks {
    /**
     * number of reentrant read write lock. Notice that this number should be a prime number for
     * uniform hash
     */
    private static final int NUM_OF_LOCKS = 1039;

    /** locks array */
    private final ReentrantReadWriteLock[] locks;

    protected PageLocks() {
      locks = new ReentrantReadWriteLock[NUM_OF_LOCKS];
      for (int i = 0; i < NUM_OF_LOCKS; i++) {
        locks[i] = new ReentrantReadWriteLock();
      }
    }

    public void readLock(int hash) {
      findLock(hash).readLock().lock();
    }

    public void readUnlock(int hash) {
      findLock(hash).readLock().unlock();
    }

    public void writeLock(int hash) {
      findLock(hash).writeLock().lock();
    }

    public void writeUnlock(int hash) {
      findLock(hash).writeLock().unlock();
    }

    private ReentrantReadWriteLock findLock(int hash) {
      return locks[hash % NUM_OF_LOCKS];
    }
  }
}
