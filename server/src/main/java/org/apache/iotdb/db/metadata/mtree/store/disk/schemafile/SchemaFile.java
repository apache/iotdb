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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaFileNotExists;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TODO: reliability design TBD
 *
 * <p>This class is mainly aimed to manage space all over the file.
 *
 * <p>This class is meant to open a .pmt(Persistent MTree) file, and maintains the header of the
 * file. It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside
 * a file. Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 */
public class SchemaFile implements ISchemaFile {

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  public static int FILE_HEADER_SIZE = 256; // size of file header in bytes

  // folder to store .pmt files
  public static String FILE_FOLDER =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir() + File.separator;

  public static int PAGE_LENGTH = 16 * 1024; // 16 kib for default
  public static short PAGE_HEADER_SIZE = 16;
  public static int PAGE_CACHE_SIZE =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getPageCacheSizeInSchemaFile(); // size of page cache
  public static int ROOT_INDEX = 0; // index of header page
  // 32 bit for page pointer, maximum .pmt file as 2^(32+14) bytes, 64 TiB
  public static int INDEX_LENGTH = 4;

  // byte length of short, which is the type of index of segment inside a page
  public static short SEG_OFF_DIG = 2;
  public static short SEG_MAX_SIZ = (short) (16 * 1024 - SchemaFile.PAGE_HEADER_SIZE - SEG_OFF_DIG);
  public static short[] SEG_SIZE_LST = {1024, 2 * 1024, 4 * 1024, 8 * 1024, SEG_MAX_SIZ};
  public static short SEG_MIN_SIZ =
      IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile() > SEG_MAX_SIZ
          ? SEG_MAX_SIZ
          : IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile();
  public static int SEG_INDEX_DIGIT = 16; // for type short
  public static long SEG_INDEX_MASK = 0xffffL; // to translate address

  // attributes for this schema file
  String filePath;
  String storageGroupName;
  long dataTTL;
  boolean isEntity;
  int templateHash;

  ByteBuffer headerContent;
  int lastPageIndex; // last page index of the file, boundary to grow

  // work as a naive cache for page instance
  final Map<Integer, ISchemaPage> pageInstCache;
  ISchemaPage rootPage;

  // attributes for file
  File pmtFile;
  FileChannel channel;

  private SchemaFile(String sgName, boolean override, long ttl, boolean isEntity)
      throws IOException, MetadataException {
    this.storageGroupName = sgName;
    filePath =
        SchemaFile.FILE_FOLDER
            + File.separator
            + sgName
            + File.separator
            + MetadataConstant.SCHEMA_FILE_NAME;

    pmtFile = new File(filePath);
    if (!pmtFile.exists() && !override) {
      throw new SchemaFileNotExists(filePath);
    }

    if (pmtFile.exists() && override) {
      logger.warn(
          String.format("Schema File [%s] will be overwritten since already exists.", filePath));
      Files.delete(Paths.get(pmtFile.toURI()));
    }

    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File folder = new File(SchemaFile.FILE_FOLDER + sgName);
      folder.mkdirs();
      pmtFile.createNewFile();
    }

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    pageInstCache = Collections.synchronizedMap(new LinkedHashMap<>(PAGE_CACHE_SIZE, 1, true));
    // will be overwritten if to init
    this.dataTTL = ttl;
    this.isEntity = isEntity;
    this.templateHash = 0;
    initFileHeader();
  }

  public static ISchemaFile initSchemaFile(String sgName) throws IOException, MetadataException {
    return new SchemaFile(
        sgName, true, IoTDBDescriptor.getInstance().getConfig().getDefaultTTL(), false);
  }

  public static ISchemaFile loadSchemaFile(String sgName) throws IOException, MetadataException {
    return new SchemaFile(sgName, false, -1L, false);
  }

  // region Interface Implementation

  @Override
  public IMNode init() throws MetadataException {
    IMNode resNode;
    String[] sgPathNodes = MetaUtils.splitPathToDetachedPath(storageGroupName);
    if (isEntity) {
      resNode =
          setNodeAddress(
              new StorageGroupEntityMNode(null, sgPathNodes[sgPathNodes.length - 1], dataTTL), 0L);
    } else {
      resNode =
          setNodeAddress(
              new StorageGroupMNode(null, sgPathNodes[sgPathNodes.length - 1], dataTTL), 0L);
    }
    if (templateHash != 0) {
      resNode.setSchemaTemplate(TemplateManager.getInstance().getTemplateFromHash(templateHash));
    }
    return resNode;
  }

  @Override
  public boolean updateStorageGroupNode(IMNode sgNode) throws IOException {
    this.dataTTL = sgNode.getAsStorageGroupMNode().getDataTTL();
    this.isEntity = sgNode.isEntity();
    this.templateHash =
        sgNode.getSchemaTemplate() == null ? 0 : sgNode.getSchemaTemplate().hashCode();
    updateHeader();
    return true;
  }

  @Override
  public void writeMNode(IMNode node) throws MetadataException, IOException {
    int pageIndex;
    short curSegIdx;
    ISchemaPage curPage = null;

    // Get corresponding page instance, segment id
    long curSegAddr = getNodeAddress(node);

    if (node.isStorageGroup()) {
      // Notice that it implies StorageGroupNode is always of 0L segmentAddress
      pageIndex = 0;
      curSegIdx = 0;
      isEntity = node.isEntity();
      setNodeAddress(node, 0L);
    } else {
      if (curSegAddr < 0) {
        throw new MetadataException(
            "Cannot store a node without segment address except for StorageGroupNode.");
      }
      pageIndex = SchemaFile.getPageIndex(curSegAddr);
      curSegIdx = SchemaFile.getSegIndex(curSegAddr);
    }

    // Flush new child
    for (Map.Entry<String, IMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().entrySet()) {

      // Translate node.child into buffer and Pre-Allocate segment for internal child.
      IMNode child = entry.getValue();
      if (!child.isMeasurement()) {
        if (getNodeAddress(child) < 0) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize);
          setNodeAddress(child, glbIndex);
        } else {
          // new child with a valid segment address, weird
          throw new MetadataException("A child in newChildBuffer shall not have segmentAddress.");
        }
      }

      ByteBuffer childBuffer = RecordUtils.node2Buffer(child);

      // Write and Handle Overflow
      // (inside curPage.write)
      // write to curSeg, return 0 if succeeded, positive if next page, exception if no next and no
      // spare
      // (inside this method)
      // get next segment if existed, retry till succeeded or exception
      // allocate new page if exception, link or transplant by original segment size, write buffer
      // to new segment
      // throw exception if record larger than one page
      try {
        curPage = getPageInstance(pageIndex);
        long npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        while (npAddress > 0) {
          // get next page and retry
          pageIndex = SchemaFile.getPageIndex(npAddress);
          curSegIdx = SchemaFile.getSegIndex(npAddress);

          curPage = getPageInstance(pageIndex);
          npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        }

      } catch (SchemaPageOverflowException e) {
        // there is no more next page, need allocate new page
        short newSegSize = SchemaFile.reEstimateSegSize(curPage.getSegmentSize(curSegIdx));
        ISchemaPage newPage = getMinApplicablePageInMem(newSegSize);

        if (newSegSize == curPage.getSegmentSize(curSegIdx)) {
          // segment on multi pages
          short newSegId = newPage.allocNewSegment(newSegSize);
          long newSegAddr = SchemaFile.getGlobalIndex(newPage.getPageIndex(), newSegId);

          // note that it doesn't modify address of node nor parental record
          newPage.setPrevSegAddress(newSegId, curSegAddr);
          curPage.setNextSegAddress(curSegIdx, newSegAddr);

          curSegAddr = newSegAddr;
        } else {
          // segment on a single new page
          curSegAddr = newPage.transplantSegment(curPage, curSegIdx, newSegSize);

          curPage.deleteSegment(curSegIdx);

          curSegIdx = SchemaFile.getSegIndex(curSegAddr);
          setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
        }

        curPage = newPage;
        curPage.write(curSegIdx, entry.getKey(), childBuffer);
      }
    }

    // Flush updated child
    for (Map.Entry<String, IMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getUpdatedChildBuffer().entrySet()) {
      // Translate child into reocrd
      IMNode child = entry.getValue();
      ByteBuffer childBuffer = RecordUtils.node2Buffer(child);

      // Get segment actually contains the record
      long actualSegAddr = getTargetSegmentAddress(curSegAddr, entry.getKey());

      try {
        curPage = getPageInstance(getPageIndex(actualSegAddr));
        curSegIdx = getSegIndex(actualSegAddr);

        // if current segment has no more space for new record, it will re-allocate segment, if
        // failed, throw exception
        curPage.update(curSegIdx, entry.getKey(), childBuffer);

      } catch (SchemaPageOverflowException e) {
        short curSegSize = curPage.getSegmentSize(curSegIdx);
        long existedSegAddr =
            getApplicableLinkedSegments(curPage, curSegIdx, entry.getKey(), childBuffer);
        if (existedSegAddr >= 0) {
          // get another existed segment
          curPage = getPageInstance(getPageIndex(existedSegAddr));
          curSegIdx = getSegIndex(existedSegAddr);
        } else {
          // no next segment to write
          short newSegSize = reEstimateSegSize(curSegSize);
          ISchemaPage newPage = getMinApplicablePageInMem(newSegSize);
          long newSegAddr = newPage.transplantSegment(curPage, curSegIdx, newSegSize);
          short newSegId = getSegIndex(newSegAddr);

          // and new seg to list for multi-page segment, modify parental record for single segment
          if (curSegSize == SEG_MAX_SIZ) {
            // allocate new page and expand max-segment-lists
            long nextSegAddr = curPage.getNextSegAddress(curSegIdx);

            // link new seg as the next to the cur, prev to the original next if existed
            if (nextSegAddr >= 0) {
              ISchemaPage nextPage = getPageInstance(getPageIndex(nextSegAddr));

              newPage.setNextSegAddress(newSegId, nextSegAddr);
              nextPage.setPrevSegAddress(getSegIndex(newSegAddr), newSegAddr);
            }

            curPage.setNextSegAddress(curSegIdx, newSegAddr);
            newPage.setPrevSegAddress(newSegId, curSegAddr);

          } else {
            // modify parental record and node
            curPage.deleteSegment(curSegIdx);
            setNodeAddress(node, newSegAddr);
            updateParentalRecord(node.getParent(), node.getName(), newSegAddr);
          }

          curPage = newPage;
          curSegAddr = newSegAddr;
          curSegIdx = newSegId;
        }

        if (existedSegAddr >= 0 || curSegSize == SEG_MAX_SIZ) {
          // remove from original segment
          getPageInstance(getPageIndex(actualSegAddr))
              .removeRecord(getSegIndex(actualSegAddr), entry.getKey());
          // flush updated record into another linked segment, or a fresh full-page segment
          curPage.write(curSegIdx, entry.getKey(), childBuffer);
        } else {
          // updated on an extended segment
          curPage.update(curSegIdx, entry.getKey(), childBuffer);
        }
      }
    }
  }

  @Override
  public void delete(IMNode node) throws IOException, MetadataException {
    long recSegAddr = node.getParent() == null ? ROOT_INDEX : getNodeAddress(node.getParent());
    recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName());
    getPageInstance(getPageIndex(recSegAddr)).removeRecord(getSegIndex(recSegAddr), node.getName());

    if (!node.isMeasurement()) {
      long delSegAddr = getNodeAddress(node);
      getPageInstance(getPageIndex(delSegAddr)).deleteSegment(getSegIndex(delSegAddr));
    }
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName)
      throws MetadataException, IOException {
    if (getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format("Node [%s] has no child in schema file.", parent.getFullPath()));
    }

    long actualSegAddr = getTargetSegmentAddress(getNodeAddress(parent), childName);
    if (actualSegAddr < 0) {
      throw new MetadataException(
          String.format("Node [%s] has no child named [%s].", parent.getFullPath(), childName));
    }

    return getPageInstance(getPageIndex(actualSegAddr)).read(getSegIndex(actualSegAddr), childName);
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    if (parent.isMeasurement() || getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format("Node [%s] has no child in schema file.", parent.getFullPath()));
    }

    int pageIdx = getPageIndex(getNodeAddress(parent));
    short segId = getSegIndex(getNodeAddress(parent));
    ISchemaPage page = getPageInstance(pageIdx);

    return new Iterator<IMNode>() {
      long nextSeg = page.getNextSegAddress(segId);
      long prevSeg = page.getPrevSegAddress(segId);
      final Queue<IMNode> children = page.getChildren(segId);

      @Override
      public boolean hasNext() {
        if (children.size() == 0) {
          // actually, 0 can never be nextSeg forever
          if (nextSeg < 0 && prevSeg < 0) {
            return false;
          }
          try {
            if (nextSeg >= 0) {
              ISchemaPage newPage = getPageInstance(getPageIndex(nextSeg));
              children.addAll(newPage.getChildren(getSegIndex(nextSeg)));
              nextSeg = newPage.getNextSegAddress(getSegIndex(nextSeg));
              return true;
            }
            if (prevSeg >= 0) {
              ISchemaPage newPage = getPageInstance(getPageIndex(prevSeg));
              children.addAll(newPage.getChildren(getSegIndex(prevSeg)));
              prevSeg = newPage.getPrevSegAddress(getSegIndex(prevSeg));
              return true;
            }
          } catch (IOException | MetadataException e) {
            return false;
          }
        }
        return true;
      }

      @Override
      public IMNode next() {
        return children.poll();
      }
    };
  }

  @Override
  public void close() throws IOException {
    updateHeader();
    flushPageToFile(rootPage);
    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      flushPageToFile(entry.getValue());
    }
    channel.close();
  }

  @Override
  public void sync() throws IOException {
    updateHeader();
    flushPageToFile(rootPage);
    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      flushPageToFile(entry.getValue());
    }
  }

  @Override
  public void clear() throws IOException, MetadataException {
    pageInstCache.clear();
    channel.close();
    rootPage = null;
    if (pmtFile.exists()) {
      Files.delete(Paths.get(pmtFile.toURI()));
    }
    pmtFile.createNewFile();

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    initFileHeader();
  }

  public String inspect() throws MetadataException, IOException {
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "==================\n"
                    + "==================\n"
                    + "SchemaFile inspect: %s, totalPages:%d\n",
                storageGroupName, lastPageIndex + 1));
    int cnt = 0;
    while (cnt <= lastPageIndex) {
      ISchemaPage page = getPageInstance(cnt);
      builder.append(String.format("----------\n%s\n", page.inspect()));
      cnt++;
    }
    return builder.toString();
  }
  // endregion

  // region File Operations

  /**
   * This method initiate file header buffer, with an empty file if meant to init. <br>
   *
   * <p><b>File Header Structure:</b>
   *
   * <ul>
   *   <li>1 int (4 bytes): last page index
   *   <li>var length: root(SG) node info
   *       <ul>
   *         <li><s>a. var length string (less than 200 bytes): path to root(SG) node</s>
   *         <li>a. 1 long (8 bytes): dataTTL
   *         <li>b. 1 bool (1 byte): isEntityStorageGroup
   *         <li>c. 1 int (4 bytes): hash code of template name
   *         <li>d. fixed length buffer (9 bytes): internal or entity node buffer [not implemented
   *             yet]
   *       </ul>
   * </ul>
   *
   * ... (Expected to extend for optimization) ...
   */
  private void initFileHeader() throws IOException, MetadataException {
    if (channel.size() == 0) {
      // new schema file
      lastPageIndex = 0;
      ReadWriteIOUtils.write(lastPageIndex, headerContent);
      ReadWriteIOUtils.write(dataTTL, headerContent);
      ReadWriteIOUtils.write(isEntity, headerContent);
      ReadWriteIOUtils.write(templateHash, headerContent);
      initRootPage();
    } else {
      channel.read(headerContent);
      headerContent.clear();
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      dataTTL = ReadWriteIOUtils.readLong(headerContent);
      isEntity = ReadWriteIOUtils.readBool(headerContent);
      templateHash = ReadWriteIOUtils.readInt(headerContent);
      rootPage = getPageInstance(0);
    }
  }

  private void updateHeader() throws IOException {
    headerContent.clear();

    ReadWriteIOUtils.write(lastPageIndex, headerContent);
    ReadWriteIOUtils.write(dataTTL, headerContent);
    ReadWriteIOUtils.write(isEntity, headerContent);
    ReadWriteIOUtils.write(templateHash, headerContent);

    headerContent.clear();
    channel.write(headerContent, 0);
    channel.force(true);
  }

  private void initRootPage() throws IOException, MetadataException {
    if (rootPage == null) {
      rootPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SEG_MAX_SIZ);

      lastPageIndex = 0;

      pageInstCache.put(rootPage.getPageIndex(), rootPage);
    }
  }

  private boolean isStorageGroupNode(IMNode node) {
    return node.getFullPath().equals(this.storageGroupName);
  }

  // endregion
  // region Segment Address Operation

  /**
   * It might be a weird but influential method. It searches for the real segment address with
   * address from parental node and child name.
   *
   * <p>For now, it examines segments one by one with target child name, which has Operation(n).
   *
   * @param curSegAddr segment address from parental node
   * @param recKey target child name
   */
  private long getTargetSegmentAddress(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    // TODO: improve efficiency
    short curSegId = getSegIndex(curSegAddr);

    ISchemaPage curPage = getPageInstance(getPageIndex(curSegAddr));

    if (curPage.hasRecordKeyInSegment(recKey, curSegId)) {
      return curSegAddr;
    }

    long nextSegAddr = curPage.getNextSegAddress(curSegId);
    while (nextSegAddr >= 0) {
      ISchemaPage pivotPage = getPageInstance(getPageIndex(nextSegAddr));
      short pSegId = getSegIndex(nextSegAddr);
      if (pivotPage.hasRecordKeyInSegment(recKey, pSegId)) {
        return nextSegAddr;
      }
      nextSegAddr = pivotPage.getNextSegAddress(pSegId);
    }

    long prevSegAddr = curPage.getPrevSegAddress(curSegId);
    while (prevSegAddr >= 0) {
      ISchemaPage pivotPage = getPageInstance(getPageIndex(prevSegAddr));
      short pSegId = getSegIndex(prevSegAddr);
      if (pivotPage.hasRecordKeyInSegment(recKey, pSegId)) {
        return prevSegAddr;
      }
      prevSegAddr = pivotPage.getPrevSegAddress(pSegId);
    }

    return -1;
  }

  /**
   * To find a segment applicable for inserting (key, buffer), among pages linked to curPage.curSeg
   *
   * @param curPage
   * @param curSegId
   * @param key
   * @param buffer
   * @return
   */
  private long getApplicableLinkedSegments(
      ISchemaPage curPage, short curSegId, String key, ByteBuffer buffer)
      throws IOException, MetadataException {
    if (curPage.getSegmentSize(curSegId) < SEG_MAX_SIZ) {
      // only max segment can be linked
      return -1;
    }

    short totalSize = (short) (key.getBytes().length + buffer.capacity() + 4 + 2);
    long nextSegAddr = curPage.getNextSegAddress(curSegId);
    while (nextSegAddr >= 0) {
      ISchemaPage nextPage = getPageInstance(getPageIndex(nextSegAddr));
      if (nextPage.isSegmentCapableFor(getSegIndex(nextSegAddr), totalSize)) {
        return nextSegAddr;
      }
      nextSegAddr = nextPage.getNextSegAddress(getSegIndex(nextSegAddr));
    }

    long prevSegAddr = curPage.getPrevSegAddress(curSegId);
    while (prevSegAddr >= 0) {
      ISchemaPage prevPage = getPageInstance(getPageIndex(prevSegAddr));
      if (prevPage.isSegmentCapableFor(getSegIndex(prevSegAddr), totalSize)) {
        return prevSegAddr;
      }
      prevSegAddr = prevPage.getPrevSegAddress(getSegIndex(prevSegAddr));
    }
    return -1;
  }

  private long preAllocateSegment(short size) throws IOException, MetadataException {
    ISchemaPage page = getMinApplicablePageInMem(size);
    return SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  // endregion

  // region Schema Page Operations

  private ISchemaPage getRootPage() {
    return rootPage;
  }

  /**
   * This method checks with cached page container, LOCK and return a minimum applicable page for
   * allocation.
   *
   * @param size size of segment
   * @return
   */
  private ISchemaPage getMinApplicablePageInMem(short size) throws IOException {
    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      if (entry.getValue().isCapableForSize(size)) {
        return pageInstCache.get(entry.getKey());
      }
    }
    return allocateNewPage();
  }

  /**
   * Get from cache, or load from file.
   *
   * @param pageIdx target page index
   * @return an existed page
   */
  private ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    // TODO: improve concurrent control
    //  since now one page may be evicted after returned but before updated
    if (pageIdx > lastPageIndex) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    if (pageInstCache.containsKey(pageIdx)) {
      if (pageInstCache.containsKey(pageIdx)) {
        return pageInstCache.get(pageIdx);
      }
    }

    ByteBuffer newBuf = ByteBuffer.allocate(PAGE_LENGTH);

    loadFromFile(newBuf, pageIdx);
    addPageToCache(pageIdx, SchemaPage.loadPage(newBuf, pageIdx));
    return pageInstCache.get(pageIdx);
  }

  private ISchemaPage allocateNewPage() throws IOException {
    lastPageIndex += 1;
    ISchemaPage newPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), lastPageIndex);
    addPageToCache(newPage.getPageIndex(), newPage);
    return newPage;
  }

  private void addPageToCache(int pageIndex, ISchemaPage page) throws IOException {
    pageInstCache.put(pageIndex, page);
    if (pageInstCache.size() >= PAGE_CACHE_SIZE) {
      int removeCnt =
          (int) (0.2 * pageInstCache.size()) > 0 ? (int) (0.2 * pageInstCache.size()) : 1;
      List<Integer> rmvIds = new ArrayList<>(pageInstCache.keySet()).subList(0, removeCnt);

      for (Integer id : rmvIds) {
        // TODO: improve concurrent control
        // for any page involved in concurrent operation, it will not be evicted
        // this may produce an inefficient eviction
        flushPageToFile(pageInstCache.get(id));
        pageInstCache.remove(id);
      }
    }
  }

  // endregion

  // region Utilities

  public static long getGlobalIndex(int pageIndex, short segIndex) {
    return ((pageIndex << SchemaFile.SEG_INDEX_DIGIT) | (segIndex & SchemaFile.SEG_INDEX_MASK));
  }

  public static int getPageIndex(long globalIndex) {
    return ((int) globalIndex >>> SchemaFile.SEG_INDEX_DIGIT);
  }

  public static short getSegIndex(long globalIndex) {
    return (short) (globalIndex & SchemaFile.SEG_INDEX_MASK);
  }

  // estimate for segment re-allocation
  static short reEstimateSegSize(int oldSize) {
    for (short size : SEG_SIZE_LST) {
      if (oldSize < size) {
        return size;
      }
    }
    return SEG_MAX_SIZ;
  }

  private void updateParentalRecord(IMNode parent, String key, long newSegAddr)
      throws IOException, MetadataException {
    long parSegAddr = parent.getParent() == null ? ROOT_INDEX : getNodeAddress(parent);
    parSegAddr = getTargetSegmentAddress(parSegAddr, key);
    ISchemaPage page = getPageInstance(getPageIndex(parSegAddr));
    ((SchemaPage) page).updateRecordSegAddr(getSegIndex(parSegAddr), key, newSegAddr);
  }

  /**
   * Estimate segment size for pre-allocate
   *
   * @param node
   * @return
   */
  private static short estimateSegmentSize(IMNode node) {
    int childNum = node.getChildren().size();
    int tier = SchemaFile.SEG_SIZE_LST.length;
    if (childNum > 300) {
      return SchemaFile.SEG_SIZE_LST[tier - 1] > SchemaFile.SEG_MIN_SIZ
          ? SchemaFile.SEG_SIZE_LST[tier - 1]
          : SchemaFile.SEG_MIN_SIZ;
    } else if (childNum > 150) {
      return SchemaFile.SEG_SIZE_LST[tier - 2] > SchemaFile.SEG_MIN_SIZ
          ? SchemaFile.SEG_SIZE_LST[tier - 2]
          : SchemaFile.SEG_MIN_SIZ;
    } else if (childNum > 75) {
      return SchemaFile.SEG_SIZE_LST[tier - 3] > SchemaFile.SEG_MIN_SIZ
          ? SchemaFile.SEG_SIZE_LST[tier - 3]
          : SchemaFile.SEG_MIN_SIZ;
    } else if (childNum > 40) {
      return SchemaFile.SEG_SIZE_LST[tier - 4] > SchemaFile.SEG_MIN_SIZ
          ? SchemaFile.SEG_SIZE_LST[tier - 4]
          : SchemaFile.SEG_MIN_SIZ;
    } else if (childNum > 20) {
      return SchemaFile.SEG_SIZE_LST[tier - 5] > SchemaFile.SEG_MIN_SIZ
          ? SchemaFile.SEG_SIZE_LST[tier - 5]
          : SchemaFile.SEG_MIN_SIZ;
    }
    // for childNum < 20, count for actually
    int totalSize = Segment.SEG_HEADER_SIZE;
    for (IMNode child : node.getChildren().values()) {
      totalSize += child.getName().getBytes().length;
      totalSize += 2 + 4; // for record offset, length of string key
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
    return (short) totalSize > SchemaFile.SEG_MIN_SIZ ? (short) totalSize : SchemaFile.SEG_MIN_SIZ;
  }

  private long getPageAddress(int pageIndex) {
    return pageIndex * PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  public static long getNodeAddress(IMNode node) {
    return ICachedMNodeContainer.getCachedMNodeContainer(node).getSegmentAddress();
  }

  public static IMNode setNodeAddress(IMNode node, long addr) {
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(addr);
    return node;
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    return channel.read(dst, getPageAddress(pageIndex));
  }

  private void flushPageToFile(ISchemaPage src) throws IOException {
    src.syncPageBuffer();

    ByteBuffer srcBuf = ByteBuffer.allocate(SchemaFile.PAGE_LENGTH);
    src.getPageBuffer(srcBuf);
    srcBuf.clear();
    channel.write(srcBuf, getPageAddress(src.getPageIndex()));
  }

  @TestOnly
  public SchemaPage getPageOnTest(int index) throws IOException, MetadataException {
    return (SchemaPage) getPageInstance(index);
  }

  // endregion

  private class PageLocks {

    /**
     * number of reentrant read write lock. Notice that this number should be a prime number for
     * uniform hash
     */
    private static final int NUM_OF_LOCKS = 1039;

    /** locks array */
    private ReentrantReadWriteLock[] locks;

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
