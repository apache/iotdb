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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaFileNotExists;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.template.TemplateManager;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This class is mainly aimed to manage space all over the file.
 *
 * <p>This class is meant to open a .pmt(Persistent MTree) file, and maintains the header of the
 * file. It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside
 * a file. Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 */
public class SchemaFile implements ISchemaFile {

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  public static int FILE_HEADER_SIZE = 256; // size of file header in bytes
  public static int ROOT_INDEX = 0; // index of header page

  // folder to store .pmt files
  public static String SCHEMA_FOLDER = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();

  // debug only config
  public static boolean DETAIL_SKETCH = true; // whether sketch leaf segment in detail
  public static int INTERNAL_SPLIT_VALVE = 0; // internal segment split at this spare

  // attributes for this schema file
  private String filePath;
  private String storageGroupName;
  private long dataTTL;
  private boolean isEntity;
  private int templateHash;

  private ByteBuffer headerContent;
  private int lastPageIndex; // last page index of the file, boundary to grow
  private long lastSGAddr; // last segment of storage group node

  // work as a naive (read-only) cache for page instance
  private final Map<Integer, SchemaPage> pageInstCache;
  private final ReentrantLock evictLock;
  private final PageLocks pageLocks;
  private SchemaPage rootPage;

  private final Map<Integer, SchemaPage> dirtyPages;

  private ThreadLocal<int[]>
      treeTrace; // a trace of b+tree index since no upward pointer within segments

  // attributes for file
  private File pmtFile;
  private FileChannel channel;

  private SchemaFile(
      String sgName, int schemaRegionId, boolean override, long ttl, boolean isEntity)
      throws IOException, MetadataException {
    treeTrace = new ThreadLocal<>(); // suppose no more than 15 level b+ tree
    treeTrace.set(new int[16]);

    this.storageGroupName = sgName;
    filePath =
        SchemaFile.SCHEMA_FOLDER
            + File.separator
            + sgName
            + File.separator
            + schemaRegionId
            + File.separator
            + MetadataConstant.SCHEMA_FILE_NAME;

    pmtFile = SystemFileFactory.INSTANCE.getFile(filePath);
    if (!pmtFile.exists() && !override) {
      throw new SchemaFileNotExists(filePath);
    }

    if (pmtFile.exists() && override) {
      logger.warn(
          String.format("Schema File [%s] will be overwritten since already exists.", filePath));
      Files.delete(Paths.get(pmtFile.toURI()));
      pmtFile.createNewFile();
    }

    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File folder =
          SystemFileFactory.INSTANCE.getFile(
              SchemaFile.SCHEMA_FOLDER + File.separator + sgName + File.separator + schemaRegionId);
      folder.mkdirs();
      pmtFile.createNewFile();
    }

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaPage.PAGE_CACHE_SIZE, 1, true));
    dirtyPages = new ConcurrentHashMap<>();
    evictLock = new ReentrantLock();
    pageLocks = new PageLocks();
    // will be overwritten if to init
    this.dataTTL = ttl;
    this.isEntity = isEntity;
    this.templateHash = 0;
    initFileHeader();
  }

  private SchemaFile(File file) throws IOException, MetadataException {
    // only be called to sketch a schema file so an arbitrary file object is necessary
    channel = new RandomAccessFile(file, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaPage.PAGE_CACHE_SIZE, 1, true));
    dirtyPages = new ConcurrentHashMap<>();
    evictLock = new ReentrantLock();
    pageLocks = new PageLocks();

    if (channel.size() <= 0) {
      channel.close();
      throw new SchemaFileNotExists(file.getAbsolutePath());
    }

    initFileHeader();
  }

  public static ISchemaFile initSchemaFile(String sgName, int schemaRegionId)
      throws IOException, MetadataException {
    return new SchemaFile(
        sgName,
        schemaRegionId,
        true,
        CommonDescriptor.getInstance().getConfig().getDefaultTTL(),
        false);
  }

  public static ISchemaFile loadSchemaFile(String sgName, int schemaRegionId)
      throws IOException, MetadataException {
    return new SchemaFile(sgName, schemaRegionId, false, -1L, false);
  }

  public static ISchemaFile loadSchemaFile(File file) throws IOException, MetadataException {
    // only be called to sketch a Schema File
    return new SchemaFile(file);
  }

  // region Interface Implementation

  @Override
  public IMNode init() throws MetadataException {
    IMNode resNode;
    String[] sgPathNodes = PathUtils.splitPathToDetachedNodes(storageGroupName);
    if (isEntity) {
      resNode =
          setNodeAddress(
              new StorageGroupEntityMNode(null, sgPathNodes[sgPathNodes.length - 1], dataTTL), 0L);
    } else {
      resNode =
          setNodeAddress(
              new StorageGroupMNode(null, sgPathNodes[sgPathNodes.length - 1], dataTTL), 0L);
    }
    resNode.setFullPath(storageGroupName);
    if (templateHash != 0) {
      resNode.setSchemaTemplate(TemplateManager.getInstance().getTemplateFromHash(templateHash));
    }
    return resNode;
  }

  @Override
  public boolean updateStorageGroupNode(IStorageGroupMNode sgNode) throws IOException {
    this.dataTTL = sgNode.getDataTTL();
    this.isEntity = sgNode.isEntity();
    this.templateHash =
        sgNode.getSchemaTemplate() == null ? 0 : sgNode.getSchemaTemplate().hashCode();
    updateHeader();
    return true;
  }

  @Override
  public void writeMNode(IMNode node) throws MetadataException, IOException {
    SchemaPage curPage;

    // first segment of the node
    long curSegAddr = getNodeAddress(node);

    if (node.isStorageGroup()) {
      curSegAddr = lastSGAddr;
      isEntity = node.isEntity();
      setNodeAddress(node, lastSGAddr);
    } else {
      if (curSegAddr < 0L) {
        // now only 32 bits page index is allowed
        throw new MetadataException(
            String.format(
                "Cannot store a node with segment address [%s] except for StorageGroupNode.",
                curSegAddr));
      }
    }

    long actualAddress; // actual segment to write record
    IMNode child;
    ByteBuffer childBuffer;
    String sk; // search key when split
    // TODO: reserve order of insert in container may be better
    for (Map.Entry<String, IMNode> entry :
        ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().entrySet().stream()
            .sorted(Map.Entry.<String, IMNode>comparingByKey())
            .collect(Collectors.toList())) {
      // check and pre-allocate
      child = entry.getValue();
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

      // prepare buffer to write
      childBuffer = RecordUtils.node2Buffer(child);
      actualAddress = getTargetSegmentAddress(curSegAddr, entry.getKey());
      curPage = getPageInstance(getPageIndex(actualAddress));
      try {
        curPage.getAsSegmentedPage().write(getSegIndex(actualAddress), entry.getKey(), childBuffer);
        dirtyPages.put(curPage.getPageIndex(), curPage);
      } catch (SchemaPageOverflowException e) {
        if (curPage.getAsSegmentedPage().getSegmentSize(getSegIndex(actualAddress))
            == SchemaPage.SEG_MAX_SIZ) {
          // cur is a leaf, split and set next ptr as link between
          SchemaPage newPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
          newPage.getAsSegmentedPage().allocNewSegment(SchemaPage.SEG_MAX_SIZ);
          sk =
              curPage
                  .getAsSegmentedPage()
                  .splitWrappedSegment(
                      entry.getKey(), childBuffer, newPage, SchemaPage.INCLINED_SPLIT);
          curPage
              .getAsSegmentedPage()
              .setNextSegAddress((short) 0, getGlobalIndex(newPage.getPageIndex(), (short) 0));
          if (treeTrace.get()[0] < 1) {
            // first leaf to split and transplant, so that curSegAddr stay unchanged
            SchemaPage trsPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
            trsPage
                .getAsSegmentedPage()
                .transplantSegment(curPage.getAsSegmentedPage(), (short) 0, SchemaPage.SEG_MAX_SIZ);
            SchemaPage repPage =
                SchemaPage.initInternalPage(
                    ByteBuffer.allocate(SchemaPage.PAGE_LENGTH),
                    curPage.pageIndex,
                    trsPage.getPageIndex());

            repPage.getAsInternalPage().insertRecord(sk, newPage.getPageIndex());
            repPage
                .getAsInternalPage()
                .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));
            replacePageInCache(repPage);

            dirtyPages.put(trsPage.getPageIndex(), trsPage);
          } else {
            insertIndexEntry(treeTrace.get()[0], sk, newPage.getPageIndex());
            dirtyPages.put(curPage.getPageIndex(), curPage);
          }
        } else {
          // transplant and delete former segment
          short actSegId = getSegIndex(actualAddress);
          short newSegSize =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity());
          SchemaPage newPage = getMinApplSegmentedPageInMem(newSegSize);

          // with single segment, curSegAddr equals actualAddress
          curSegAddr =
              newPage
                  .getAsSegmentedPage()
                  .transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSize);
          newPage.getAsSegmentedPage().write(getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);
          setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
          dirtyPages.put(curPage.getPageIndex(), curPage);
        }
      }
    }

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

      try {
        curPage
            .getAsSegmentedPage()
            .update(getSegIndex(actualAddress), entry.getKey(), childBuffer);
        dirtyPages.put(curPage.getPageIndex(), curPage);
      } catch (SchemaPageOverflowException e) {
        if (curPage.getAsSegmentedPage().getSegmentSize(getSegIndex(actualAddress))
            == SchemaPage.SEG_MAX_SIZ) {
          // split and update higer nodes
          SchemaPage splPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
          splPage.getAsSegmentedPage().allocNewSegment(SchemaPage.SEG_MAX_SIZ);
          sk = curPage.getAsSegmentedPage().splitWrappedSegment(null, null, splPage, false);
          curPage
              .getAsSegmentedPage()
              .setNextSegAddress((short) 0, getGlobalIndex(splPage.getPageIndex(), (short) 0));

          // update on page where it exists
          if (entry.getKey().compareTo(sk) >= 0) {
            splPage.getAsSegmentedPage().update((short) 0, entry.getKey(), childBuffer);
          } else {
            curPage.getAsSegmentedPage().update((short) 0, entry.getKey(), childBuffer);
          }

          // insert index entry upward
          if (treeTrace.get()[0] < 1) {
            SchemaPage trsPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
            trsPage
                .getAsSegmentedPage()
                .transplantSegment(curPage.getAsSegmentedPage(), (short) 0, SchemaPage.SEG_MAX_SIZ);
            SchemaPage repPage =
                SchemaPage.initInternalPage(
                    ByteBuffer.allocate(SchemaPage.PAGE_LENGTH),
                    curPage.getPageIndex(),
                    trsPage.getPageIndex());

            repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex());
            repPage
                .getAsInternalPage()
                .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));
            replacePageInCache(repPage);

            dirtyPages.put(trsPage.getPageIndex(), trsPage);
          } else {
            insertIndexEntry(treeTrace.get()[0], sk, splPage.getPageIndex());
            dirtyPages.put(curPage.getPageIndex(), curPage);
          }
        } else {
          // transplant into another bigger segment
          short actSegId = getSegIndex(actualAddress);
          short newSegSiz =
              reEstimateSegSize(
                  curPage.getAsSegmentedPage().getSegmentSize(actSegId) + childBuffer.capacity());
          SchemaPage newPage = getMinApplSegmentedPageInMem(newSegSiz);

          // assign new segment address
          curSegAddr =
              newPage
                  .getAsSegmentedPage()
                  .transplantSegment(curPage.getAsSegmentedPage(), actSegId, newSegSiz);
          curPage.getAsSegmentedPage().deleteSegment(actSegId);

          newPage.getAsSegmentedPage().update(getSegIndex(curSegAddr), entry.getKey(), childBuffer);
          setNodeAddress(node, curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
          dirtyPages.put(curPage.getPageIndex(), curPage);
        }
      }
    }

    flushAllDirtyPages();
  }

  /**
   * Insert an index entry into an internal page. Cascade insert or internal split conducted if
   * necessary.
   *
   * @param key key of the entry
   * @param ptr pointer of the entry
   */
  private synchronized void insertIndexEntry(int treeTraceIndex, String key, int ptr)
      throws MetadataException, IOException {
    SchemaPage iPage = getPageInstance(treeTrace.get()[treeTraceIndex]);
    if (iPage.getAsInternalPage().insertRecord(key, ptr) < INTERNAL_SPLIT_VALVE) {
      if (treeTraceIndex > 1) {
        // overflow, but parent exists
        ByteBuffer dstBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        String sk =
            iPage.getAsInternalPage().splitByKey(key, ptr, dstBuffer, SchemaPage.INCLINED_SPLIT);
        SchemaPage dstPage = SchemaPage.loadSchemaPage(dstBuffer);
        registerAsNewPage(dstPage);
        insertIndexEntry(treeTraceIndex - 1, sk, dstPage.getPageIndex());
      } else {
        // split as root internal
        ByteBuffer splBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        ByteBuffer trsBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        String sk =
            iPage.getAsInternalPage().splitByKey(key, ptr, splBuffer, SchemaPage.INCLINED_SPLIT);
        iPage.getAsInternalPage().extendsTo(trsBuffer);
        SchemaPage splPage = SchemaPage.loadSchemaPage(splBuffer);
        SchemaPage trsPage = SchemaPage.loadSchemaPage(trsBuffer);
        registerAsNewPage(splPage);
        registerAsNewPage(trsPage);

        iPage.getAsInternalPage().resetBuffer(trsPage.getPageIndex());
        if (iPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex()) < 0) {
          throw new MetadataException("Key too large to store in a new InternalSegment: " + sk);
        }
        iPage
            .getAsInternalPage()
            .setNextSegAddress(trsPage.getAsInternalPage().getNextSegAddress());
      }
    }
    dirtyPages.put(iPage.getPageIndex(), iPage);
  }

  @Override
  public void delete(IMNode node) throws IOException, MetadataException {
    long recSegAddr = node.getParent() == null ? ROOT_INDEX : getNodeAddress(node.getParent());
    recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName());
    SchemaPage tarPage = getPageInstance(getPageIndex(recSegAddr));
    dirtyPages.putIfAbsent(tarPage.getPageIndex(), tarPage);
    tarPage.getAsSegmentedPage().removeRecord(getSegIndex(recSegAddr), node.getName());

    if (!node.isMeasurement()) {
      long delSegAddr = getNodeAddress(node);
      tarPage = getPageInstance(getPageIndex(delSegAddr));
      dirtyPages.putIfAbsent(tarPage.getPageIndex(), tarPage);
      tarPage.getAsSegmentedPage().deleteSegment(getSegIndex(delSegAddr));
    }

    flushAllDirtyPages();
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName)
      throws MetadataException, IOException {
    if (getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format(
              "Node [%s] has no valid segment address in schema file.", parent.getFullPath()));
    }

    long actualSegAddr = getTargetSegmentAddress(getNodeAddress(parent), childName);
    if (actualSegAddr < 0) {
      // no target child
      return null;
    }

    return getPageInstance(getPageIndex(actualSegAddr))
        .getAsSegmentedPage()
        .read(getSegIndex(actualSegAddr), childName);
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    if (parent.isMeasurement() || getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format("Node [%s] has no child in schema file.", parent.getFullPath()));
    }

    int pageIdx = getPageIndex(getNodeAddress(parent));
    short segId = getSegIndex(getNodeAddress(parent));
    SchemaPage page = getPageInstance(pageIdx);

    while (page.getAsSegmentedPage() == null) {
      page = getPageInstance(getPageIndex(page.getAsInternalPage().getNextSegAddress()));
    }

    long actualSegAddr = page.getAsSegmentedPage().getNextSegAddress(segId);
    Queue<IMNode> initChildren = page.getAsSegmentedPage().getChildren(segId);
    return new Iterator<IMNode>() {
      long nextSeg = actualSegAddr;
      Queue<IMNode> children = initChildren;

      @Override
      public boolean hasNext() {
        if (children.size() > 0) {
          return true;
        }
        if (nextSeg < 0) {
          return false;
        }

        try {
          SchemaPage nPage = getPageInstance(getPageIndex(nextSeg));
          children = nPage.getAsSegmentedPage().getChildren(getSegIndex(nextSeg));
          nextSeg = nPage.getAsSegmentedPage().getNextSegAddress(getSegIndex(nextSeg));
        } catch (MetadataException | IOException e) {
          return false;
        }

        return children.size() > 0;
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
    for (Map.Entry<Integer, SchemaPage> entry : dirtyPages.entrySet()) {
      flushPageToFile(entry.getValue());
    }
    channel.close();
  }

  @Override
  public void sync() throws IOException {
    updateHeader();
    flushPageToFile(rootPage);
    for (Map.Entry<Integer, SchemaPage> entry : dirtyPages.entrySet()) {
      flushPageToFile(entry.getValue());
    }
  }

  @Override
  public void clear() throws IOException, MetadataException {
    pageInstCache.clear();
    dirtyPages.clear();
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
                "=============================\n"
                    + "== Schema File Sketch Tool ==\n"
                    + "=============================\n"
                    + "== Notice: \n"
                    + "==  Internal/Entity presents as (name, is_aligned, child_segment_address)\n"
                    + "==  Measurement presents as (name, data_type, encoding, compressor, alias_if_exist)\n"
                    + "=============================\n"
                    + "Belong to StorageGroup: [%s], segment of SG:%s, total pages:%d\n",
                storageGroupName == null ? "NOT SPECIFIED" : storageGroupName,
                Long.toHexString(lastSGAddr),
                lastPageIndex + 1));
    int cnt = 0;
    while (cnt <= lastPageIndex) {
      SchemaPage page = getPageInstance(cnt);
      builder.append(String.format("---------------------\n%s\n", page.inspect()));
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
   *   <li>1 int (4 bytes): last page index {@link #lastPageIndex}
   *   <li>var length: root(SG) node info
   *       <ul>
   *         <li><s>a. var length string (less than 200 bytes): path to root(SG) node</s>
   *         <li>a. 1 long (8 bytes): dataTTL {@link #dataTTL}
   *         <li>b. 1 bool (1 byte): isEntityStorageGroup {@link #isEntity}
   *         <li>c. 1 int (4 bytes): hash code of template name {@link #templateHash}
   *         <li>d. 1 long (8 bytes): last segment address of storage group {@link #lastSGAddr}
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
      lastSGAddr = 0L;
      initRootPage();
    } else {
      channel.read(headerContent);
      headerContent.clear();
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      dataTTL = ReadWriteIOUtils.readLong(headerContent);
      isEntity = ReadWriteIOUtils.readBool(headerContent);
      templateHash = ReadWriteIOUtils.readInt(headerContent);
      lastSGAddr = ReadWriteIOUtils.readLong(headerContent);
      rootPage = getPageInstance(0);
    }
  }

  private void updateHeader() throws IOException {
    headerContent.clear();

    ReadWriteIOUtils.write(lastPageIndex, headerContent);
    ReadWriteIOUtils.write(dataTTL, headerContent);
    ReadWriteIOUtils.write(isEntity, headerContent);
    ReadWriteIOUtils.write(templateHash, headerContent);
    ReadWriteIOUtils.write(lastSGAddr, headerContent);

    headerContent.clear();
    channel.write(headerContent, 0);
    channel.force(true);
  }

  private void initRootPage() throws IOException, MetadataException {
    if (rootPage == null) {
      rootPage = SchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaPage.PAGE_LENGTH), 0);
      rootPage.getAsSegmentedPage().allocNewSegment(SchemaPage.SEG_MAX_SIZ);

      lastPageIndex = 0;

      pageInstCache.put(rootPage.getPageIndex(), rootPage);
      dirtyPages.putIfAbsent(rootPage.getPageIndex(), rootPage);
    }
  }

  private boolean isStorageGroupNode(IMNode node) {
    return node.getFullPath().equals(this.storageGroupName);
  }

  // endregion
  // region Segment Address Operation

  private long getTargetSegmentAddress(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    treeTrace.get()[0] = 0;
    SchemaPage curPage = getPageInstance(getPageIndex(curSegAddr));
    if (curPage.getAsSegmentedPage() != null) {
      return curSegAddr;
    }

    int i = 0; // mark the trace of b+ tree node
    while (curPage.getAsInternalPage() != null) {
      i++;
      treeTrace.get()[i] = curPage.getPageIndex();
      curPage = getPageInstance(curPage.getAsInternalPage().getRecordByKey(recKey));
    }
    treeTrace.get()[0] = i; // bound in no.0 elem, points the parent the return

    return getGlobalIndex(curPage.getPageIndex(), (short) 0);
  }

  private long preAllocateSegment(short size) throws IOException, MetadataException {
    SchemaPage page = getMinApplSegmentedPageInMem(size);
    return SchemaFile.getGlobalIndex(
        page.getPageIndex(), page.getAsSegmentedPage().allocNewSegment(size));
  }

  // endregion

  // region Schema Page Operations

  /**
   * This method checks with cached page containers and returns a minimum applicable page for
   * allocation.
   *
   * <p><b>Since it will only be called during write procedure, any {@link SchemaPage} returned will
   * be added to {@link #dirtyPages}.</b>
   *
   * @param size size of segment
   * @return
   */
  private SchemaPage getMinApplSegmentedPageInMem(short size) throws IOException {
    for (Map.Entry<Integer, SchemaPage> entry : dirtyPages.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().isCapableForSize(size)) {
        return dirtyPages.get(entry.getKey());
      }
    }

    for (Map.Entry<Integer, SchemaPage> entry : pageInstCache.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().isCapableForSize(size)) {
        dirtyPages.putIfAbsent(entry.getKey(), entry.getValue());
        return pageInstCache.get(entry.getKey());
      }
    }
    return allocateNewSegmentedPage();
  }

  /**
   * Get from cache, or load from file.<br>
   * When to operate on page cache, need to handle concurrency control with {@link
   * SchemaFile#addPageToCache}
   *
   * @param pageIdx target page index
   * @return an existed page
   */
  private SchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    if (pageIdx == ROOT_INDEX && rootPage != null) {
      return rootPage;
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

      ByteBuffer newBuf = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);

      loadFromFile(newBuf, pageIdx);
      return addPageToCache(pageIdx, SchemaPage.loadSchemaPage(newBuf));
    } finally {
      pageLocks.writeUnlock(pageIdx);
    }
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    return channel.read(dst, getPageAddress(pageIndex));
  }

  private synchronized SchemaPage allocateNewSegmentedPage() {
    lastPageIndex += 1;
    SchemaPage newPage =
        SchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaPage.PAGE_LENGTH), lastPageIndex);
    dirtyPages.put(newPage.getPageIndex(), newPage);
    return addPageToCache(newPage.getPageIndex(), newPage);
  }

  private synchronized SchemaPage registerAsNewPage(SchemaPage page) {
    lastPageIndex += 1;
    page.setPageIndex(lastPageIndex);
    dirtyPages.put(lastPageIndex, page);
    return addPageToCache(lastPageIndex, page);
  }

  // TODO: improve concurrency control
  private synchronized SchemaPage replacePageInCache(SchemaPage page) {
    evictLock.lock();
    try {
      if (page.getPageIndex() == ROOT_INDEX) {
        this.rootPage = page;
      }

      dirtyPages.put(page.getPageIndex(), page);
      return addPageToCache(page.getPageIndex(), page);
    } finally {
      evictLock.unlock();
    }
  }

  private SchemaPage addPageToCache(int pageIndex, SchemaPage page) {
    pageInstCache.put(pageIndex, page);
    // only one thread evicts and flushes pages
    if (evictLock.tryLock()) {
      try {
        if (pageInstCache.size() > SchemaPage.PAGE_CACHE_SIZE) {
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

  // endregion

  // region Utilities

  public static long getGlobalIndex(int pageIndex, short segIndex) {
    return (((SchemaPage.PAGE_INDEX_MASK & pageIndex) << SchemaPage.SEG_INDEX_DIGIT)
        | (segIndex & SchemaPage.SEG_INDEX_MASK));
  }

  public static int getPageIndex(long globalIndex) {
    return (int)
        ((globalIndex & (SchemaPage.PAGE_INDEX_MASK << SchemaPage.SEG_INDEX_DIGIT))
            >> SchemaPage.SEG_INDEX_DIGIT);
  }

  public static short getSegIndex(long globalIndex) {
    return (short) (globalIndex & SchemaPage.SEG_INDEX_MASK);
  }

  private void updateParentalRecord(IMNode parent, String key, long newSegAddr)
      throws IOException, MetadataException {
    if (parent == null || parent.getChild(key).isStorageGroup()) {
      lastSGAddr = newSegAddr;
      updateHeader();
      return;
    }
    long parSegAddr = parent.getParent() == null ? ROOT_INDEX : getNodeAddress(parent);
    parSegAddr = getTargetSegmentAddress(parSegAddr, key);
    SchemaPage page = getPageInstance(getPageIndex(parSegAddr));
    ((SegmentedPage) page).updateRecordSegAddr(getSegIndex(parSegAddr), key, newSegAddr);
    dirtyPages.putIfAbsent(page.getPageIndex(), page);
  }

  static short reEstimateSegSize(int oldSize) {
    for (short size : SchemaPage.SEG_SIZE_LST) {
      if (oldSize < size) {
        return size;
      }
    }
    return SchemaPage.SEG_MAX_SIZ;
  }

  /**
   * Estimate segment size for pre-allocate
   *
   * @param node
   * @return
   */
  static short estimateSegmentSize(IMNode node) {
    int childNum = node.getChildren().size();
    int tier = SchemaPage.SEG_SIZE_LST.length;
    if (childNum > 300) {
      return SchemaPage.SEG_SIZE_LST[tier - 1] > SchemaPage.SEG_MIN_SIZ
          ? SchemaPage.SEG_SIZE_LST[tier - 1]
          : SchemaPage.SEG_MIN_SIZ;
    } else if (childNum > 150) {
      return SchemaPage.SEG_SIZE_LST[tier - 2] > SchemaPage.SEG_MIN_SIZ
          ? SchemaPage.SEG_SIZE_LST[tier - 2]
          : SchemaPage.SEG_MIN_SIZ;
    } else if (childNum > 75) {
      return SchemaPage.SEG_SIZE_LST[tier - 3] > SchemaPage.SEG_MIN_SIZ
          ? SchemaPage.SEG_SIZE_LST[tier - 3]
          : SchemaPage.SEG_MIN_SIZ;
    } else if (childNum > 40) {
      return SchemaPage.SEG_SIZE_LST[tier - 4] > SchemaPage.SEG_MIN_SIZ
          ? SchemaPage.SEG_SIZE_LST[tier - 4]
          : SchemaPage.SEG_MIN_SIZ;
    } else if (childNum > 20) {
      return SchemaPage.SEG_SIZE_LST[tier - 5] > SchemaPage.SEG_MIN_SIZ
          ? SchemaPage.SEG_SIZE_LST[tier - 5]
          : SchemaPage.SEG_MIN_SIZ;
    }
    // for childNum < 20, count for actually
    int totalSize = SchemaPage.SEG_HEADER_SIZE;
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
    return (short) totalSize > SchemaPage.SEG_MIN_SIZ ? (short) totalSize : SchemaPage.SEG_MIN_SIZ;
  }

  public static long getPageAddress(int pageIndex) {
    return (SchemaPage.PAGE_INDEX_MASK & pageIndex) * SchemaPage.PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  public static long getNodeAddress(IMNode node) {
    return ICachedMNodeContainer.getCachedMNodeContainer(node).getSegmentAddress();
  }

  public static IMNode setNodeAddress(IMNode node, long addr) {
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(addr);
    return node;
  }

  private void flushPageToFile(SchemaPage src) throws IOException {
    if (src == null) {
      return;
    }
    src.flushPageToChannel(channel);
  }

  private synchronized void flushAllDirtyPages() throws IOException {
    for (SchemaPage page : dirtyPages.values()) {
      flushPageToFile(page);
    }
    updateHeader();
    dirtyPages.clear();
  }

  @TestOnly
  public SchemaPage getPageOnTest(int index) throws IOException, MetadataException {
    return getPageInstance(index);
  }

  @TestOnly
  public long getTargetSegmentOnTest(long srcSegAddr, String key)
      throws IOException, MetadataException {
    return getTargetSegmentAddress(srcSegAddr, key);
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
