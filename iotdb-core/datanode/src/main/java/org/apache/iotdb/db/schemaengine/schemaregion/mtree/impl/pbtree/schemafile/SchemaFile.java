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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaFileNotExists;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.BTreePageManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.IPageManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.PageManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * This class is mainly aimed to manage space all over the file.
 *
 * <p>This class is meant to open a .pst(Persistent mTree) file, and maintains the header of the
 * file. It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside
 * a file. Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 */
@SuppressWarnings("java:S2095")
public class SchemaFile implements ISchemaFile {

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  // attributes for this pbtree file
  private final String filePath;
  private final String logPath;
  private String storageGroupName;
  // TODO: Useless
  private long dataTTL;
  private boolean isEntity;
  private int sgNodeTemplateIdWithState;

  private ByteBuffer headerContent;
  private int lastPageIndex; // last page index of the file, boundary to grow
  private long lastSGAddr; // last segment of database node

  private IPageManager pageManager;

  // attributes for file
  private File pmtFile;
  private FileChannel channel;

  private final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();

  private static String getDirPath(String sgName, int schemaRegionId) {
    return SchemaFileConfig.SCHEMA_FOLDER
        + File.separator
        + sgName
        + File.separator
        + schemaRegionId;
  }

  // region Constructors

  // todo refactor constructor for schema file in Jan.
  @SuppressWarnings("java:S899")
  private SchemaFile(
      String sgName, int schemaRegionId, boolean override, long ttl, boolean isEntity)
      throws IOException, MetadataException {
    String dirPath = getDirPath(sgName, schemaRegionId);
    this.storageGroupName = sgName;
    this.filePath = dirPath + File.separator + SchemaConstant.PBTREE_FILE_NAME;
    this.logPath = dirPath + File.separator + SchemaConstant.PBTREE_LOG_FILE_NAME;

    pmtFile = SystemFileFactory.INSTANCE.getFile(filePath);
    if (!pmtFile.exists() && !override) {
      throw new SchemaFileNotExists(filePath);
    }

    if (pmtFile.exists() && override) {
      logger.warn("PBTree File [{}] will be overwritten since already exists.", filePath);
      Files.delete(Paths.get(pmtFile.toURI()));
      pmtFile.createNewFile();
    }

    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File dir = SystemFileFactory.INSTANCE.getFile(dirPath);
      dir.mkdirs();
      pmtFile.createNewFile();
    }

    this.channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    this.headerContent = ByteBuffer.allocate(SchemaFileConfig.FILE_HEADER_SIZE);
    // will be overwritten if to init
    this.dataTTL = ttl;
    this.isEntity = isEntity;
    this.sgNodeTemplateIdWithState = -1;
    initFileHeader();
  }

  private SchemaFile(File file) throws IOException, MetadataException {
    // only used to sketch a pbtree file so a file object is necessary while
    //  components of log manipulations are not.
    pmtFile = file;
    filePath = pmtFile.getPath();
    logPath = file.getParent() + File.separator + SchemaConstant.PBTREE_LOG_FILE_NAME;
    channel = new RandomAccessFile(file, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFileConfig.FILE_HEADER_SIZE);

    if (channel.size() <= 0) {
      channel.close();
      throw new SchemaFileNotExists(file.getAbsolutePath());
    }

    initFileHeader();
  }

  // load or init
  public static SchemaFile initSchemaFile(String sgName, int schemaRegionId)
      throws IOException, MetadataException {
    File pmtFile =
        SystemFileFactory.INSTANCE.getFile(
            getDirPath(sgName, schemaRegionId) + File.separator + SchemaConstant.PBTREE_FILE_NAME);
    return new SchemaFile(
        sgName,
        schemaRegionId,
        !pmtFile.exists()
            || IoTDBDescriptor.getInstance()
                .getConfig()
                .getSchemaRegionConsensusProtocolClass()
                .equals(ConsensusFactory.RATIS_CONSENSUS),
        Long.MAX_VALUE,
        false);
  }

  public static SchemaFile loadSchemaFile(String sgName, int schemaRegionId)
      throws IOException, MetadataException {
    return new SchemaFile(sgName, schemaRegionId, false, -1L, false);
  }

  public static SchemaFile loadSchemaFile(File file) throws IOException, MetadataException {
    // only be called to sketch a PBTree File
    return new SchemaFile(file);
  }

  // endregion

  // region Interface Implementation

  @Override
  public ICachedMNode init() throws MetadataException {
    ICachedMNode resNode;
    String[] sgPathNodes =
        storageGroupName == null
            ? new String[] {"noName"}
            : PathUtils.splitPathToDetachedNodes(storageGroupName);
    if (isEntity) {
      resNode =
          setNodeAddress(
              nodeFactory.createDatabaseDeviceMNode(null, sgPathNodes[sgPathNodes.length - 1]), 0L);
      resNode.getAsDeviceMNode().setSchemaTemplateId(sgNodeTemplateIdWithState);
      resNode.getAsDeviceMNode().setUseTemplate(sgNodeTemplateIdWithState > -1);
    } else {
      resNode =
          setNodeAddress(
              nodeFactory
                  .createDatabaseMNode(null, sgPathNodes[sgPathNodes.length - 1])
                  .getAsMNode(),
              0L);
    }
    resNode.setFullPath(storageGroupName);
    return resNode;
  }

  @Override
  public boolean updateDatabaseNode(IDatabaseMNode<ICachedMNode> sgNode) throws IOException {
    this.isEntity = sgNode.isDevice();
    if (sgNode.isDevice()) {
      this.sgNodeTemplateIdWithState = sgNode.getAsDeviceMNode().getSchemaTemplateIdWithState();
    }
    updateHeaderBuffer();
    return true;
  }

  @Override
  public void delete(ICachedMNode node) throws IOException, MetadataException {
    if (node.isDatabase()) {
      // should clear this file
      clear();
    } else {
      pageManager.delete(node);
    }
  }

  @Override
  public void writeMNode(ICachedMNode node) throws MetadataException, IOException {
    long curSegAddr = getNodeAddress(node);

    if (node.isDatabase()) {
      isEntity = node.isDevice();
      setNodeAddress(node, lastSGAddr);
    } else {
      if (curSegAddr < 0L) {
        if (node.isDevice() && node.getAsDeviceMNode().isUseTemplate()) {
          throw new MetadataException(
              String.format(
                  "Adding or updating children of device using template [%s] is NOT allowed.",
                  node.getFullPath()));
        }

        // now only 32 bits page index is allowed
        throw new MetadataException(
            String.format(
                "Cannot flush any node with negative address [%s] except for DatabaseNode.",
                node.getFullPath()));
      }
    }
    pageManager.writeMNode(node);
    updateHeaderBuffer();
  }

  @Override
  public ICachedMNode getChildNode(ICachedMNode parent, String childName)
      throws MetadataException, IOException {
    return pageManager.getChildNode(parent, childName);
  }

  @Override
  public Iterator<ICachedMNode> getChildren(ICachedMNode parent)
      throws MetadataException, IOException {
    if (parent.isMeasurement() || getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format("Node [%s] has no child in pbtree file.", parent.getFullPath()));
    }

    return pageManager.getChildren(parent);
  }

  @Override
  public void close() throws IOException {
    updateHeaderBuffer();
    pageManager.close();
    forceChannel();
    channel.close();
  }

  @Override
  public void sync() throws IOException {
    updateHeaderBuffer();
    forceChannel();
  }

  @Override
  public void clear() throws IOException, MetadataException {
    pageManager.clear();
    pageManager.close();
    channel.close();
    if (pmtFile.exists()) {
      Files.delete(Paths.get(pmtFile.toURI()));
    }
    pmtFile.createNewFile();

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFileConfig.FILE_HEADER_SIZE);
    initFileHeader();
  }

  public String inspect() throws MetadataException, IOException {
    return inspect(null);
  }

  public String inspect(PrintWriter pw) throws MetadataException, IOException {
    String header =
        String.format(
            "=============================\n"
                + "== PBTree File Sketch Tool ==\n"
                + "=============================\n"
                + "== Notice: \n"
                + "==  Internal/Entity presents as (name, is_aligned, child_segment_address)\n"
                + "==  Measurement presents as (name, data_type, encoding, compressor, alias_if_exist)\n"
                + "=============================\n"
                + "Belong to StorageGroup: [%s], segment of SG:%s, total pages:%d\n",
            storageGroupName == null ? "NOT SPECIFIED" : storageGroupName,
            Long.toHexString(lastSGAddr),
            lastPageIndex + 1);
    if (pw == null) {
      pw = new PrintWriter(System.out);
    }
    pw.print(header);
    pageManager.inspect(pw);
    return String.format("SchemaFile[%s] had been inspected.", this.filePath);
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
   *         <li>c. 1 int (4 bytes): hash code of template name {@link #sgNodeTemplateIdWithState}
   *         <li>d. 1 long (8 bytes): last segment address of database {@link #lastSGAddr}
   *         <li>e. 1 int (4 bytes): version of pbtree file {@linkplain
   *             SchemaFileConfig#SCHEMA_FILE_VERSION}
   *       </ul>
   * </ul>
   *
   * ... (Expected to extend for optimization) ...
   */
  private void initFileHeader() throws IOException, MetadataException {
    if (channel.size() == 0) {
      // new pbtree file
      lastPageIndex = 0;
      ReadWriteIOUtils.write(lastPageIndex, headerContent);
      ReadWriteIOUtils.write(dataTTL, headerContent);
      ReadWriteIOUtils.write(isEntity, headerContent);
      ReadWriteIOUtils.write(sgNodeTemplateIdWithState, headerContent);
      ReadWriteIOUtils.write(SchemaFileConfig.SCHEMA_FILE_VERSION, headerContent);
      lastSGAddr = 0L;
      pageManager = new BTreePageManager(channel, pmtFile, -1, logPath);
    } else {
      channel.read(headerContent);
      headerContent.clear();
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      dataTTL = ReadWriteIOUtils.readLong(headerContent);
      isEntity = ReadWriteIOUtils.readBool(headerContent);
      sgNodeTemplateIdWithState = ReadWriteIOUtils.readInt(headerContent);
      lastSGAddr = ReadWriteIOUtils.readLong(headerContent);

      if (ReadWriteIOUtils.readInt(headerContent) != SchemaFileConfig.SCHEMA_FILE_VERSION) {
        channel.close();
        throw new MetadataException("SchemaFile with wrong version, please check or upgrade.");
      }

      pageManager = new BTreePageManager(channel, pmtFile, lastPageIndex, logPath);
    }
  }

  private void updateHeaderBuffer() throws IOException {
    headerContent.clear();

    ReadWriteIOUtils.write(pageManager.getLastPageIndex(), headerContent);
    ReadWriteIOUtils.write(dataTTL, headerContent);
    ReadWriteIOUtils.write(isEntity, headerContent);
    ReadWriteIOUtils.write(sgNodeTemplateIdWithState, headerContent);
    ReadWriteIOUtils.write(lastSGAddr, headerContent);
    ReadWriteIOUtils.write(SchemaFileConfig.SCHEMA_FILE_VERSION, headerContent);

    headerContent.flip();
    channel.write(headerContent, 0);
  }

  private void forceChannel() throws IOException {
    channel.force(true);
  }

  // endregion

  // region Utilities

  public static long getGlobalIndex(int pageIndex, short segIndex) {
    return (((SchemaFileConfig.PAGE_INDEX_MASK & pageIndex) << SchemaFileConfig.SEG_INDEX_DIGIT)
        | (segIndex & SchemaFileConfig.SEG_INDEX_MASK));
  }

  public static int getPageIndex(long globalIndex) {
    return (int)
        ((globalIndex & (SchemaFileConfig.PAGE_INDEX_MASK << SchemaFileConfig.SEG_INDEX_DIGIT))
            >> SchemaFileConfig.SEG_INDEX_DIGIT);
  }

  public static short getSegIndex(long globalIndex) {
    return (short) (globalIndex & SchemaFileConfig.SEG_INDEX_MASK);
  }

  /** TODO: shall merge with PageManager#reEstimateSegSize */
  static short reEstimateSegSize(int oldSize) {
    for (short size : SchemaFileConfig.SEG_SIZE_LST) {
      if (oldSize < size) {
        return size;
      }
    }
    return SchemaFileConfig.SEG_MAX_SIZ;
  }

  public static long getPageAddress(int pageIndex) {
    return (SchemaFileConfig.PAGE_INDEX_MASK & pageIndex) * SchemaFileConfig.PAGE_LENGTH
        + SchemaFileConfig.FILE_HEADER_SIZE;
  }

  public static long getNodeAddress(ICachedMNode node) {
    return ICachedMNodeContainer.getCachedMNodeContainer(node).getSegmentAddress();
  }

  public static ICachedMNode setNodeAddress(ICachedMNode node, long addr) {
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(addr);
    return node;
  }

  @TestOnly
  public ISchemaPage getPageOnTest(int index) throws IOException, MetadataException {
    return ((PageManager) pageManager).getPageInstanceOnTest(index);
  }

  @TestOnly
  public long getTargetSegmentOnTest(long srcSegAddr, String key)
      throws IOException, MetadataException {
    return ((PageManager) pageManager).getTargetSegmentAddressOnTest(srcSegAddr, key);
  }

  public void setMetric(SchemaRegionCachedMetric metric) {
    pageManager.setMetric(metric);
  }

  // endregion

  // region Snapshot

  @Override
  public boolean createSnapshot(File snapshotDir) {
    File schemaFileSnapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.PBTREE_SNAPSHOT);
    try {
      sync();
      if (schemaFileSnapshot.exists() && !schemaFileSnapshot.delete()) {
        logger.error(
            "Failed to delete old snapshot {} while creating pbtree file snapshot.",
            schemaFileSnapshot.getName());
        return false;
      }
      Files.copy(Paths.get(filePath), schemaFileSnapshot.toPath());
      return true;
    } catch (IOException e) {
      logger.error("Failed to create SchemaFile snapshot due to {}", e.getMessage(), e);
      schemaFileSnapshot.delete();
      return false;
    }
  }

  public static SchemaFile loadSnapshot(File snapshotDir, String sgName, int schemaRegionId)
      throws IOException, MetadataException {
    File snapshot = SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.PBTREE_SNAPSHOT);
    if (!snapshot.exists()) {
      throw new SchemaFileNotExists(snapshot.getPath());
    }
    File schemaFile =
        SystemFileFactory.INSTANCE.getFile(
            getDirPath(sgName, schemaRegionId), SchemaConstant.PBTREE_FILE_NAME);
    Files.deleteIfExists(schemaFile.toPath());

    if (!IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      // schemaFileLog disabled with RATIS consensus
      File schemaLogFile =
          SystemFileFactory.INSTANCE.getFile(
              getDirPath(sgName, schemaRegionId), SchemaConstant.PBTREE_LOG_FILE_NAME);
      Files.deleteIfExists(schemaLogFile.toPath());
    }

    Files.copy(snapshot.toPath(), schemaFile.toPath());
    return new SchemaFile(sgName, schemaRegionId, false, Long.MAX_VALUE, false);
  }

  // endregion

}
