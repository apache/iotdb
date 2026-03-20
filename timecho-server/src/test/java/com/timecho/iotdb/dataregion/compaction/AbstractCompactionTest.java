/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.timecho.iotdb.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.utils.EnvironmentUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractCompactionTest {
  protected List<TsFileResource> seqResources = new ArrayList<>();
  protected List<TsFileResource> unseqResources = new ArrayList<>();
  private int chunkGroupSize = 0;
  private int pageSize = 0;
  protected static String COMPACTION_TEST_SG = TsFileGeneratorUtils.testStorageGroup;
  private TSDataType dataType;

  protected int maxDeviceNum = 25;
  protected int maxMeasurementNum = 25;

  private long[] timestamp = {0, 0, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 6, 6, 7, 7, 7};
  private int[] seqVersion = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  // seq version is not in order
  //  private int[] seqVersion = {18, 19, 0, 1, 14, 15, 16, 2, 3, 4, 12, 13, 5, 6, 9, 10, 11, 7, 8,
  // 17};
  private int[] unseqVersion = {
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
  };

  private static final long oldTargetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  private static final long oldTargetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
  private static final int oldChunkGroupSize =
      TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
  private static final int oldPagePointMaxNumber =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final int oldMaxCrossCompactionFileNum =
      IoTDBDescriptor.getInstance().getConfig().getFileLimitPerCrossTask();

  private final int oldMaxDegreeOfIndexNode =
      TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();

  private final long oldLowerTargetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  private final long oldLowerTargetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  private final int oldMinCrossCompactionUnseqLevel =
      IoTDBDescriptor.getInstance().getConfig().getMinCrossCompactionUnseqFileLevel();

  private final long oldModsFileSize =
      IoTDBDescriptor.getInstance().getConfig().getInnerCompactionTaskSelectionModsFileThreshold();

  private final long oldLongestExpiredTime =
      IoTDBDescriptor.getInstance().getConfig().getMaxExpiredTime();

  protected static File STORAGE_GROUP_DIR =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "sequence"
              + File.separator
              + COMPACTION_TEST_SG);
  protected static File SEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "sequence"
              + File.separator
              + COMPACTION_TEST_SG
              + File.separator
              + "0"
              + File.separator
              + "0");
  protected static File UNSEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "unsequence"
              + File.separator
              + COMPACTION_TEST_SG
              + File.separator
              + "0"
              + File.separator
              + "0");

  protected Map<Long, Pair<File, File>> registeredTimePartitionDirs = new HashMap<>();

  private int fileVersion = 0;

  private int fileCount = 0;

  protected TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    fileCount = 0;
    if (!SEQ_DIRS.exists()) {
      Assert.assertTrue(SEQ_DIRS.mkdirs());
    }
    if (!UNSEQ_DIRS.exists()) {
      Assert.assertTrue(UNSEQ_DIRS.mkdirs());
    }
    dataType = TSDataType.INT64;
    CompactionTaskManager.getInstance().restart();
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    tsFileManager.getOrCreateSequenceListByTimePartition(0);
    tsFileManager.getOrCreateUnsequenceListByTimePartition(0);
    registeredTimePartitionDirs.put(0L, new Pair<>(SEQ_DIRS, UNSEQ_DIRS));
  }

  protected void createTimePartitionDirIfNotExist(long timePartition) {
    if (registeredTimePartitionDirs.containsKey(timePartition)) {
      return;
    }
    File seqTimePartitionDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                + "data"
                + File.separator
                + "sequence"
                + File.separator
                + COMPACTION_TEST_SG
                + File.separator
                + "0"
                + File.separator
                + timePartition);
    seqTimePartitionDir.mkdirs();
    File unseqTimePartitionDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                + "data"
                + File.separator
                + "unsequence"
                + File.separator
                + COMPACTION_TEST_SG
                + File.separator
                + "0"
                + File.separator
                + timePartition);
    unseqTimePartitionDir.mkdirs();
    registeredTimePartitionDirs.put(
        timePartition, new Pair<File, File>(seqTimePartitionDir, unseqTimePartitionDir));
  }

  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    CompactionTaskManager.getInstance().stop();
    seqResources.clear();
    unseqResources.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMinCrossCompactionUnseqFileLevel(oldMinCrossCompactionUnseqLevel);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(oldTargetChunkSize);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(oldTargetChunkPointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setFileLimitPerCrossTask(oldMaxCrossCompactionFileNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(oldTargetChunkPointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(oldLowerTargetChunkPointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTaskSelectionModsFileThreshold(oldModsFileSize);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(oldLongestExpiredTime);
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(oldChunkGroupSize);

    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldPagePointMaxNumber);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(oldMaxDegreeOfIndexNode);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    WALManager.getInstance().clear();

    EnvironmentUtils.cleanAllDir();

    if (SEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(SEQ_DIRS);
    }
    if (UNSEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(UNSEQ_DIRS);
    }
    for (Map.Entry<Long, Pair<File, File>> entry : registeredTimePartitionDirs.entrySet()) {
      File seqDir = entry.getValue().left;
      File unseqDir = entry.getValue().right;
      if (seqDir.exists() && seqDir.isDirectory()) {
        FileUtils.deleteDirectory(seqDir);
      }
      if (unseqDir.exists() && unseqDir.isDirectory()) {
        FileUtils.deleteDirectory(unseqDir);
      }
    }
    registeredTimePartitionDirs.clear();
    tsFileManager.clear();
    TsFileResourceManager.getInstance().clear();
  }

  private void removeFiles() throws IOException {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unseqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
  }

  /**
   * Check whether target file contain empty chunk group or not. Assert fail if it contains empty
   * chunk group whose deviceID is not in the deviceIdList.
   */
  protected void check(TsFileResource targetResource, List<IDeviceID> deviceIdList)
      throws IOException {
    byte marker;
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource.getTsFile().getAbsolutePath())) {
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            int dataSize = header.getDataSize();
            reader.position(reader.position() + dataSize);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            IDeviceID deviceID = chunkGroupHeader.getDeviceID();
            if (!deviceIdList.contains(deviceID)) {
              Assert.fail(
                  "Target file "
                      + targetResource.getTsFile().getPath()
                      + " contains empty chunk group "
                      + deviceID.toString());
            }
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
    }
  }

  protected TsFileResource createEmptyFileAndResource(boolean isSeq) {
    return createEmptyFileAndResource(isSeq, 0);
  }

  protected TsFileResource createEmptyFileAndResource(boolean isSeq, int innerCompactionCnt) {
    fileVersion = isSeq ? seqVersion[fileCount] : unseqVersion[fileCount];
    String fileName =
        timestamp[fileCount++]
            + FilePathUtils.FILE_NAME_SEPARATOR
            + fileVersion
            + String.format("-%d-0.tsfile", innerCompactionCnt);
    String filePath;
    if (isSeq) {
      filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    } else {
      filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
    }
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.updatePlanIndexes(fileVersion);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    return resource;
  }

  protected TsFileResource createEmptyFileAndResourceWithName(
      String fileName, boolean isSeq, int innerCompactionCnt) {
    String filePath;
    if (isSeq) {
      filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    } else {
      filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
    }
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    return resource;
  }

  protected TsFileResource createEmptyFileAndResourceWithName(
      String fileName, long timePartition, boolean isSeq) {
    if (!registeredTimePartitionDirs.containsKey(timePartition)) {
      createTimePartitionDirIfNotExist(timePartition);
    }
    String filePath;
    if (isSeq) {
      filePath =
          registeredTimePartitionDirs.get(timePartition).left.getPath() + File.separator + fileName;
    } else {
      filePath =
          registeredTimePartitionDirs.get(timePartition).right.getPath()
              + File.separator
              + fileName;
    }
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    return resource;
  }
}
