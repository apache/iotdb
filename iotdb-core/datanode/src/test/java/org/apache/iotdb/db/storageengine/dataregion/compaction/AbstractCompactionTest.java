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
package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.protocol.rest.StringUtil;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.IBatchDataIterator;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.junit.Assert.fail;

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

  /**
   * @param fileNum the number of file
   * @param deviceNum device number in each file
   * @param measurementNum measurement number in each device of each file
   * @param pointNum data point number of each timeseries in each file
   * @param startTime start time of each timeseries
   * @param startValue start value of each timeseries
   * @param timeInterval time interval of each timeseries between files
   * @param valueInterval value interval of each timeseries between files
   * @param isAlign when it is true, it will create mix tsfile which contains aligned and nonAligned
   *     timeseries
   * @param isSeq
   * @throws IOException
   * @throws WriteProcessException
   * @throws MetadataException
   */
  protected void createFiles(
      int fileNum,
      int deviceNum,
      int measurementNum,
      int pointNum,
      long startTime,
      int startValue,
      int timeInterval,
      int valueInterval,
      boolean isAlign,
      boolean isSeq)
      throws IOException, WriteProcessException, MetadataException {
    for (int i = 0; i < fileNum; i++) {
      fileVersion = isSeq ? seqVersion[fileCount] : unseqVersion[fileCount];
      String fileName =
          timestamp[fileCount++] + FilePathUtils.FILE_NAME_SEPARATOR + fileVersion + "-0-0.tsfile";
      String filePath;
      if (isSeq) {
        filePath = SEQ_DIRS.getPath() + File.separator + fileName;
      } else {
        filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
      }
      File file;
      if (isAlign) {
        file =
            TsFileGeneratorUtils.generateAlignedTsFile(
                filePath,
                deviceNum,
                measurementNum,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                startValue + pointNum * i + valueInterval * i,
                chunkGroupSize,
                pageSize);
      } else {
        file =
            TsFileGeneratorUtils.generateNonAlignedTsFile(
                filePath,
                deviceNum,
                measurementNum,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                startValue + pointNum * i + valueInterval * i,
                chunkGroupSize,
                pageSize);
      }
      addResource(
          file,
          deviceNum,
          startTime + pointNum * i + timeInterval * i,
          startTime + pointNum * i + timeInterval * i + pointNum - 1,
          isAlign,
          isSeq);
    }
    // sleep a few milliseconds to avoid generating files with same timestamps
    try {
      Thread.sleep(10);
    } catch (Exception e) {

    }
  }

  /**
   * @param fileNum the number of file
   * @param deviceIndexes device index in each file
   * @param measurementIndexes measurement index in each device of each file
   * @param pointNum data point number of each timeseries in each file
   * @param startTime start time of each timeseries
   * @param timeInterval time interval of each timeseries between files
   * @param isAlign when it is true, it will create mix tsfile which contains aligned and nonAligned
   *     timeseries
   * @param isSeq
   */
  protected void createFilesWithTextValue(
      int fileNum,
      List<Integer> deviceIndexes,
      List<Integer> measurementIndexes,
      int pointNum,
      int startTime,
      int timeInterval,
      boolean isAlign,
      boolean isSeq)
      throws IOException, WriteProcessException {
    String value = isSeq ? "seqTestValue" : "unseqTestValue";
    for (int i = 0; i < fileNum; i++) {
      fileVersion = isSeq ? seqVersion[fileCount] : unseqVersion[fileCount];
      String fileName =
          timestamp[fileCount++] + FilePathUtils.FILE_NAME_SEPARATOR + fileVersion + "-0-0.tsfile";
      String filePath;
      if (isSeq) {
        filePath = SEQ_DIRS.getPath() + File.separator + fileName;
      } else {
        filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
      }
      File file;
      if (isAlign) {
        file =
            TsFileGeneratorUtils.generateAlignedTsFileWithTextValues(
                filePath,
                deviceIndexes,
                measurementIndexes,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                value,
                chunkGroupSize,
                pageSize);
      } else {
        file =
            TsFileGeneratorUtils.generateNonAlignedTsFileWithTextValues(
                filePath,
                deviceIndexes,
                measurementIndexes,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                value,
                chunkGroupSize,
                pageSize);
      }
      // add resource
      TsFileResource resource = new TsFileResource(file);
      int deviceStartindex = isAlign ? TsFileGeneratorUtils.getAlignDeviceOffset() : 0;
      for (int j = 0; j < deviceIndexes.size(); j++) {
        resource.updateStartTime(
            new PlainDeviceID(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (deviceIndexes.get(j) + deviceStartindex)),
            startTime + pointNum * i + timeInterval * i);
        resource.updateEndTime(
            new PlainDeviceID(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (deviceIndexes.get(j) + deviceStartindex)),
            startTime + pointNum * i + timeInterval * i + pointNum - 1);
      }
      resource.updatePlanIndexes(fileVersion);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      resource.serialize();
      if (isSeq) {
        seqResources.add(resource);
      } else {
        unseqResources.add(resource);
      }
    }
    // sleep a few milliseconds to avoid generating files with same timestamps
    try {
      Thread.sleep(10);
    } catch (Exception e) {

    }
  }

  private void addResource(
      File file, int deviceNum, long startTime, long endTime, boolean isAlign, boolean isSeq)
      throws IOException {
    TsFileResource resource = new TsFileResource(file);
    int deviceStartindex = isAlign ? TsFileGeneratorUtils.getAlignDeviceOffset() : 0;

    for (int i = deviceStartindex; i < deviceStartindex + deviceNum; i++) {
      resource.updateStartTime(
          new PlainDeviceID(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i), startTime);
      resource.updateEndTime(
          new PlainDeviceID(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i), endTime);
    }

    resource.updatePlanIndexes(fileVersion);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.serialize();
    if (isSeq) {
      seqResources.add(resource);
    } else {
      unseqResources.add(resource);
    }
  }

  protected void registerTimeseriesInMManger(int deviceNum, int measurementNum, boolean isAligned)
      throws MetadataException {
    for (int i = 0; i < deviceNum; i++) {
      if (isAligned) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        for (int j = 0; j < measurementNum; j++) {
          measurements.add("s" + j);
          dataTypes.add(dataType);
          encodings.add(TSEncoding.PLAIN);
          compressionTypes.add(CompressionType.UNCOMPRESSED);
        }
      }
    }
  }

  protected void deleteTimeseriesInMManager(List<String> timeseries) throws MetadataException {}

  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
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

  protected void validateSeqFiles(boolean isSeq) {
    TsFileValidationTool.clearMap(true);
    List<File> files = new ArrayList<>();
    for (TsFileResource resource : tsFileManager.getTsFileList(isSeq)) {
      files.add(resource.getTsFile());
    }
    TsFileValidationTool.findUncorrectFiles(files);
    Assert.assertEquals(0, TsFileValidationTool.badFileNum);
  }

  protected Map<PartialPath, List<TimeValuePair>> readSourceFiles(
      List<PartialPath> timeseriesPaths, List<TSDataType> dataTypes) throws IOException {
    Map<PartialPath, List<TimeValuePair>> sourceData = new LinkedHashMap<>();
    for (PartialPath path : timeseriesPaths) {
      List<TimeValuePair> dataList = new ArrayList<>();
      sourceData.put(path, dataList);
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              path,
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              tsFileManager.getTsFileList(true),
              tsFileManager.getTsFileList(false),
              true);
      while (tsBlockReader.hasNextBatch()) {
        TsBlock block = tsBlockReader.nextBatch();
        IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
        while (iterator.hasNext()) {
          dataList.add(
              new TimeValuePair(
                  iterator.currentTime(), ((TsPrimitiveType[]) iterator.currentValue())[0]));
          iterator.next();
        }
      }
    }
    return sourceData;
  }

  protected void validateTargetDatas(
      Map<PartialPath, List<TimeValuePair>> sourceDatas, List<TSDataType> dataTypes)
      throws IOException {
    Map<PartialPath, List<TimeValuePair>> tmpSourceDatas = new HashMap<>();
    for (Map.Entry<PartialPath, List<TimeValuePair>> entry : sourceDatas.entrySet()) {
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              entry.getKey(),
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              tsFileManager.getTsFileList(true),
              tsFileManager.getTsFileList(false),
              true);
      List<TimeValuePair> timeseriesData = entry.getValue();
      tmpSourceDatas.put(entry.getKey(), new ArrayList<>(timeseriesData));
      while (tsBlockReader.hasNextBatch()) {
        TsBlock block = tsBlockReader.nextBatch();
        IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
        while (iterator.hasNext()) {
          TimeValuePair data = timeseriesData.remove(0);
          Assert.assertEquals(data.getTimestamp(), iterator.currentTime());
          Assert.assertEquals(data.getValue(), ((TsPrimitiveType[]) iterator.currentValue())[0]);
          iterator.next();
        }
      }
      if (timeseriesData.size() > 0) {
        // there are still data points left, which are not in the target file. Lost the data after
        // compaction.
        fail();
      }
    }
    sourceDatas.putAll(tmpSourceDatas);
  }

  protected void generateModsFile(
      int deviceNum,
      int measurementNum,
      List<TsFileResource> resources,
      long startTime,
      long endTime)
      throws IllegalPathException, IOException {
    List<String> seriesPaths = new ArrayList<>();
    for (int dIndex = 0; dIndex < deviceNum; dIndex++) {
      for (int mIndex = 0; mIndex < measurementNum; mIndex++) {
        seriesPaths.add(
            COMPACTION_TEST_SG
                + IoTDBConstant.PATH_SEPARATOR
                + "d"
                + dIndex
                + IoTDBConstant.PATH_SEPARATOR
                + "s"
                + mIndex);
      }
    }
    generateModsFile(seriesPaths, resources, startTime, endTime);
  }

  protected void generateModsFile(
      List<String> seriesPaths, List<TsFileResource> resources, long startValue, long endValue)
      throws IllegalPathException, IOException {
    for (TsFileResource resource : resources) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (String path : seriesPaths) {
        deleteMap.put(path, new Pair<>(startValue, endValue));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }
  }

  public void generateModsFile(
      List<PartialPath> seriesPaths, TsFileResource resource, long startValue, long endValue)
      throws IllegalPathException, IOException {
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (PartialPath path : seriesPaths) {
      String fullPath =
          (path instanceof AlignedPath)
              ? path.getFullPath()
                  + PATH_SEPARATOR
                  + ((AlignedPath) path).getMeasurementList().get(0)
              : path.getFullPath();
      deleteMap.put(fullPath, new Pair<>(startValue, endValue));
    }
    CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
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
                      + ((PlainDeviceID) deviceID).toStringID());
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

  protected void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  protected void resetFileName(TsFileResource resource, int version) {
    // rename TsFile
    File file = resource.getTsFile();
    String[] fileInfo = file.getPath().split(FILE_NAME_SEPARATOR);
    fileInfo[1] = String.valueOf(version);
    String newFileName = StringUtil.join(fileInfo, FILE_NAME_SEPARATOR);
    file.renameTo(new File(newFileName));

    resource.setVersion(version);
    resource.setFile(new File(newFileName));

    // rename resource file
    file = new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX);
    file.renameTo(new File(newFileName + TsFileResource.RESOURCE_SUFFIX));

    // rename mods file
    file = new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX);
    file.renameTo(new File(newFileName + ModificationFile.FILE_SUFFIX));
  }
}
