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
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.junit.Assert.fail;

public class AbstractCompactionTest {
  protected int seqFileNum = 5;
  protected int unseqFileNum = 0;
  protected List<TsFileResource> seqResources = new ArrayList<>();
  protected List<TsFileResource> unseqResources = new ArrayList<>();
  private int chunkGroupSize = 0;
  private int pageSize = 0;
  protected static String COMPACTION_TEST_SG = TsFileGeneratorUtils.testStorageGroup;
  private TSDataType dataType;

  private static final long oldTargetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  private static final long oldTargetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
  private static final int oldChunkGroupSize =
      TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
  private static final int oldPagePointMaxNumber =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final int oldMaxCrossCompactionFileNum =
      IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileNum();

  private final int oldMaxDegreeOfIndexNode =
      TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();

  private final long oldLowerTargetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  private final long oldLowerTargetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  private int oldMinCrossCompactionUnseqLevel =
      IoTDBDescriptor.getInstance().getConfig().getMinCrossCompactionUnseqFileLevel();

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

  private int fileVersion = 0;

  protected TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
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
      int startTime,
      int startValue,
      int timeInterval,
      int valueInterval,
      boolean isAlign,
      boolean isSeq)
      throws IOException, WriteProcessException, MetadataException {
    for (int i = 0; i < fileNum; i++) {
      String fileName =
          System.currentTimeMillis()
              + FilePathUtils.FILE_NAME_SEPARATOR
              + fileVersion++
              + "-0-0.tsfile";
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
      String fileName =
          System.currentTimeMillis()
              + FilePathUtils.FILE_NAME_SEPARATOR
              + fileVersion++
              + "-0-0.tsfile";
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
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + (deviceIndexes.get(j) + deviceStartindex),
            startTime + pointNum * i + timeInterval * i);
        resource.updateEndTime(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + (deviceIndexes.get(j) + deviceStartindex),
            startTime + pointNum * i + timeInterval * i + pointNum - 1);
      }
      resource.updatePlanIndexes(fileVersion);
      resource.setStatus(TsFileResourceStatus.CLOSED);
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
      resource.updateStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, startTime);
      resource.updateEndTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, endTime);
    }

    resource.updatePlanIndexes(fileVersion);
    resource.setStatus(TsFileResourceStatus.CLOSED);
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
        .setMaxCrossCompactionCandidateFileNum(oldMaxCrossCompactionFileNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(oldTargetChunkPointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(oldLowerTargetChunkPointNum);
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(oldChunkGroupSize);

    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldPagePointMaxNumber);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(oldMaxDegreeOfIndexNode);
    EnvironmentUtils.cleanAllDir();

    if (SEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(SEQ_DIRS);
    }
    if (UNSEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(UNSEQ_DIRS);
    }
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
              seqResources,
              unseqResources,
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
    for (Map.Entry<PartialPath, List<TimeValuePair>> entry : sourceDatas.entrySet()) {
      IDataBlockReader tsBlockReader =
          new SeriesDataBlockReader(
              entry.getKey(),
              FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                  EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
              tsFileManager.getTsFileList(true),
              Collections.emptyList(),
              true);
      List<TimeValuePair> timeseriesData = entry.getValue();
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
        // there are still data points left, which are not in the target file
        fail();
      }
    }
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
  protected void check(TsFileResource targetResource, List<String> deviceIdList)
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
            String deviceID = chunkGroupHeader.getDeviceID();
            if (!deviceIdList.contains(deviceID)) {
              Assert.fail(
                  "Target file "
                      + targetResource.getTsFile().getPath()
                      + " contains empty chunk group "
                      + deviceID);
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
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + fileVersion
            + "-0-0.tsfile";
    String filePath;
    if (isSeq) {
      filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    } else {
      filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
    }
    TsFileResource resource = new TsFileResource(new File(filePath));
    resource.updatePlanIndexes(fileVersion++);
    resource.setStatus(TsFileResourceStatus.CLOSED);
    return resource;
  }

  protected void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }
}
