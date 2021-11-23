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

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class LevelCompactionRestoreTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static String dataDir =
      TestConstant.BASE_OUTPUT_PATH
          + "data".concat(File.separator)
          + "sequence".concat(File.separator)
          + COMPACTION_TEST_SG.concat(File.separator)
          + "0".concat(File.separator)
          + "0";
  LevelCompactionTsFileManagementForTest tsFileManagementForTest;

  protected int seqFileNum = 6;
  int unseqFileNum = 0;
  protected int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private int prevMergeChunkThreshold;

  @Before
  public void setUp() throws IOException, MetadataException, WriteProcessException {
    File dataDirectory = new File(dataDir);
    if (!dataDirectory.exists()) {
      Assert.assertTrue(dataDirectory.mkdirs());
    }
    IoTDB.metaManager.init();
    IoTDB.metaManager.clear();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    tsFileManagementForTest =
        new LevelCompactionTsFileManagementForTest(
            "root.compactionTest", "0", TestConstant.BASE_OUTPUT_PATH);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    File dataDirectory = new File(dataDir);
    if (dataDirectory.exists()) {
      FileUtils.deleteDirectory(dataDirectory);
    }
    IoTDB.metaManager.clear();
    File tmpDataDir = new File(TestConstant.BASE_OUTPUT_PATH, "tmp");
    if (tmpDataDir.exists()) {
      FileUtils.deleteDirectory(tmpDataDir);
    }
    tsFileManagementForTest = null;
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = COMPACTION_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.metaManager.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              dataDir
                  + File.separator.concat(
                      i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes((long) i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              dataDir
                  + File.separator.concat(
                      (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  @Test
  public void test1() throws Exception {
    tsFileManagementForTest.addAll(seqResources, true);
    tsFileManagementForTest.addAll(unseqResources, false);
    tsFileManagementForTest.shouldThrowExceptionInDLFID = true;
    tsFileManagementForTest.forkCurrentFileList(0);
    TsFileManagement.CompactionMergeTask compactionMergeTask =
        tsFileManagementForTest.new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeTask.call();
    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            tsFileManagementForTest.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    Assert.assertEquals(count, 600);
    // only target file exists
    Assert.assertEquals(tsFileManagementForTest.getTsFileList(true).size(), 1);
    File logFile =
        new File(TestConstant.BASE_OUTPUT_PATH, COMPACTION_TEST_SG.concat(".compaction.log"));
    // compaction log should be deleted
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void test2() throws Exception {
    tsFileManagementForTest.addAll(seqResources, true);
    tsFileManagementForTest.addAll(unseqResources, false);
    tsFileManagementForTest.shouldThrowExceptionInDLFIL = true;
    tsFileManagementForTest.forkCurrentFileList(0);
    TsFileManagement.CompactionMergeTask compactionMergeTask =
        tsFileManagementForTest.new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeTask.call();
    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            tsFileManagementForTest.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    Assert.assertEquals(count, 600);
    // only target file exists
    Assert.assertEquals(tsFileManagementForTest.getTsFileList(true).size(), 1);
    File logFile =
        new File(TestConstant.BASE_OUTPUT_PATH, COMPACTION_TEST_SG.concat(".compaction.log"));
    // compaction log should be deleted
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void test3() throws Exception {
    tsFileManagementForTest.addAll(seqResources, true);
    tsFileManagementForTest.addAll(unseqResources, false);
    tsFileManagementForTest.shouldThrowExceptionInCheck = true;
    tsFileManagementForTest.forkCurrentFileList(0);
    TsFileManagement.CompactionMergeTask compactionMergeTask =
        tsFileManagementForTest.new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeTask.call();
    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            tsFileManagementForTest.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    Assert.assertEquals(count, 600);
    // only source files exists
    Assert.assertEquals(tsFileManagementForTest.getTsFileList(true).size(), 6);
    File logFile =
        new File(TestConstant.BASE_OUTPUT_PATH, COMPACTION_TEST_SG.concat(".compaction.log"));
    // compaction log should be deleted
    Assert.assertFalse(logFile.exists());
  }

  private void closeCompactionMergeCallBack(
      boolean isMergeExecutedInCurrentTask, long timePartitionId) {}

  private static class LevelCompactionTsFileManagementForTest
      extends LevelCompactionTsFileManagement {

    public boolean shouldThrowExceptionInDLFIL = false;
    public boolean shouldThrowExceptionInDLFID = false;
    public boolean shouldThrowExceptionInCheck = false;

    public LevelCompactionTsFileManagementForTest(
        String storageGroupName, String virtualStorageGroupId, String storageGroupDir) {
      super(storageGroupName, virtualStorageGroupId, storageGroupDir);
    }

    @Override
    protected void deleteLevelFilesInList(
        long timePartitionId,
        Collection<TsFileResource> mergeTsFiles,
        int level,
        boolean sequence) {
      if (shouldThrowExceptionInDLFIL) {
        throw new RuntimeException("test");
      }
    }

    @Override
    protected void deleteLevelFile(TsFileResource seqFile) {
      seqFile.writeLock();
      try {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
        FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
        seqFile.setDeleted(true);
        seqFile.delete();
      } catch (IOException ignored) {
      } finally {
        seqFile.writeUnlock();
      }
    }

    @Override
    protected void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
      int count = 0;
      int size = mergeTsFiles.size();
      for (TsFileResource mergeFile : mergeTsFiles) {
        deleteLevelFile(mergeFile);
        count++;
        if (shouldThrowExceptionInDLFID && count > size / 2) {
          throw new RuntimeException("test");
        }
      }
    }

    @Override
    protected boolean checkAndSetFilesMergingIfNotSet(
        Collection<TsFileResource> seqFiles, Collection<TsFileResource> unseqFiles) {
      if (shouldThrowExceptionInCheck) {
        throw new RuntimeException("test");
      }
      return true;
    }
  }
}
