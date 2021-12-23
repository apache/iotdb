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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LevelCompactionRecoverTest {

  File tempSGDir;
  static final String COMPACTION_TEST_SG = "root.compactionTest";

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
              TestConstant.SEQUENCE_DATA_DIR.concat(
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
              TestConstant.UNSEQUENCE_DATA_DIR.concat(
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

  private void removeFiles() throws IOException {
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
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
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

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    IoTDB.metaManager.init();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    tempSGDir = new File(TestConstant.SEQUENCE_DATA_DIR.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    FileUtils.deleteDirectory(tempSGDir);
  }

  /** compaction recover merge finished */
  @Test
  public void testRecoverCompleteTargetFileAndCompactionLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.merge(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true,
        new ArrayList<>(),
        null);
    compactionLogger.close();
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(600, count);
  }

  /** compeleted target file, and not resource files, compaction log exists */
  @Test
  public void testCompactionRecoverWithCompletedTargetFileAndLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.merge(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true,
        new ArrayList<>(),
        null);
    compactionLogger.close();
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      levelCompactionTsFileManagement.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = levelCompactionTsFileManagement.getTsFileList(true);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(600, count);
  }

  /** compeleted target file, and not resource files, compaction log exists */
  @Test
  public void testCompactionRecoverWithCompletedTargetFile()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.merge(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true,
        new ArrayList<>(),
        null);
    compactionLogger.close();
    File logFile =
        SystemFileFactory.INSTANCE.getFile(
            tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME);
    long totalWaitingTime = 0;
    while (logFile.exists()) {
      logFile.delete();
      System.gc();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {

      }
      totalWaitingTime += 100;
      if (totalWaitingTime > 10_000) {
        System.out.println("failed to delete " + logFile);
        fail();
      }
    }
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      levelCompactionTsFileManagement.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(600, count);
  }

  /** compaction recover merge finished,unseq */
  @Test
  public void testCompactionMergeRecoverMergeFinishedUnseq()
      throws IOException, IllegalPathException {
    int prevUnseqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(2);

    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(seqResources, false);
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(false);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.merge(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        false,
        new ArrayList<>(),
        null);
    compactionLogger.close();
    levelCompactionTsFileManagement.add(targetTsFileResource, false);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            levelCompactionTsFileManagement.getTsFileList(false),
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(600, count);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnseqLevelNum);
  }

  // log exists, target file not exists
  /** compaction recover merge start just log source file */
  @Test
  public void testCompactionMergeRecoverMergeStartSourceLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.close();
    levelCompactionTsFileManagement.recover();
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);
  }

  /** compaction recover merge start just log source file and sequence flag */
  @Test
  public void testCompactionMergeRecoverMergeStartSequenceLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    compactionLogger.close();
    levelCompactionTsFileManagement.recover();
    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            levelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    assertEquals(600, count);
  }

  /** compaction recover merge start target file logged */
  @Test
  public void testCompactionMergeRecoverMergeStart() throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    compactionLogger.close();
    levelCompactionTsFileManagement.recover();
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);
  }

  /** compaction recover merge finished but no finish log */
  @Test
  public void testCompactionMergeRecoverMergeFinishedNoLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, "0", tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.SEQUENCE_DATA_DIR.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile")));
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.SEQUENCE_DATA_DIR.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.merge(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true,
        new ArrayList<>(),
        null);
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    compactionLogger.close();
    levelCompactionTsFileManagement.recover();
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
            levelCompactionTsFileManagement.getTsFileList(true),
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
    assertEquals(600, count);
  }

  public void deleteFileIfExists(File file) {
    long waitingTime = 0l;
    while (file.exists()) {
      file.delete();
      System.gc();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {

      }
      waitingTime += 100;
      if (waitingTime > 20_000) {
        System.out.println("fail to delete " + file);
        break;
      }
    }
  }
}
