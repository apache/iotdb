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

package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaTestUtils;
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
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_INFO;
import static org.junit.Assert.assertEquals;

public class SizeTieredCompactionRecoverTest {
  private static final Logger logger =
      LoggerFactory.getLogger(SizeTieredCompactionRecoverTest.class);

  File tempSGDir;
  protected static final String COMPACTION_TEST_SG = "root.compactionTest";
  protected TsFileManager tsFileManager;

  protected int seqFileNum = 5;
  protected int unseqFileNum = 0;
  protected int measurementNum = 10;
  protected int deviceNum = 10;
  protected long ptNum = 100;
  protected long flushInterval = 20;
  protected TSEncoding encoding = TSEncoding.PLAIN;

  static String SEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "sequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";
  static String UNSEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "unsequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";

  protected String[] deviceIds;
  protected UnaryMeasurementSchema[] measurementSchemas;

  protected List<TsFileResource> seqResources = new ArrayList<>();
  protected List<TsFileResource> unseqResources = new ArrayList<>();

  private int prevMergeChunkThreshold;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                + "data"
                + File.separator
                + "sequence"
                + File.separator
                + "root.compactionTest"
                + File.separator
                + "0"
                + File.separator
                + "0");
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    if (!new File(SEQ_DIRS).exists()) {
      Assert.assertTrue(new File(SEQ_DIRS).mkdirs());
    }
    if (!new File(UNSEQ_DIRS).exists()) {
      Assert.assertTrue(new File(UNSEQ_DIRS).mkdirs());
    }

    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.init();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new UnaryMeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new UnaryMeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = COMPACTION_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String device : deviceIds) {
      for (UnaryMeasurementSchema measurementSchema : measurementSchemas) {
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
              SEQ_DIRS
                  + File.separator.concat(
                      i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes((long) i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              UNSEQ_DIRS
                  + File.separator.concat(
                      (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file =
        new File(
            UNSEQ_DIRS
                + File.separator.concat(
                    unseqFileNum
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + unseqFileNum
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (UnaryMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
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
    EnvironmentUtils.cleanEnv();
    if (tempSGDir.exists()) {
      FileUtils.deleteDirectory(tempSGDir);
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

  /** Target file uncompleted, source files and log exists */
  @Test
  public void testCompactionRecoverWithUncompletedTargetFileAndLog() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);

    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                SEQ_DIRS
                    + File.separator.concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger =
        new SizeTieredCompactionLogger(compactionLogFile.getPath());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        true);
    compactionLogger.close();

    BufferedReader logReader = new BufferedReader(new FileReader(compactionLogFile));
    List<String> logs = new ArrayList<>();
    String line;
    while ((line = logReader.readLine()) != null) {
      logs.add(line);
    }
    logReader.close();
    BufferedWriter logStream =
        new BufferedWriter(
            new FileWriter(
                SystemFileFactory.INSTANCE.getFile(
                    tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME),
                false));
    for (int i = 0; i < logs.size() - 1; i++) {
      logStream.write(logs.get(i));
      logStream.newLine();
    }
    logStream.close();

    TsFileOutput out =
        FSFactoryProducer.getFileOutputFactory()
            .getTsFileOutput(targetTsFileResource.getTsFile().getPath(), true);
    out.truncate(((long) (targetTsFileResource.getTsFileSize() * 0.9)));
    out.close();

    tsFileManager.addForRecover(targetTsFileResource, true);
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  /** compaction recover merge finished, delete one offset */
  @Test
  public void testRecoverCompleteTargetFileAndCompactionLog() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);

    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                SEQ_DIRS
                    + File.separator.concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger =
        new SizeTieredCompactionLogger(compactionLogFile.getPath());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        true);
    compactionLogger.close();
    tsFileManager.add(targetTsFileResource, true);
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    System.out.println(tsFileManager.getTsFileList(true));
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  @Test
  public void testCompactionRecoverWithCompletedTargetFileAndLog() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);

    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                SEQ_DIRS
                    + File.separator.concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger =
        new SizeTieredCompactionLogger(compactionLogFile.getPath());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        true);
    compactionLogger.close();
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      tsFileManager.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    tsFileManager.add(targetTsFileResource, true);
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  /** compeleted target file, and not resource files, compaction log exists */
  @Test
  public void testCompactionRecoverWithCompletedTargetFile() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);

    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                SEQ_DIRS
                    + File.separator.concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger =
        new SizeTieredCompactionLogger(compactionLogFile.getPath());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    compactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        true);
    compactionLogger.close();
    long totalWaitingTime = 0;
    deleteFileIfExists(compactionLogFile);
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      tsFileManager.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    tsFileManager.add(targetTsFileResource, true);
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    logger.warn("TsFiles in list is {}", tsFileManager.getTsFileList(true));
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  /** compaction recover merge start just log source file */
  @Test
  public void testCompactionMergeRecoverMergeStartSourceLog()
      throws IOException, MetadataException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.close();
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  /** compaction recover merge start just log source file and sequence flag */
  @Test
  public void testCompactionMergeRecoverMergeStartSequenceLog()
      throws IOException, MetadataException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.logSequence(true);
    sizeTieredCompactionLogger.close();
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
  }

  /** compaction recover merge start target file logged */
  @Test
  public void testCompactionMergeRecoverMergeStart() throws IOException, MetadataException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.logSequence(true);
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                SEQ_DIRS
                    + File.separator.concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    sizeTieredCompactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
    tsFileManager.addForRecover(targetTsFileResource, true);
    sizeTieredCompactionLogger.close();
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true),
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
    assertEquals(500, count);
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
