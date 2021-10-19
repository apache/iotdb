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
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.InnerCompactionTest;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
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
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_NAME;
import static org.junit.Assert.assertEquals;

public class SizeTieredCompactionRecoverTest extends InnerCompactionTest {
  private static final Logger logger =
      LoggerFactory.getLogger(SizeTieredCompactionRecoverTest.class);

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
    tempSGDir =
        new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0).concat("tempSG"));
    tempSGDir.mkdirs();
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  /** Target file uncompleted, source files and log exists */
  @Test
  public void testCompactionRecoverWithUncompletedTargetFileAndLog() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    PartialPath path =
        new PartialPath(
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
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
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
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
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
        new PartialPath(
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
    PartialPath path =
        new PartialPath(
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
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
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
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
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
        new PartialPath(
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

  @Test
  public void testCompactionRecoverWithCompletedTargetFileAndLog() throws Exception {
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    PartialPath path =
        new PartialPath(
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
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
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
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
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
        new PartialPath(
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
    PartialPath path =
        new PartialPath(
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
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
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
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    InnerSpaceCompactionUtils.compact(
        targetTsFileResource,
        new ArrayList<>(seqResources.subList(0, 3)),
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
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
        new PartialPath(
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
      throws IOException, IllegalPathException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.close();
    PartialPath path =
        new PartialPath(
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
      throws IOException, IllegalPathException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.logSequence(true);
    sizeTieredCompactionLogger.close();
    PartialPath path =
        new PartialPath(
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
  public void testCompactionMergeRecoverMergeStart() throws IOException, IllegalPathException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionLogger sizeTieredCompactionLogger =
        new SizeTieredCompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    sizeTieredCompactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    sizeTieredCompactionLogger.logSequence(true);
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
                        0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 1
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile")));
    sizeTieredCompactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    tsFileManager.addForRecover(targetTsFileResource, true);
    sizeTieredCompactionLogger.close();
    PartialPath path =
        new PartialPath(
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
