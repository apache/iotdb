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
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class LevelCompactionRecoverTest extends LevelCompactionTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  // uncompeleted target file and log
  /** compaction recover merge finished, delete one device - offset */
  @Test
  public void testCompactionRecoverWithUncompletedTargetFileAndLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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

    BufferedReader logReader =
        new BufferedReader(
            new FileReader(
                SystemFileFactory.INSTANCE.getFile(
                    tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME)));
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
    out.truncate(Long.parseLong(logs.get(logs.size() - 1).split(" ")[1]) - 1);
    out.close();

    levelCompactionTsFileManagement.addRecover(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
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
    assertEquals(500, count);
  }

  /** compaction recover merge finished */
  @Test
  public void testRecoverCompleteTargetFileAndCompactionLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
    assertEquals(500, count);
  }

  /** compeleted target file, and not resource files, compaction log exists */
  @Test
  public void testCompactionRecoverWithCompletedTargetFileAndLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
      resource.delete();
      assertFalse(resource.getTsFile().exists());
    }
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
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
    assertEquals(500, count);
  }

  /** compeleted target file, and not resource files, compaction log exists */
  @Test
  public void testCompactionRecoverWithCompletedTargetFile()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(true);
    deleteFileIfExists(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
      resource.delete();
      assertFalse(resource.getTsFile().exists());
    }
    levelCompactionTsFileManagement.add(targetTsFileResource, true);
    levelCompactionTsFileManagement.recover();
    context = new QueryContext();
    path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
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
    assertEquals(500, count);
  }

  /** compaction recover merge finished,unseq */
  @Test
  public void testCompactionMergeRecoverMergeFinishedUnseq()
      throws IOException, IllegalPathException {
    int prevUnseqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(2);

    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(seqResources, false);
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);

    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(0).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(1).getTsFile());
    compactionLogger.logFile(SOURCE_NAME, seqResources.get(2).getTsFile());
    compactionLogger.logSequence(false);
    deleteFileIfExists(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
    assertEquals(500, count);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnseqLevelNum);
  }

  // log exists, target file not exists
  /** compaction recover merge start just log source file */
  @Test
  public void testCompactionMergeRecoverMergeStartSourceLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
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
    assertEquals(500, count);
  }

  /** compaction recover merge start just log source file and sequence flag */
  @Test
  public void testCompactionMergeRecoverMergeStartSequenceLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
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
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        count++;
      }
    }
    assertEquals(500, count);
  }

  /** compaction recover merge start target file logged */
  @Test
  public void testCompactionMergeRecoverMergeStart() throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
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
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
    assertEquals(500, count);
  }

  /** compaction recover merge finished but no finish log */
  @Test
  public void testCompactionMergeRecoverMergeFinishedNoLog()
      throws IOException, IllegalPathException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
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
            TestConstant.BASE_OUTPUT_PATH.concat(
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
                TestConstant.BASE_OUTPUT_PATH.concat(
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
