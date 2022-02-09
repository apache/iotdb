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
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTest;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaTestUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_INFO;
import static org.junit.Assert.assertEquals;

public class SizeTieredCompactionRecoverTest extends AbstractInnerSpaceCompactionTest {

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, COMPACTION_TEST_SG);
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
    closeTsFileSequenceReader();
    assertEquals(500, count);
  }

  @Test
  public void testRecoverWithAllSourceFilesExisted() throws Exception {
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    // all source file should still exist
    Assert.assertTrue(seqResources.get(0).getTsFile().exists());
    Assert.assertTrue(seqResources.get(1).getTsFile().exists());
    Assert.assertTrue(seqResources.get(2).getTsFile().exists());
    // tmp target file, target file and target resource file should be deleted
    Assert.assertFalse(targetTsFileResource.getTsFile().exists());
    Assert.assertFalse(
        new File(
                targetTsFileResource
                    .getTsFilePath()
                    .replace(
                        IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX))
            .exists());
    Assert.assertFalse(
        new File(targetTsFileResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());

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
    closeTsFileSequenceReader();
    assertEquals(500, count);
  }

  @Test
  public void testRecoverWithoutAllSourceFilesExisted() throws Exception {
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, COMPACTION_TEST_SG);
    // delete one source file
    seqResources.get(0).remove();
    compactionLogger.close();
    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            compactionLogFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    // all source files should be deleted
    Assert.assertFalse(seqResources.get(0).getTsFile().exists());
    Assert.assertFalse(seqResources.get(1).getTsFile().exists());
    Assert.assertFalse(seqResources.get(2).getTsFile().exists());
    // target file and target resource file should exist
    Assert.assertTrue(targetTsFileResource.getTsFile().exists());
    Assert.assertTrue(
        new File(targetTsFileResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    // tmp target file should be deleted
    Assert.assertFalse(
        new File(
                targetTsFileResource
                    .getTsFilePath()
                    .replace(
                        TsFileConstant.TSFILE_SUFFIX, IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX))
            .exists());

    tsFileManager.add(targetTsFileResource, true);
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
    closeTsFileSequenceReader();
    assertEquals(500, count);
  }

  /**
   * All source files exist, each source file has compaction mods file which have been combined into
   * new mods file of the target file.
   */
  @Test
  public void testRecoverWithAllSourcesFileAndCompactonModFileExist() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(seqResources, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(seqResources.get(0).getTsFile().getParent(), targetFileName));
    File logFile =
        new File(
            targetResource.getTsFile().getPath() + SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger = new SizeTieredCompactionLogger(logFile.getPath());
    for (TsFileResource source : seqResources) {
      compactionLogger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, source.getTsFile());
    }
    compactionLogger.logSequence(true);
    compactionLogger.logFileInfo(
        SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
    InnerSpaceCompactionUtils.compact(targetResource, seqResources, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    InnerSpaceCompactionUtils.combineModsInCompaction(seqResources, targetResource);
    compactionLogger.close();

    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            logFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    // all source file should exist
    for (int i = 0; i < seqResources.size(); i++) {
      Assert.assertTrue(seqResources.get(i).getTsFile().exists());
      Assert.assertTrue(seqResources.get(i).resourceFileExists());
    }

    // tmp target file, target file and target resource file should be deleted
    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(
        new File(
                targetResource
                    .getTsFilePath()
                    .replace(
                        IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX))
            .exists());
    Assert.assertFalse(
        new File(targetResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());

    // all compaction mods file of each source file should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      Assert.assertFalse(seqResources.get(i).getCompactionModFile().exists());
    }

    // all mods file of each source file should exist
    for (int i = 0; i < seqResources.size(); i++) {
      seqResources.get(i).resetModFile();
      Assert.assertTrue(seqResources.get(i).getModFile().exists());
      Assert.assertEquals(2, seqResources.get(i).getModFile().getModifications().size());
    }

    // mods file of the target file should not exist
    Assert.assertFalse(targetResource.getModFile().exists());

    // compaction log file should not exist
    Assert.assertFalse(logFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Some source files have been deleted, each source file has old mods file and new compaction mods
   * file.
   */
  @Test
  public void testRecoverWithoutAllSourceFilesExistAndModFiles() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(seqResources, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(seqResources.get(0).getTsFile().getParent(), targetFileName));
    File logFile =
        new File(
            targetResource.getTsFile().getPath() + SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
    SizeTieredCompactionLogger compactionLogger = new SizeTieredCompactionLogger(logFile.getPath());
    for (TsFileResource source : seqResources) {
      compactionLogger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, source.getTsFile());
    }
    compactionLogger.logSequence(true);
    compactionLogger.logFileInfo(
        SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
    InnerSpaceCompactionUtils.compact(targetResource, seqResources, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    InnerSpaceCompactionUtils.combineModsInCompaction(seqResources, targetResource);
    seqResources.get(0).remove();
    compactionLogger.close();

    new SizeTieredCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            logFile,
            tempSGDir.getAbsolutePath(),
            true,
            CompactionTaskManager.currentTaskNum)
        .call();
    // all source files should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      Assert.assertFalse(seqResources.get(i).getTsFile().exists());
      Assert.assertFalse(seqResources.get(i).resourceFileExists());
    }

    // target file and target resource file should exist
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());

    // tmp target file should be deleted
    Assert.assertFalse(
        new File(
                targetResource
                    .getTsFilePath()
                    .replace(
                        TsFileConstant.TSFILE_SUFFIX, IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX))
            .exists());

    // all compaction mods file and old mods file of each source file should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      Assert.assertFalse(seqResources.get(i).getCompactionModFile().exists());
      Assert.assertFalse(seqResources.get(i).getModFile().exists());
    }

    // mods file of the target file should exist
    Assert.assertTrue(targetResource.getModFile().exists());

    // compaction log file should not exist
    Assert.assertFalse(logFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, COMPACTION_TEST_SG);
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
    closeTsFileSequenceReader();
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, COMPACTION_TEST_SG);
    compactionLogger.close();
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      deleteFileIfExists(resource.getTsFile());
      tsFileManager.remove(resource, true);
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
    closeTsFileSequenceReader();
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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
    InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, COMPACTION_TEST_SG);
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
    closeTsFileSequenceReader();
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
    closeTsFileSequenceReader();
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
    closeTsFileSequenceReader();
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
                            + IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)));
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

  private void closeTsFileSequenceReader() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }
}
