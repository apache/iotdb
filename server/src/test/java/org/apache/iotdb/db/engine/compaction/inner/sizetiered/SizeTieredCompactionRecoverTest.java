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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTest;
import org.apache.iotdb.db.engine.compaction.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.engine.compaction.log.CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX;
import static org.apache.iotdb.db.engine.compaction.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.log.CompactionLogger.STR_TARGET_FILES;
import static org.junit.Assert.assertEquals;

public class SizeTieredCompactionRecoverTest extends AbstractInnerSpaceCompactionTest {

  ICompactionPerformer performer = new FastCompactionPerformer(false);

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
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
                    tempSGDir.getPath(), COMPACTION_TEST_SG + INNER_COMPACTION_LOG_NAME_SUFFIX),
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
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                        TsFileConstant.TSFILE_SUFFIX))
            .exists());
    Assert.assertFalse(
        new File(targetTsFileResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());

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
  public void testRecoverWithAllSourceFilesExistedAndTargetFileNotExist() throws Exception {
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    // target file may not exist
    targetTsFileResource.remove();
    compactionLogger.close();
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                        TsFileConstant.TSFILE_SUFFIX))
            .exists());
    Assert.assertFalse(
        new File(targetTsFileResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());

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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
    // delete one source file
    seqResources.get(0).remove();
    compactionLogger.close();
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
                        TsFileConstant.TSFILE_SUFFIX,
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX))
            .exists());

    tsFileManager.add(targetTsFileResource, true);
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
            tsFileManager.getTsFileList(true).subList(3, 6),
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResource);
    compactionLogger.close();

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true).doCompaction();
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
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                        TsFileConstant.TSFILE_SUFFIX))
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
      Assert.assertEquals(1, seqResources.get(i).getModFile().getModifications().size());
    }

    // mods file of the target file should not exist
    Assert.assertFalse(targetResource.getModFile().exists());

    // compaction log file should not exist
    Assert.assertFalse(logFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testRecoverWithAllSourcesFileAndCompactonModFileExistAndTargetFileNotExist()
      throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    // target file may not exist
    targetResource.remove();
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    compactionLogger.close();

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true).doCompaction();
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
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                        TsFileConstant.TSFILE_SUFFIX))
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
      Assert.assertEquals(1, seqResources.get(i).getModFile().getModifications().size());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResource);
    seqResources.get(0).remove();
    compactionLogger.close();

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true).doCompaction();
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
                        TsFileConstant.TSFILE_SUFFIX,
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX))
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
    path =
        SchemaTestUtils.getMeasurementPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    TimeSeriesMetadataCache.getInstance().clear();
    ChunkCache.getInstance().clear();
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            tsFileManager.getTsFileList(true).subList(0, 5),
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
    compactionLogger.close();
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      deleteFileIfExists(resource.getTsFile());
      tsFileManager.remove(resource, true);
    }
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
    deleteFileIfExists(compactionLogFile);
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      tsFileManager.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
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
    CompactionLogger sizeTieredCompactionLogger =
        new CompactionLogger(
            new File(
                tempSGDir.getPath(),
                COMPACTION_TEST_SG + COMPACTION_TEST_SG + INNER_COMPACTION_LOG_NAME_SUFFIX));
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    sizeTieredCompactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
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
    CompactionLogger sizeTieredCompactionLogger =
        new CompactionLogger(
            new File(tempSGDir.getPath(), COMPACTION_TEST_SG + INNER_COMPACTION_LOG_NAME_SUFFIX));
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    sizeTieredCompactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
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
    CompactionLogger sizeTieredCompactionLogger =
        new CompactionLogger(
            new File(tempSGDir.getPath(), COMPACTION_TEST_SG + INNER_COMPACTION_LOG_NAME_SUFFIX));
    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));
    sizeTieredCompactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
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
                            + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
    sizeTieredCompactionLogger.logFiles(
        Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
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
