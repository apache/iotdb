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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner.sizetiered;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.inner.AbstractInnerSpaceCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaTestUtils;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.writer.TsFileOutput;
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

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
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

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
    path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    performer.setSummary(new FastCompactionTaskSummary());
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
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    performer.setSummary(new FastCompactionTaskSummary());
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
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    closeTsFileSequenceReader();
    assertEquals(500, count);

    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));

    TsFileResource targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(tmpSeqResources, true);
    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
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
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());

    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true).subList(3, 6),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
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
    performer.setSummary(new FastCompactionTaskSummary());
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
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
    path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    TimeSeriesMetadataCache.getInstance().clear();
    ChunkCache.getInstance().clear();
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true).subList(0, 5),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    closeTsFileSequenceReader();
    assertEquals(500, count);

    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));

    TsFileResource targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(tmpSeqResources, true);

    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    compactionLogger.close();
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      deleteFileIfExists(resource.getTsFile());
      tsFileManager.remove(resource, true);
    }
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
    path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
        count++;
      }
    }
    tsFilesReader.close();
    closeTsFileSequenceReader();
    assertEquals(500, count);

    List<TsFileResource> tmpSeqResources = new ArrayList<>();
    tmpSeqResources.add(seqResources.get(0));
    tmpSeqResources.add(seqResources.get(1));
    tmpSeqResources.add(seqResources.get(2));

    TsFileResource targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(tmpSeqResources, true);

    File compactionLogFile =
        new File(
            seqResources.get(0).getTsFile().getParent()
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(tmpSeqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetTsFileResource), STR_TARGET_FILES);
    deleteFileIfExists(targetTsFileResource.getTsFile());
    performer.setSourceFiles(new ArrayList<>(seqResources.subList(0, 3)));
    performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetTsFileResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    deleteFileIfExists(compactionLogFile);
    for (TsFileResource resource : new ArrayList<>(seqResources.subList(0, 3))) {
      tsFileManager.remove(resource, true);
      deleteFileIfExists(resource.getTsFile());
    }
    tsFileManager.add(targetTsFileResource, true);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();
    path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    logger.warn("TsFiles in list is {}", tsFileManager.getTsFileList(true));
    tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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
    sizeTieredCompactionLogger.close();
    IFullPath path =
        SchemaTestUtils.getNonAlignedFullPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(batchData.getTimeByIndex(i), batchData.getColumn(0).getDouble(i), 0.001);
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

  @Test
  public void testWhenTargetFileShouldBeDeletedAfterCompactionAndSomeSourceFilesLost()
      throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // generate mods file, the target file should be deleted after compaction
    for (int device = 0; device < deviceNum; device++) {
      for (int measurement = 0; measurement < measurementNum; measurement++) {
        Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "device"
                + device
                + PATH_SEPARATOR
                + "sensor"
                + measurement,
            new Pair(Long.MIN_VALUE, Long.MAX_VALUE));
        seqResources.forEach(
            x -> {
              try {
                CompactionFileGeneratorUtils.generateMods(deleteMap, x, false);
              } catch (IllegalPathException | IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);

    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, Collections.emptyList(), targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResources.get(0));
    compactionLogger.logFile(targetResources.get(0), STR_DELETED_TARGET_FILES);
    compactionLogger.close();
    seqResources.get(0).remove();

    // recover compaction
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // the target file should be deleted
    Assert.assertFalse(targetResources.get(0).getTsFile().exists());
    Assert.assertFalse(targetResources.get(0).resourceFileExists());
  }

  @Test
  public void testWhenTargetFileIsDeletedAfterCompactionAndSomeSourceFilesLost() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // generate mods file, the target file should be deleted after compaction
    for (int device = 0; device < deviceNum; device++) {
      for (int measurement = 0; measurement < measurementNum; measurement++) {
        Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "device"
                + device
                + PATH_SEPARATOR
                + "sensor"
                + measurement,
            new Pair(Long.MIN_VALUE, Long.MAX_VALUE));
        seqResources.forEach(
            x -> {
              try {
                CompactionFileGeneratorUtils.generateMods(deleteMap, x, false);
              } catch (IllegalPathException | IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);

    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, Collections.emptyList(), targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResources.get(0));
    compactionLogger.logFile(targetResources.get(0), STR_DELETED_TARGET_FILES);
    compactionLogger.close();
    CompactionUtils.deleteTsFilesInDisk(seqResources, COMPACTION_TEST_SG);
    CompactionUtils.deleteModificationForSourceFile(seqResources, COMPACTION_TEST_SG);

    if (targetResources.get(0).isDeleted()) {
      targetResources.get(0).remove();
    }

    // recover compaction
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // the target file should be deleted
    Assert.assertFalse(targetResources.get(0).getTsFile().exists());
    Assert.assertFalse(targetResources.get(0).resourceFileExists());
  }

  @Test
  public void testWhenTargetFileIsDeletedAfterCompactionAndAllSourceFilesExisted()
      throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // generate mods file, the target file should be deleted after compaction
    for (int device = 0; device < deviceNum; device++) {
      for (int measurement = 0; measurement < measurementNum; measurement++) {
        Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "device"
                + device
                + PATH_SEPARATOR
                + "sensor"
                + measurement,
            new Pair(Long.MIN_VALUE, Long.MAX_VALUE));
        seqResources.forEach(
            x -> {
              try {
                CompactionFileGeneratorUtils.generateMods(deleteMap, x, false);
              } catch (IOException | IllegalPathException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.close();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, Collections.emptyList(), targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResources.get(0));

    // recover compaction
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, true)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // tmp target file, target file and target resource file should be deleted after compaction
    for (TsFileResource resource : targetResources) {
      if (resource == null) {
        continue;
      }
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                              TsFileConstant.TSFILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
    }
  }

  private void closeTsFileSequenceReader() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }
}
