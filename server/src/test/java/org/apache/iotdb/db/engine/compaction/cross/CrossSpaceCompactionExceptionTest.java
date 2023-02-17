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
package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.execute.exception.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class CrossSpaceCompactionExceptionTest extends AbstractCompactionTest {

  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
    new CompactionConfigRestorer().restoreCompactionConfig();
  }

  @Test
  public void testHandleWithAllSourceFilesExisted() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);
    // all source file should still exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    // tmp target file, target file and target resource file should be deleted
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                              TsFileConstant.TSFILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
    }
    Assert.assertEquals(4, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(5, tsFileManager.getOrCreateUnsequenceListByTimePartition(0).size());
    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testHandleWithAllSourceFilesExistedAndTargetFilesMoved() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    compactionLogger.close();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);
    // all source file should still exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    // tmp target file, target file and target resource file should be deleted
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX,
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                              TsFileConstant.TSFILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
    }
    Assert.assertEquals(4, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(5, tsFileManager.getOrCreateUnsequenceListByTimePartition(0).size());
    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testHandleWithSomeSourceFilesExisted() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    for (TsFileResource resource : seqResources) {
      tsFileManager.getOrCreateSequenceListByTimePartition(0).remove(resource);
    }
    for (TsFileResource resource : unseqResources) {
      tsFileManager.getOrCreateUnsequenceListByTimePartition(0).remove(resource);
    }
    for (TsFileResource resource : targetResources) {
      tsFileManager.getOrCreateSequenceListByTimePartition(0).keepOrderInsert(resource);
    }
    seqResources.get(0).getTsFile().delete();
    compactionLogger.close();
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);
    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
    }
    // tmp target file and tmp target resource file should not exist, target file and target
    // resource file should exist
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX,
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              TsFileConstant.TSFILE_SUFFIX,
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    Assert.assertEquals(4, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(0, tsFileManager.getOrCreateUnsequenceListByTimePartition(0).size());
    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Some source files have been deleted, each source file has old mods file and new compaction mods
   * file.
   */
  @Test
  public void testHandleWithoutAllSourceFilesAndModFilesExist() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    compactionLogger.close();
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);
    for (TsFileResource resource : seqResources) {
      tsFileManager.getOrCreateSequenceListByTimePartition(0).remove(resource);
    }
    for (TsFileResource resource : unseqResources) {
      tsFileManager.getOrCreateUnsequenceListByTimePartition(0).remove(resource);
    }
    for (TsFileResource resource : targetResources) {
      tsFileManager.getOrCreateSequenceListByTimePartition(0).keepOrderInsert(resource);
    }
    seqResources.get(0).remove();

    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);
    // All source file should not exist. All compaction mods file and old mods file of each source
    // file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    // tmp target file and tmp target resource file should not exist, target file and target
    // resource file should exist
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX,
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              TsFileConstant.TSFILE_SUFFIX,
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      // mods file of the target file should exist
      Assert.assertTrue(resource.getModFile().exists());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertEquals(4, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(0, tsFileManager.getOrCreateUnsequenceListByTimePartition(0).size());
    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * All source files exist, each source file has compaction mods file which have been combined into
   * new mods file of the target file.
   */
  @Test
  public void testHandleWithAllSourcesFileAndCompactonModFileExist() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    compactionLogger.close();
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);

    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);
    // all source file should still exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    // tmp target file, target file and target resource file should be deleted
    for (TsFileResource resource : targetResources) {
      // xxx.tsfile should not exist
      Assert.assertFalse(resource.getTsFile().exists());
      // xxx.merge should not exist
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX,
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX))
              .exists());
      // xxx.tsfile.resource should not exist
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());

      // mods file of the target file should not exist
      Assert.assertFalse(resource.getModFile().exists());
    }

    // all compaction mods file of each source file should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      seqResources.get(i).resetModFile();
      ModificationFile f = seqResources.get(i).getCompactionModFile();
      Assert.assertFalse(f.exists());
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      unseqResources.get(i).resetModFile();
      Assert.assertFalse(unseqResources.get(i).getCompactionModFile().exists());
    }

    // all mods file of each source file should exist
    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(1, resource.getModFile().getModifications().size());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(1, resource.getModFile().getModifications().size());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertEquals(4, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(5, tsFileManager.getOrCreateUnsequenceListByTimePartition(0).size());
    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testWhenTargetFileIsDeletedAfterCompactionAndSomeSourceFilesLost() throws Exception {
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // generate mods file, the first target file should be deleted after compaction
    for (int device = 0; device < 3; device++) {
      for (int measurement = 0; measurement < 4; measurement++) {
        Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + device + PATH_SEPARATOR + "s" + measurement,
            new Pair(0L, 300L));
        CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(0), false);
        CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(0), false);
        CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(1), false);
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    compactionLogger.close();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);
    seqResources.get(0).remove();

    // meet errors and handle exception
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // the first target file should be deleted after compaction, the others still exist
    for (TsFileResource resource : targetResources) {
      if (resource.getVersion() == 0) {
        Assert.assertFalse(resource.getTsFile().exists());
        Assert.assertFalse(resource.resourceFileExists());
      } else {
        Assert.assertTrue(resource.getTsFile().exists());
        Assert.assertTrue(resource.resourceFileExists());
        Assert.assertEquals(TsFileResourceStatus.CLOSED, resource.getStatus());
      }
    }
  }

  @Test
  public void testWhenTargetFileIsDeletedAfterCompactionAndAllSourceFilesExisted()
      throws Exception {
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // generate mods file, the first target file should be deleted after compaction
    for (int device = 0; device < 3; device++) {
      for (int measurement = 0; measurement < 4; measurement++) {
        Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + device + PATH_SEPARATOR + "s" + measurement,
            new Pair(0L, 300L));
        seqResources.forEach(
            x -> {
              try {
                CompactionFileGeneratorUtils.generateMods(deleteMap, x, false);
              } catch (IllegalPathException | IOException e) {
                throw new RuntimeException(e);
              }
            });
        unseqResources.forEach(
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
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            targetResources.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SOURCE_FILES);
    compactionLogger.logFiles(unseqResources, STR_SOURCE_FILES);
    compactionLogger.close();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);

    // meet errors and handle exception
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        unseqResources,
        tsFileManager,
        0,
        false,
        true);

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
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
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                              TsFileConstant.TSFILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
    }
  }
}
