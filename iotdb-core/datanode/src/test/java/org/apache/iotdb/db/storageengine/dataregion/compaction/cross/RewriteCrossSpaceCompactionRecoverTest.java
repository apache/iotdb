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
package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class RewriteCrossSpaceCompactionRecoverTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  @Test
  public void testRecoverWithAllSourceFilesExisted() throws Exception {
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
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
  }

  @Test
  public void testRecoverWithAllSourceFilesExistedAndSomeTargetFilesNotExist() throws Exception {
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
    // Target files may not exist
    for (int i = 0; i < targetResources.size(); i++) {
      if (i < 2) {
        targetResources.get(i).removeResourceFile();
      } else {
        targetResources.get(i).remove();
      }
    }
    compactionLogger.close();
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
  }

  @Test
  public void testRecoverWithAllSourceFilesExistedAndTargetFilesMoved() throws Exception {
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
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
  }

  @Test
  public void testRecoverWithSomeSourceFilesExisted() throws Exception {
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
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    seqResources.get(0).getTsFile().delete();
    compactionLogger.close();
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
  }

  /**
   * Some source files have been deleted, each source file has old mods file and new compaction mods
   * file.
   */
  @Test
  public void testRecoverWithoutAllSourceFilesAndModFilesExist() throws Exception {
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
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
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
    seqResources.get(0).remove();

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
    // All source file should not exist. All compaction mods file and old mods file of each source
    // file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
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
      Assert.assertTrue(resource.anyModFileExists());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * All source files exist, each source file has compaction mods file which have been combined into
   * new mods file of the target file.
   */
  @Test
  public void testRecoverWithAllSourcesFileAndCompactonModFileExist() throws Exception {
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
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
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

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
    }

    // all compaction mods file of each source file should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      seqResources.get(i).resetModFile();
      ModificationFile f = seqResources.get(i).getCompactionModFile();
      Assert.assertFalse(f.getFileLength() > 0);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      unseqResources.get(i).resetModFile();
      Assert.assertFalse(unseqResources.get(i).getCompactionModFile().getFileLength() > 0);
    }

    // all mods file of each source file should exist
    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(1, resource.getAllModEntries().size());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(1, resource.getAllModEntries().size());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testRecoverWithAllSourcesFileAndCompactonModFileExistAndSomeTargetFilesNotExist()
      throws Exception {
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
    // Target files may not exist
    for (int i = 0; i < targetResources.size(); i++) {
      if (i < 2) {
        targetResources.get(i).removeResourceFile();
      } else {
        targetResources.get(i).remove();
      }
    }
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

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();
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
      Assert.assertFalse(resource.anyModFileExists());
    }

    // all compaction mods file of each source file should not exist
    for (int i = 0; i < seqResources.size(); i++) {
      seqResources.get(i).resetModFile();
      ModificationFile f = seqResources.get(i).getCompactionModFile();
      Assert.assertFalse(f.getFileLength() > 0);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      unseqResources.get(i).resetModFile();
      Assert.assertFalse(unseqResources.get(i).getCompactionModFile().getFileLength() > 0);
    }

    // all mods file of each source file should exist
    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(1, resource.getAllModEntries().size());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(1, resource.getAllModEntries().size());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  @Test
  public void testWhenTargetFileShouldBeDeletedAfterCompactionAndSomeSourceFilesLost()
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
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);
    compactionLogger.logFile(targetResources.get(0), STR_DELETED_TARGET_FILES);
    compactionLogger.close();
    seqResources.get(0).remove();

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    // the first target file should be deleted after recovery
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource resource = targetResources.get(i);
      if (i == 0) {
        Assert.assertFalse(resource.getTsFile().exists());
        Assert.assertFalse(resource.resourceFileExists());
      } else {
        Assert.assertTrue(resource.getTsFile().exists());
        Assert.assertTrue(resource.resourceFileExists());
      }
    }
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
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);
    compactionLogger.logFile(targetResources.get(0), CompactionLogger.STR_DELETED_TARGET_FILES);
    compactionLogger.close();
    CompactionUtils.deleteTsFilesInDisk(seqResources, COMPACTION_TEST_SG);
    CompactionUtils.deleteModificationForSourceFile(seqResources, COMPACTION_TEST_SG);
    CompactionUtils.deleteTsFilesInDisk(unseqResources, COMPACTION_TEST_SG);
    CompactionUtils.deleteModificationForSourceFile(unseqResources, COMPACTION_TEST_SG);

    if (targetResources.get(0).isDeleted()) {
      targetResources.get(0).remove();
    }

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // the first target file should be deleted after recovery
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource resource = targetResources.get(i);
      if (i == 0) {
        Assert.assertFalse(resource.getTsFile().exists());
        Assert.assertFalse(resource.resourceFileExists());
      } else {
        Assert.assertTrue(resource.getTsFile().exists());
        Assert.assertTrue(resource.resourceFileExists());
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
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInCrossCompaction(seqResources, unseqResources, targetResources);

    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, compactionLogFile, false)
        .doCompaction();

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(resource.anyModFileExists());
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
