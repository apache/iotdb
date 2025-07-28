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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class SettleCompactionRecoverTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    // params have been reset in AbstractCompactionTest.teardown
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTLForTree();
  }

  // region Handle exception

  @Test
  public void handExceptionWhenSettlingAllDeletedFilesWithOnlyAllDeletedFiles()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);

    // delete the first all_deleted file
    task.setRecoverMemoryStatus(true);
    allDeletedFiles.get(0).getTsFile().delete();

    // add compaction mods
    generateDeviceCompactionMods(3);

    // handle exception, delete all_deleted files
    task.recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void handExceptionWhenSettlingAllDeletedFiles()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);

    // delete the first all_deleted file
    task.setRecoverMemoryStatus(true);
    allDeletedFiles.get(0).getTsFile().delete();

    // add compaction mods
    generateDeviceCompactionMods(3);

    // handle exception, delete all_deleted files
    task.recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void handExceptionWhenSettlingPartialDeletedFilesWithAllSourceFileExisted()
      throws Exception {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // handle exception, delete all_deleted files
    task.recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
    Assert.assertTrue(!tsFileManager.contains(targetResource, false));
    Assert.assertTrue(!tsFileManager.contains(targetResource, true));
    // resource file exist
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    // target resource not exist
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.anyModFileExists());
  }

  @Test
  public void handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLosted()
      throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // delete source file
    partialDeletedFiles.get(0).remove();

    // handle exception, delete all_deleted files
    task.recover();

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // target file exist
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertTrue(targetResource.tsFileExists());
    Assert.assertTrue(targetResource.anyModFileExists());

    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
  }

  @Test
  public void
      handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndEmptyTargetFile()
          throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // delete source file
    partialDeletedFiles.get(0).remove();
    // target resource is marked deleted after compaction
    Assert.assertTrue(targetResource.isDeleted());

    // handle exception, delete all_deleted files
    task.recover();

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    // target file is deleted after compaction
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.anyModFileExists());
    Assert.assertFalse(targetResource.getCompactionModFile().exists());

    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void
      handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndTargetFileLosted()
          throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // delete source file
    partialDeletedFiles.get(0).remove();

    // target file not exist
    targetResource.getTsFile().delete();

    // handle exception, delete all_deleted files
    task.recoverFullyDirtyFiles();
    try {
      task.recoverSettleTaskInfoFromLogFile();
      Assert.fail();
    } catch (Exception e) {
      // do nothing
    }

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
  }

  // endregion

  // region recover

  @Test
  public void recoverWhenSettlingAllDeletedFilesWithOnlyAllDeletedFiles()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);

    // add compaction mods
    generateDeviceCompactionMods(3);

    File logFile =
        new File(
            task.getAllSourceTsFiles().get(0).getTsFilePath()
                + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.force();
      // delete the first all_deleted file
      allDeletedFiles.get(0).getTsFile().delete();
    }

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.getTotalModSizeInByte() > 0);
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingAllDeletedFiles()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);

    // add compaction mods
    generateDeviceCompactionMods(3);

    File logFile =
        new File(
            task.getAllSourceTsFiles().get(0).getTsFilePath()
                + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.force();
      // delete the first all_deleted file
      allDeletedFiles.get(0).getTsFile().delete();
    }

    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getCompactionModFile().exists());
    }

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.getTotalModSizeInByte() > 0);
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingPartialDeletedFilesWithAllSourceFileExisted() throws Exception {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // finish to settle all_deleted files and settle the first partial_deleted group
      task.setRecoverMemoryStatus(true);
      task.settleWithFullyDirtyFiles();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    // resource file exist
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }

    // target resource not exist
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.getTotalModSizeInByte() > 0);

    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingPartialDeletedFilesWithAllSourceFileExisted2() throws Exception {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
    }

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }

    // resource file exist
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }

    // target resource not exist
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.anyModFileExists());

    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingPartialDeletedFilesWithSomeSourceFileLosted() throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // delete source file
    partialDeletedFiles.get(0).remove();

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    // target file exist
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertTrue(targetResource.tsFileExists());
    Assert.assertTrue(targetResource.getTotalModSizeInByte() > 0);

    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndEmptyTargetFile()
      throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
      compactionLogger.logEmptyTargetFile(targetResource);
      compactionLogger.force();
      targetResource.getTsFile().delete();
    }

    // delete source file
    partialDeletedFiles.get(0).remove();
    // target resource is marked deleted after compaction
    Assert.assertTrue(targetResource.isDeleted());

    // handle exception, delete all_deleted files
    new SettleCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile).recover();

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : partialDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.getTotalModSizeInByte() > 0);
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().getFileLength() > 0);
    }
    // target file is deleted after compaction
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.getTotalModSizeInByte() > 0);
    Assert.assertFalse(targetResource.getCompactionModFile().getFileLength() > 0);

    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void recoverWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndTargetFileLosted()
      throws Exception {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithFullyDirtyFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    task.setTargetTsFileResource(targetResource);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource), CompactionTaskType.SETTLE, COMPACTION_TEST_SG);

      CompactionUtils.combineModsInInnerCompaction(partialDeletedFiles, targetResource);
      tsFileManager.replace(
          Collections.emptyList(),
          partialDeletedFiles,
          Collections.singletonList(targetResource),
          0);
    }

    // delete source file
    partialDeletedFiles.get(0).remove();

    // target file not exist
    targetResource.getTsFile().delete();

    // handle exception, delete all_deleted files
    task.recoverFullyDirtyFiles();

    try {
      task.recoverSettleTaskInfoFromLogFile();
      Assert.fail();
    } catch (Exception e) {
      // do nothing
    }

    // source files not exist
    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    Assert.assertTrue(logFile.exists());
  }

  // endregion

  private void generateDeviceCompactionMods(int deviceNum)
      throws IllegalPathException, IOException {
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < deviceNum; d++) {
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "**",
          new Pair(Long.MIN_VALUE, Long.MAX_VALUE));
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, true);
    }
    for (TsFileResource resource : unseqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, true);
    }
  }
}
