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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.RewriteCrossCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
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

import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class RewriteCrossSpaceCompactionRecoverTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
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
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources);
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
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
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources);
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
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
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources);
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    seqResources.get(0).getTsFile().delete();
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
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
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources);
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
    compactionLogger.logStringInfo(MAGIC_STRING);
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
    CompactionUtils.combineModsInCompaction(seqResources, unseqResources, targetResources);
    seqResources.get(0).remove();
    compactionLogger.close();

    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
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
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources);
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    CompactionUtils.moveTargetFile(targetResources, false, COMPACTION_TEST_SG);
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
    CompactionUtils.combineModsInCompaction(seqResources, unseqResources, targetResources);

    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
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
      Assert.assertEquals(2, resource.getModFile().getModifications().size());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(2, resource.getModFile().getModifications().size());
    }

    // compaction log file should not exist
    Assert.assertFalse(compactionLogFile.exists());

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }
}
