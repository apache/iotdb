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
package org.apache.iotdb.db.engine.compaction.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.execute.recover.CompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class RewriteCrossSpaceCompactionRecoverCompatibleTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();

  @Override
  @Before
  public void setUp()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @Override
  @After
  public void tearDown() throws StorageEngineException, IOException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  @Test
  public void testCompatibleWithAllSourceFilesExistWithFileInfo() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(3, 2, 3, 50, 0, 0, 50, 50, false, false);
    registerTimeseriesInMManger(2, 3, false);
    List<TsFileResource> tmpTargetResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      File mergeFile =
          new File(
              resource.getTsFilePath() + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      mergeFile.createNewFile();
      tmpTargetResources.add(new TsFileResource(mergeFile));
    }

    File logFile =
        new File(SEQ_DIRS.getPath(), CompactionLogger.CROSS_COMPACTION_LOG_NAME_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    logWriter.write("seqFiles\n");
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(
          String.format(
              COMPACTION_TEST_SG + " 0 0 %s true\n", tsFileResource.getTsFile().getName()));
    }
    logWriter.write("unseqFiles\n");
    for (TsFileResource tsFileResource : unseqResources) {
      logWriter.write(
          String.format(
              COMPACTION_TEST_SG + " 0 0 %s false\n", tsFileResource.getTsFile().getName()));
    }
    logWriter.close();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    ModificationFile mergeMods =
        new ModificationFile(
            SEQ_DIRS + File.separator + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
    mergeMods.write(new Deletion(new PartialPath("root.d1.s1"), 100, 0, 100));
    mergeMods.close();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, false)
        .doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }
    for (TsFileResource resource : tmpTargetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }
    for (TsFileResource resource :
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources)) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }

    Assert.assertFalse(logFile.exists());
    Assert.assertFalse(mergeMods.exists());
  }

  @Test
  public void testCompatibleWithSomeSourceFilesLostWithFileInfo() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(3, 2, 3, 50, 0, 0, 50, 50, false, false);
    registerTimeseriesInMManger(2, 3, false);
    List<TsFileResource> tmpTargetResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      File mergeFile =
          new File(
              resource.getTsFilePath() + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      Files.copy(resource.getTsFile(), mergeFile);
      tmpTargetResources.add(new TsFileResource(mergeFile));
    }

    File logFile =
        new File(SEQ_DIRS.getPath(), CompactionLogger.CROSS_COMPACTION_LOG_NAME_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    logWriter.write("seqFiles\n");
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(
          String.format(
              COMPACTION_TEST_SG + " 0 0 %s true\n", tsFileResource.getTsFile().getName()));
    }
    logWriter.write("unseqFiles\n");
    for (TsFileResource tsFileResource : unseqResources) {
      logWriter.write(
          String.format(
              COMPACTION_TEST_SG + " 0 0 %s false\n", tsFileResource.getTsFile().getName()));
    }
    logWriter.close();

    // First seq files lost
    seqResources.get(0).delete();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    ModificationFile mergeMods =
        new ModificationFile(
            SEQ_DIRS + File.separator + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
    mergeMods.write(new Deletion(new PartialPath("root.d1.s1"), 100, 0, 100));
    mergeMods.close();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, false)
        .doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : tmpTargetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }
    for (TsFileResource seqResource :
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources)) {
      TsFileResource resource =
          new TsFileResource(
              new File(
                  seqResource
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX)));
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }

    Assert.assertFalse(logFile.exists());
    Assert.assertFalse(mergeMods.exists());
  }

  @Test
  public void testCompatibleWithAllSourceFilesExistWithFilePath() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(3, 2, 3, 50, 0, 0, 50, 50, false, false);
    registerTimeseriesInMManger(2, 3, false);
    List<TsFileResource> tmpTargetResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      File mergeFile =
          new File(
              resource.getTsFilePath() + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      mergeFile.createNewFile();
      tmpTargetResources.add(new TsFileResource(mergeFile));
    }

    File logFile =
        new File(SEQ_DIRS.getPath(), CompactionLogger.CROSS_COMPACTION_LOG_NAME_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    logWriter.write("seqFiles\n");
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(tsFileResource.getTsFile().getAbsolutePath());
      logWriter.write("\n");
    }
    logWriter.write("unseqFiles\n");
    for (TsFileResource tsFileResource : unseqResources) {
      logWriter.write(tsFileResource.getTsFile().getAbsolutePath());
      logWriter.write("\n");
    }
    logWriter.write(CompactionLogger.STR_MERGE_START_FROM_OLD);
    logWriter.write("\n");
    logWriter.write(seqResources.get(0).getTsFile().getAbsolutePath() + " " + 100);
    logWriter.close();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    ModificationFile mergeMods =
        new ModificationFile(
            SEQ_DIRS + File.separator + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
    mergeMods.write(new Deletion(new PartialPath("root.d1.s1"), 100, 0, 100));
    mergeMods.close();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, false)
        .doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }
    for (TsFileResource resource : tmpTargetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }
    for (TsFileResource resource :
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources)) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }

    Assert.assertFalse(logFile.exists());
    Assert.assertFalse(mergeMods.exists());
  }

  @Test
  public void testCompatibleWithSomeSourceFilesLostWithFilePath() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(3, 2, 3, 50, 0, 0, 50, 50, false, false);
    registerTimeseriesInMManger(2, 3, false);
    List<TsFileResource> tmpTargetResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      File mergeFile =
          new File(
              resource.getTsFilePath() + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD);
      Files.copy(resource.getTsFile(), mergeFile);
      tmpTargetResources.add(new TsFileResource(mergeFile));
    }

    File logFile =
        new File(SEQ_DIRS.getPath(), CompactionLogger.CROSS_COMPACTION_LOG_NAME_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    logWriter.write("seqFiles\n");
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(tsFileResource.getTsFile().getAbsolutePath());
      logWriter.write("\n");
    }
    logWriter.write("unseqFiles\n");
    for (TsFileResource tsFileResource : unseqResources) {
      logWriter.write(tsFileResource.getTsFile().getAbsolutePath());
      logWriter.write("\n");
    }
    logWriter.write(CompactionLogger.STR_MERGE_START_FROM_OLD);
    logWriter.write("\n");
    logWriter.write(seqResources.get(0).getTsFile().getAbsolutePath() + " " + 100);
    logWriter.close();

    // First seq files lost
    seqResources.get(0).delete();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s1",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }
    ModificationFile mergeMods =
        new ModificationFile(
            SEQ_DIRS + File.separator + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
    mergeMods.write(new Deletion(new PartialPath("root.d1.s1"), 100, 0, 100));
    mergeMods.close();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, false)
        .doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : tmpTargetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
    }
    for (TsFileResource seqResource :
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources)) {
      TsFileResource resource =
          new TsFileResource(
              new File(
                  seqResource
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX)));
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }

    Assert.assertFalse(logFile.exists());
    Assert.assertFalse(mergeMods.exists());
  }
}
