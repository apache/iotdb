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

import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.CompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/** This test is used to test the compatibility of compaction recovery with 0.12. */
public class SizeTieredCompactionRecoverCompatibleTest extends AbstractCompactionTest {
  @Override
  @Before
  public void setUp() throws IOException, MetadataException, WriteProcessException {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws StorageEngineException, IOException {
    super.tearDown();
  }

  @Test
  public void testCompatibleWithAllSourceFilesExistWithFilePath() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    registerTimeseriesInMManger(2, 3, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    RandomAccessFile targetFile = new RandomAccessFile(targetResource.getTsFile(), "rw");
    long fileLength = targetFile.length();
    targetFile.getChannel().truncate(fileLength - 20);
    targetFile.close();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }

    File logFile =
        new File(
            targetResource.getTsFile().getParent(),
            "root.compactionTest" + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(
          String.format(
              "info-source\nroot.compactionTest 0 0 %s sequence\n",
              tsFileResource.getTsFile().getName()));
    }
    logWriter.write("sequence\n");
    logWriter.write(
        String.format(
            "info-target\nroot.compactionTest 0 0 %s sequence\n",
            targetResource.getTsFile().getName()));
    logWriter.close();

    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", targetResource.getTsFile().getParent());
    tsFileManager.addAll(seqResources, true);
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true);
    recoverTask.doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getModFile().exists());
    }

    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.getModFile().exists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testCompatibleWithSomeSourceFilesLostWithFilePath() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    registerTimeseriesInMManger(2, 3, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, "root.compactionTest");

    // first source file does not exist
    seqResources.get(0).delete();

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }

    File logFile =
        new File(
            targetResource.getTsFile().getParent(),
            "root.compactionTest" + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX_FROM_OLD);
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(
          String.format(
              "info-source\nroot.compactionTest 0 0 %s sequence\n",
              tsFileResource.getTsFile().getName()));
    }
    logWriter.write("sequence\n");
    logWriter.write(
        String.format(
            "info-target\nroot.compactionTest 0 0 %s sequence\n",
            targetResource.getTsFile().getName()));
    logWriter.close();

    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", targetResource.getTsFile().getParent());
    tsFileManager.addAll(seqResources, true);
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true);
    recoverTask.doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }

    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testCompatibleWithAllSourceFilesExistWithFileInfo() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, true);
    registerTimeseriesInMManger(2, 3, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    RandomAccessFile targetFile = new RandomAccessFile(targetResource.getTsFile(), "rw");
    long fileLength = targetFile.length();
    targetFile.getChannel().truncate(fileLength - 20);
    targetFile.close();

    File logFile =
        new File(targetResource.getTsFile().getParent(), "root.compactionTest" + ".compaction.log");
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    for (TsFileResource tsFileResource : seqResources) {
      logWriter.write(String.format("source\n%s\n", tsFileResource.getTsFile().getAbsolutePath()));
    }
    logWriter.write("sequence\n");
    logWriter.write(String.format("target\n%s\n", targetResource.getTsFile().getAbsolutePath()));
    logWriter.close();

    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", targetResource.getTsFile().getParent());
    tsFileManager.addAll(seqResources, true);
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true);
    recoverTask.doCompaction();

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
    }

    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testCompatibleWithSomeSourceFilesLostWithFileInfo() throws Exception {
    createFiles(6, 2, 3, 100, 0, 0, 50, 50, false, false);
    registerTimeseriesInMManger(2, 3, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(unseqResources, true);
    InnerSpaceCompactionUtils.compact(targetResource, unseqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, "root.compactionTest");

    // first source file does not exist
    unseqResources.get(0).delete();

    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0",
          new Pair(i * 10L, i * 10L + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    File logFile =
        new File(targetResource.getTsFile().getParent(), "root.compactionTest" + ".compaction.log");
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    for (TsFileResource tsFileResource : unseqResources) {
      logWriter.write(String.format("source\n%s\n", tsFileResource.getTsFile().getAbsolutePath()));
    }
    logWriter.write("sequence\n");
    logWriter.write(String.format("target\n%s\n", targetResource.getTsFile().getAbsolutePath()));
    logWriter.close();

    TsFileManager tsFileManager =
        new TsFileManager("root.compactionTest", "0", targetResource.getTsFile().getParent());
    tsFileManager.addAll(unseqResources, false);
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(COMPACTION_TEST_SG, "0", tsFileManager, logFile, true);
    recoverTask.doCompaction();

    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getModFile().exists());
    }

    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertFalse(logFile.exists());
  }
}
