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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionExceptionHandler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class InnerSpaceCompactionExceptionTest extends AbstractInnerSpaceCompactionTest {

  ICompactionPerformer performer = new FastCompactionPerformer(false);

  /**
   * Test when all source files exist, and target file is not complete. System should delete target
   * file and its resource at this time.
   *
   * @throws Exception
   */
  @Test
  public void testWhenAllSourceExistsAndTargetNotComplete() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    try (FileOutputStream os = new FileOutputStream(targetResource.getTsFile(), true);
        FileChannel channel = os.getChannel()) {
      channel.truncate(targetResource.getTsFileSize() - 10);
    }
    compactionLogger.close();
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(targetResource.resourceFileExists());

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Test when all source files exist, and target file is complete. System should delete target file
   * and its resource at this time.
   *
   * @throws Exception
   */
  @Test
  public void testWhenAllSourceExistsAndTargetComplete() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    compactionLogger.close();
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(targetResource.resourceFileExists());

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Test some source files lost and target file is complete. System should delete source files and
   * add target file to list.
   *
   * @throws Exception
   */
  @Test
  public void testWhenSomeSourceLostAndTargetComplete() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    for (TsFileResource resource : seqResources) {
      tsFileManager.getOrCreateSequenceListByTimePartition(0).remove(resource);
    }
    tsFileManager.getOrCreateSequenceListByTimePartition(0).keepOrderInsert(targetResource);
    Files.delete(seqResources.get(0).getTsFile().toPath());
    seqResources.get(0).remove();
    compactionLogger.close();
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
    Assert.assertEquals(1, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(
        targetResource, tsFileManager.getOrCreateSequenceListByTimePartition(0).get(0));
  }

  /**
   * Test some source files are lost and target file is not complete.
   *
   * @throws Exception
   */
  @Test
  public void testWhenSomeSourceLostAndTargetNotComplete() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    seqResources.get(0).remove();
    try (FileOutputStream os = new FileOutputStream(targetResource.getTsFile(), true);
        FileChannel channel = os.getChannel()) {
      channel.truncate(targetResource.getTsFileSize() - 10);
    }
    compactionLogger.close();
    Assert.assertTrue(targetResource.getTsFile().exists());
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
    }
  }

  /**
   * Test some source files are lost, and there are some compaction mods for source files. System
   * should collect these mods together and write them to mods file of target file.
   *
   * @throws Exception
   */
  @Test
  public void testHandleWithCompactionMods() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
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
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementName(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
    }
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResource);

    seqResources.get(0).remove();
    compactionLogger.close();

    Assert.assertTrue(targetResource.getTsFile().exists());
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertTrue(targetResource.anyModFileExists());
    Collection<ModEntry> modifications = targetResource.getAllModEntries();
    Assert.assertEquals(seqResources.size(), modifications.size());
    for (ModEntry modification : modifications) {
      Assert.assertEquals(
          deviceIds[0] + PATH_SEPARATOR + measurementSchemas[0].getMeasurementName(),
          ((TreeDeletionEntry) modification).getPathPattern().getFullPath());
    }

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Test some source files are lost, and there are some mods file for source files. System should
   * collect them and generate a new mods file for target file.
   *
   * @throws Exception
   */
  @Test
  public void testHandleWithNormalMods() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);

    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementName(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    seqResources.get(0).remove();
    compactionLogger.close();

    Assert.assertTrue(targetResource.getTsFile().exists());
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.anyModFileExists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }

  /**
   * Test source files exists, target file is not complete, and there are mods and compaction mods
   * for source files. System should remove target file, and combine the compaction mods and normal
   * mods together for each source file.
   *
   * @throws Exception
   */
  @Test
  public void testHandleWithCompactionModsAndNormalMods() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);
    File logFile =
        new File(
            targetResource.getTsFile().getPath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger compactionLogger = new CompactionLogger(logFile);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.logFiles(
        Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementName(),
          new Pair<>(i * ptNum, i * ptNum + 5));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
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
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementName(),
          new Pair<>(i * ptNum + 10, i * ptNum + 15));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    compactionLogger.close();

    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        Collections.singletonList(targetResource),
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);
    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.anyModFileExists());

    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertFalse(resource.compactionModFileExists());
      Collection<ModEntry> modifications = resource.getAllModEntries();
      Assert.assertEquals(2, modifications.size());
      for (ModEntry modification : modifications) {
        Assert.assertEquals(
            deviceIds[0] + PATH_SEPARATOR + measurementSchemas[0].getMeasurementName(),
            ((TreeDeletionEntry) modification).getPathPattern().getFullPath());
      }
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
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
    compactionLogger.logFiles(targetResources, CompactionLogger.STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.close();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, Collections.emptyList(), targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResources.get(0));
    seqResources.get(0).remove();

    // meet errors and handle exception
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.compactionModFileExists());
    }
    // the target file will be deleted
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
    compactionLogger.logFiles(targetResources, CompactionLogger.STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, CompactionLogger.STR_SOURCE_FILES);
    compactionLogger.close();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, Collections.emptyList(), targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    CompactionUtils.combineModsInInnerCompaction(seqResources, targetResources.get(0));

    // meet errors and handle exception
    CompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        compactionLogFile,
        targetResources,
        seqResources,
        Collections.emptyList(),
        tsFileManager,
        0,
        true,
        true);

    Assert.assertTrue(tsFileManager.isAllowCompaction());

    // all source file should exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertFalse(resource.compactionModFileExists());
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
}
