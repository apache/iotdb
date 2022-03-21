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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

import org.h2.store.fs.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InnerSpaceCompactionExceptionTest extends AbstractInnerSpaceCompactionTest {

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
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    try (FileOutputStream os = new FileOutputStream(targetResource.getTsFile(), true);
        FileChannel channel = os.getChannel()) {
      channel.truncate(targetResource.getTsFileSize() - 10);
    }
    compactionLogger.close();
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
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
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    compactionLogger.close();
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
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
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    for (TsFileResource resource : seqResources) {
      tsFileManager.getSequenceListByTimePartition(0).remove(resource);
    }
    tsFileManager.getSequenceListByTimePartition(0).keepOrderInsert(targetResource);
    FileUtils.delete(seqResources.get(0).getTsFile().getPath());
    seqResources.get(0).remove();
    compactionLogger.close();
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
    Assert.assertEquals(1, tsFileManager.getSequenceListByTimePartition(0).size());
    Assert.assertEquals(targetResource, tsFileManager.getSequenceListByTimePartition(0).get(0));
  }

  /**
   * Test some source files are lost and target file is not complete. System should be set to read
   * only at this time.
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
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    seqResources.get(0).remove();
    try (FileOutputStream os = new FileOutputStream(targetResource.getTsFile(), true);
        FileChannel channel = os.getChannel()) {
      channel.truncate(targetResource.getTsFileSize() - 10);
    }
    compactionLogger.close();
    Assert.assertTrue(targetResource.getTsFile().exists());
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
    }

    Assert.assertFalse(tsFileManager.isAllowCompaction());
    Assert.assertTrue(IoTDBDescriptor.getInstance().getConfig().isReadOnly());
    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);
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
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
    }
    InnerSpaceCompactionUtils.combineModsInCompaction(seqResources, targetResource);

    seqResources.get(0).remove();
    compactionLogger.close();

    Assert.assertTrue(targetResource.getTsFile().exists());
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertTrue(targetResource.getModFile().exists());
    Collection<Modification> modifications = targetResource.getModFile().getModifications();
    Assert.assertEquals(seqResources.size(), modifications.size());
    for (Modification modification : modifications) {
      Assert.assertEquals(deviceIds[0], modification.getDevice());
      Assert.assertEquals(measurementSchemas[0].getMeasurementId(), modification.getMeasurement());
      Assert.assertEquals(Long.MAX_VALUE, modification.getFileOffset());
    }

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
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
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 10));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    seqResources.get(0).remove();
    compactionLogger.close();

    Assert.assertTrue(targetResource.getTsFile().exists());
    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
    Assert.assertTrue(targetResource.getTsFile().exists());
    Assert.assertTrue(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.getModFile().exists());

    seqResources.remove(0);
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
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
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum, i * ptNum + 5));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    InnerSpaceCompactionUtils.compact(targetResource, seqResources);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    for (int i = 0; i < seqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          deviceIds[0] + "." + measurementSchemas[0].getMeasurementId(),
          new Pair<>(i * ptNum + 10, i * ptNum + 15));
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), true);
      CompactionFileGeneratorUtils.generateMods(deleteMap, seqResources.get(i), false);
    }
    compactionLogger.close();

    InnerSpaceCompactionExceptionHandler.handleException(
        COMPACTION_TEST_SG,
        logFile,
        targetResource,
        seqResources,
        tsFileManager,
        tsFileManager.getSequenceListByTimePartition(0));
    Assert.assertFalse(targetResource.getTsFile().exists());
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.getModFile().exists());

    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertFalse(ModificationFile.getCompactionMods(resource).exists());
      Collection<Modification> modifications = resource.getModFile().getModifications();
      Assert.assertEquals(2, modifications.size());
      for (Modification modification : modifications) {
        Assert.assertEquals(deviceIds[0], modification.getDevice());
        Assert.assertEquals(
            measurementSchemas[0].getMeasurementId(), modification.getMeasurement());
        Assert.assertEquals(Long.MAX_VALUE, modification.getFileOffset());
      }
    }

    Assert.assertTrue(tsFileManager.isAllowCompaction());
  }
}
