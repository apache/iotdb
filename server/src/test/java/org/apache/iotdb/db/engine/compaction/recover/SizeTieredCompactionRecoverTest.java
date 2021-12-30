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
package org.apache.iotdb.db.engine.compaction.recover;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SizeTieredCompactionRecoverTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String SEQ_FILE_DIR =
      TestConstant.BASE_OUTPUT_PATH
          + File.separator
          + "data"
          + File.separator
          + "sequence"
          + File.separator
          + COMPACTION_TEST_SG
          + File.separator
          + "0"
          + File.separator
          + "0";
  static final String UNSEQ_FILE_DIR =
      TestConstant.BASE_OUTPUT_PATH
          + File.separator
          + "data"
          + File.separator
          + "unsequence"
          + File.separator
          + COMPACTION_TEST_SG
          + File.separator
          + "0"
          + File.separator
          + "0";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device0.sensor1",
        COMPACTION_TEST_SG + ".device0.sensor2",
        COMPACTION_TEST_SG + ".device0.sensor3",
        COMPACTION_TEST_SG + ".device0.sensor4",
        COMPACTION_TEST_SG + ".device0.sensor5",
        COMPACTION_TEST_SG + ".device0.sensor6",
        COMPACTION_TEST_SG + ".device0.sensor7",
        COMPACTION_TEST_SG + ".device0.sensor8",
        COMPACTION_TEST_SG + ".device0.sensor9",
        COMPACTION_TEST_SG + ".device1.sensor0",
        COMPACTION_TEST_SG + ".device1.sensor1",
        COMPACTION_TEST_SG + ".device1.sensor2",
        COMPACTION_TEST_SG + ".device1.sensor3",
        COMPACTION_TEST_SG + ".device1.sensor4",
      };
  static String logFilePath =
      TestConstant.BASE_OUTPUT_PATH + File.separator + "test-compaction.compaction.log";
  static String[] originDataDirs = null;
  static String[] testDataDirs = new String[] {TestConstant.BASE_OUTPUT_PATH + "data"};
  static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws Exception {
    originDataDirs = config.getDataDirs();
    setDataDirs(testDataDirs);
    if (!new File(SEQ_FILE_DIR).exists()) {
      Assert.assertTrue(new File(SEQ_FILE_DIR).mkdirs());
    }
    if (!new File(UNSEQ_FILE_DIR).exists()) {
      Assert.assertTrue(new File(UNSEQ_FILE_DIR).mkdirs());
    }
  }

  @After
  public void tearDown() throws Exception {
    setDataDirs(originDataDirs);
    File dataDir = new File(testDataDirs[0]);
    if (dataDir.exists()) {
      FileUtils.forceDelete(dataDir);
    }
    File logFile = new File(logFilePath);
    if (logFile.exists()) {
      Assert.assertTrue(logFile.delete());
    }
  }

  public void setDataDirs(String[] dataDirs) throws Exception {
    Class configClass = config.getClass();
    Field dataDirsField = configClass.getDeclaredField("dataDirs");
    dataDirsField.setAccessible(true);
    dataDirsField.set(config, dataDirs);
  }

  /** Test when a file that is not a directory exists under virtual storageGroup dir. */
  @Test
  public void testRecoverWithUncorrectTimePartionDir() {
    try {
      File timePartitionDir = new File(SEQ_FILE_DIR);
      File f = new File(timePartitionDir.getParent() + File.separator + "test.tmp");
      f.createNewFile();
      new VirtualStorageGroupProcessor(
          TestConstant.BASE_OUTPUT_PATH + File.separator + "data" + File.separator + "sequence",
          "0",
          new TsFileFlushPolicy.DirectFlushPolicy(),
          COMPACTION_TEST_SG);
    } catch (StorageGroupProcessorException | IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Test a compaction task in finished. The compaction log use file info to record files. The
   * sources file are all existed.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithCompleteTargetFileUsingFileInfo() throws Exception {
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          new TsFileResource(
              new File(
                  SEQ_FILE_DIR
                      + File.separator.concat(
                          i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + ".tsfile")));
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      sourceFiles.add(tsFileResource);
    }
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
    SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
    for (TsFileResource resource : sourceFiles) {
      logger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, resource.getTsFile());
    }
    logger.logFileInfo(SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
    logger.close();
    InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    SizeTieredCompactionRecoverTask recoverTask =
        new SizeTieredCompactionRecoverTask(
            COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
    recoverTask.doCompaction();
    // all the source file should still exist
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  /**
   * Test a compaction task in not finished. The compaction log use file info to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithIncompleteTargetFileUsingFileInfo() throws Exception {
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          new TsFileResource(
              new File(
                  SEQ_FILE_DIR
                      + File.separator.concat(
                          i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + ".tsfile")));
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      sourceFiles.add(tsFileResource);
    }
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
    SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
    for (TsFileResource resource : sourceFiles) {
      logger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, resource.getTsFile());
    }
    logger.logFileInfo(SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
    logger.close();
    InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    SizeTieredCompactionRecoverTask recoverTask =
        new SizeTieredCompactionRecoverTask(
            COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
    recoverTask.doCompaction();
    // all the source file should be deleted
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  /**
   * Test a compaction task in finished. The compaction log use file path to record files. All the
   * sources file is still existed.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithCompleteTargetFileUsingFilePath() throws Exception {
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          new TsFileResource(
              new File(
                  SEQ_FILE_DIR
                      + File.separator.concat(
                          i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + ".tsfile")));
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      sourceFiles.add(tsFileResource);
    }
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
    SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
    for (TsFileResource resource : sourceFiles) {
      logger.logFile(SizeTieredCompactionLogger.SOURCE_NAME, resource.getTsFile());
    }
    logger.logFile(SizeTieredCompactionLogger.TARGET_NAME, targetResource.getTsFile());
    logger.close();
    InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    SizeTieredCompactionRecoverTask recoverTask =
        new SizeTieredCompactionRecoverTask(
            COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
    recoverTask.doCompaction();
    // all the source file should still exist
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  /**
   * Test a compaction task in not finished. The compaction log use file path to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithIncompleteTargetFileUsingFilePath() throws Exception {
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          new TsFileResource(
              new File(
                  SEQ_FILE_DIR
                      + File.separator.concat(
                          i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + i
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + ".tsfile")));
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      sourceFiles.add(tsFileResource);
    }
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
    TsFileResource targetResource =
        new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
    SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
    for (TsFileResource resource : sourceFiles) {
      logger.logFile(SizeTieredCompactionLogger.SOURCE_NAME, resource.getTsFile());
    }
    logger.logFile(SizeTieredCompactionLogger.TARGET_NAME, targetResource.getTsFile());
    logger.close();
    InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
    InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    SizeTieredCompactionRecoverTask recoverTask =
        new SizeTieredCompactionRecoverTask(
            COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
    recoverTask.doCompaction();
    // all the source file should be deleted
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  /**
   * Test a compaction task is finished, and the data dirs of the system is change. The compaction
   * log use file info to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithCompleteTargetFileUsingFileInfoAndChangingDataDirs() throws Exception {
    try {
      List<TsFileResource> sourceFiles = new ArrayList<>();
      List<String> sourceFileNames = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            new TsFileResource(
                new File(
                    SEQ_FILE_DIR
                        + File.separator.concat(
                            i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile")));
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        sourceFiles.add(tsFileResource);
        sourceFileNames.add(tsFileResource.getTsFile().getName());
      }
      String targetFileName =
          TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
      TsFileResource targetResource =
          new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
      SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
      for (TsFileResource resource : sourceFiles) {
        logger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, resource.getTsFile());
      }
      logger.logFileInfo(SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
      logger.close();
      InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
      InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
      long sizeOfTargetFile = targetResource.getTsFileSize();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[] {TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"});
      SizeTieredCompactionRecoverTask recoverTask =
          new SizeTieredCompactionRecoverTask(
              COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
      recoverTask.doCompaction();
      // all the source files should exist
      for (String sourceFileName : sourceFileNames) {
        Assert.assertTrue(
            new File(
                    TestConstant.BASE_OUTPUT_PATH
                        + File.separator
                        + "data1"
                        + File.separator
                        + "sequence"
                        + File.separator
                        + COMPACTION_TEST_SG
                        + File.separator
                        + "0"
                        + File.separator
                        + "0",
                    sourceFileName)
                .exists());
      }
      File targetFileAfterMoved =
          new File(
              TestConstant.BASE_OUTPUT_PATH
                  + File.separator
                  + "data1"
                  + File.separator
                  + "sequence"
                  + File.separator
                  + COMPACTION_TEST_SG
                  + File.separator
                  + "0"
                  + File.separator
                  + "0",
              targetFileName.replace(
                  IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
      Assert.assertFalse(targetFileAfterMoved.exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  /**
   * Test a compaction task is not finished, and the data dirs of the system is change The
   * compaction log use file info to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithIncompleteTargetFileUsingFileInfoAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = new ArrayList<>();
      List<String> sourceFileNames = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            new TsFileResource(
                new File(
                    SEQ_FILE_DIR
                        + File.separator.concat(
                            i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile")));
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        sourceFiles.add(tsFileResource);
        sourceFileNames.add(tsFileResource.getTsFile().getName());
      }
      String targetFileName =
          TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
      TsFileResource targetResource =
          new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
      SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
      for (TsFileResource resource : sourceFiles) {
        logger.logFileInfo(SizeTieredCompactionLogger.SOURCE_INFO, resource.getTsFile());
      }
      logger.logFileInfo(SizeTieredCompactionLogger.TARGET_INFO, targetResource.getTsFile());
      logger.close();
      InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
      InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[] {TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"});
      SizeTieredCompactionRecoverTask recoverTask =
          new SizeTieredCompactionRecoverTask(
              COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
      recoverTask.doCompaction();
      // all the source file should be deleted
      for (String sourceFileName : sourceFileNames) {
        Assert.assertTrue(
            new File(
                    TestConstant.BASE_OUTPUT_PATH
                        + File.separator
                        + "data1"
                        + File.separator
                        + "sequence"
                        + File.separator
                        + COMPACTION_TEST_SG
                        + File.separator
                        + "0"
                        + File.separator
                        + "0",
                    sourceFileName)
                .exists());
      }
      Assert.assertFalse(
          new File(
                  TestConstant.BASE_OUTPUT_PATH
                      + File.separator
                      + "data1"
                      + File.separator
                      + "sequence"
                      + File.separator
                      + COMPACTION_TEST_SG
                      + File.separator
                      + "0"
                      + File.separator
                      + "0",
                  targetFileName)
              .exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  /**
   * Test a compaction task is finished, and the data dirs of the system is change. The compaction
   * log use file path to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithCompleteTargetFileUsingFilePathAndChangingDataDirs() throws Exception {
    try {
      List<TsFileResource> sourceFiles = new ArrayList<>();
      List<String> sourceFileNames = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            new TsFileResource(
                new File(
                    SEQ_FILE_DIR
                        + File.separator.concat(
                            i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile")));
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        sourceFiles.add(tsFileResource);
        sourceFileNames.add(tsFileResource.getTsFile().getName());
      }
      String targetFileName =
          TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
      TsFileResource targetResource =
          new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
      SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
      for (TsFileResource resource : sourceFiles) {
        logger.logFile(SizeTieredCompactionLogger.SOURCE_NAME, resource.getTsFile());
      }
      logger.logFile(SizeTieredCompactionLogger.TARGET_NAME, targetResource.getTsFile());
      logger.close();
      InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
      InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
      long sizeOfTargetFile = targetResource.getTsFileSize();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[] {TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"});
      SizeTieredCompactionRecoverTask recoverTask =
          new SizeTieredCompactionRecoverTask(
              COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
      recoverTask.doCompaction();
      // all the source files should exist
      for (String sourceFileName : sourceFileNames) {
        Assert.assertTrue(
            new File(
                    TestConstant.BASE_OUTPUT_PATH
                        + File.separator
                        + "data1"
                        + File.separator
                        + "sequence"
                        + File.separator
                        + COMPACTION_TEST_SG
                        + File.separator
                        + "0"
                        + File.separator
                        + "0",
                    sourceFileName)
                .exists());
      }
      File targetFileAfterMoved =
          new File(
              TestConstant.BASE_OUTPUT_PATH
                  + File.separator
                  + "data1"
                  + File.separator
                  + "sequence"
                  + File.separator
                  + COMPACTION_TEST_SG
                  + File.separator
                  + "0"
                  + File.separator
                  + "0",
              targetFileName.replace(
                  IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
      Assert.assertFalse(targetFileAfterMoved.exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  /**
   * Test a compaction task is not finished, and the data dirs of the system is change. The
   * compaction log use file path to record files.
   *
   * @throws Exception
   */
  @Test
  public void testRecoverWithIncompleteTargetFileUsingFilePathAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = new ArrayList<>();
      List<String> sourceFileNames = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            new TsFileResource(
                new File(
                    SEQ_FILE_DIR
                        + File.separator.concat(
                            i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + i
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile")));
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        sourceFiles.add(tsFileResource);
        sourceFileNames.add(tsFileResource.getTsFile().getName());
      }
      String targetFileName =
          TsFileNameGenerator.getInnerCompactionFileName(sourceFiles, true).getName();
      TsFileResource targetResource =
          new TsFileResource(new File(SEQ_FILE_DIR + File.separator + targetFileName));
      SizeTieredCompactionLogger logger = new SizeTieredCompactionLogger(logFilePath);
      for (TsFileResource resource : sourceFiles) {
        logger.logFile(SizeTieredCompactionLogger.SOURCE_NAME, resource.getTsFile());
      }
      logger.logFile(SizeTieredCompactionLogger.TARGET_NAME, targetResource.getTsFile());
      logger.close();
      InnerSpaceCompactionUtils.compact(targetResource, sourceFiles, COMPACTION_TEST_SG, true);
      InnerSpaceCompactionUtils.moveTargetFile(targetResource, COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[] {TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"});
      SizeTieredCompactionRecoverTask recoverTask =
          new SizeTieredCompactionRecoverTask(
              COMPACTION_TEST_SG, "0", 0, new File(logFilePath), "", true, new AtomicInteger(0));
      recoverTask.doCompaction();
      // all the source file should be deleted
      for (String sourceFileName : sourceFileNames) {
        Assert.assertTrue(
            new File(
                    TestConstant.BASE_OUTPUT_PATH
                        + File.separator
                        + "data1"
                        + File.separator
                        + "sequence"
                        + File.separator
                        + COMPACTION_TEST_SG
                        + File.separator
                        + "0"
                        + File.separator
                        + "0",
                    sourceFileName)
                .exists());
      }
      Assert.assertFalse(
          new File(
                  TestConstant.BASE_OUTPUT_PATH
                      + File.separator
                      + "data1"
                      + File.separator
                      + "sequence"
                      + File.separator
                      + COMPACTION_TEST_SG
                      + File.separator
                      + "0"
                      + File.separator
                      + "0",
                  targetFileName)
              .exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }
}
