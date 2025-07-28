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

package org.apache.iotdb.db.storageengine.dataregion.compaction.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;

public class SizeTieredCompactionRecoverTest {
  private ICompactionPerformer performer = new FastCompactionPerformer(false);

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
  static final TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", TestConstant.BASE_OUTPUT_PATH);
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
  static final MeasurementSchema[] schemas = new MeasurementSchema[fullPaths.length];
  static String logFilePath =
      TestConstant.BASE_OUTPUT_PATH + File.separator + "test-compaction.compaction.log";
  static String[][] originDataDirs = null;
  static String[][] testDataDirs = new String[][] {{TestConstant.BASE_OUTPUT_PATH + "data"}};
  static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws Exception {
    CompactionTaskManager.getInstance().start();
    originDataDirs = config.getTierDataDirs();
    setDataDirs(testDataDirs);
    if (!new File(SEQ_FILE_DIR).exists()) {
      Assert.assertTrue(new File(SEQ_FILE_DIR).mkdirs());
    }
    if (!new File(UNSEQ_FILE_DIR).exists()) {
      Assert.assertTrue(new File(UNSEQ_FILE_DIR).mkdirs());
    }
    createTimeSeries();
  }

  @After
  public void tearDown() throws Exception {
    new CompactionConfigRestorer().restoreCompactionConfig();
    CompactionTaskManager.getInstance().stop();
    setDataDirs(originDataDirs);
    File dataDir = new File(testDataDirs[0][0]);
    if (dataDir.exists()) {
      FileUtils.forceDelete(dataDir);
    }
    File logFile = new File(logFilePath);
    if (logFile.exists()) {
      Assert.assertTrue(logFile.delete());
    }
    EnvironmentUtils.cleanEnv();
  }

  private void createTimeSeries() throws MetadataException {
    PartialPath[] deviceIds = new PartialPath[fullPaths.length];
    for (int i = 0; i < fullPaths.length; ++i) {
      schemas[i] =
          new MeasurementSchema(
              fullPaths[i].split("\\.")[3],
              TSDataType.INT64,
              TSEncoding.RLE,
              CompressionType.UNCOMPRESSED);
      deviceIds[i] = new PartialPath(fullPaths[i].substring(0, 27));
    }
  }

  public void setDataDirs(String[][] dataDirs) throws Exception {
    Class configClass = config.getClass();
    Field dataDirsField = configClass.getDeclaredField("tierDataDirs");
    dataDirsField.setAccessible(true);
    dataDirsField.set(config, dataDirs);
  }

  /** Test when a file that is not a directory exists under virtual storageGroup dir. */
  @Test
  public void testRecoverWithUncorrectTimePartionDir() throws StartupException {
    StorageEngine.getInstance().start();
    try {
      File timePartitionDir = new File(SEQ_FILE_DIR);
      File f = new File(timePartitionDir.getParent() + File.separator + "test.tmp");
      f.createNewFile();
      new DataRegion(
          TestConstant.BASE_OUTPUT_PATH + File.separator + "data" + File.separator + "sequence",
          "0",
          new TsFileFlushPolicy.DirectFlushPolicy(),
          COMPACTION_TEST_SG);
    } catch (DataRegionException | IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      StorageEngine.getInstance().stop();
    }
  }

  /**
   * Test a compaction task in finished. The compaction log use file info to record files. The
   * sources file are all existed.
   */
  private List<TsFileResource> getSourceFiles() throws IllegalPathException, IOException {
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
    return sourceFiles;
  }

  @Test
  public void testRecoverWithCompleteTargetFileUsingFileInfo() throws Exception {
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
    compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
    compactionLogger.logFile(targetResource, STR_TARGET_FILES);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);

    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(
            COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
    recoverTask.doCompaction();
    // all the source file should still exist
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  @Test
  public void testInnerRecoverWithCompleteTargetFileUsingFileInfo() throws Exception {
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
    compactionLogger.logSourceFiles(sourceFiles);
    compactionLogger.logTargetFile(targetResource);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);

    InnerSpaceCompactionTask innerSpaceCompactionTask =
        new InnerSpaceCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
    innerSpaceCompactionTask.recover();
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
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
    compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(
            COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
    recoverTask.doCompaction();
    // all the source file should be deleted
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  @Test
  public void testInnerRecoverWithIncompleteTargetFileUsingFileInfo() throws Exception {
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
    compactionLogger.logSourceFiles(sourceFiles);
    compactionLogger.logTargetFile(targetResource);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    InnerSpaceCompactionTask recoverTask =
        new InnerSpaceCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
    recoverTask.recover();
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
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    CompactionLogger logger = new CompactionLogger(new File(logFilePath));
    logger.logFiles(sourceFiles, CompactionLogger.STR_SOURCE_FILES);
    logger.logFiles(Collections.singletonList(targetResource), CompactionLogger.STR_TARGET_FILES);
    logger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(
            COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
    recoverTask.doCompaction();
    // all the source file should still exist
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  @Test
  public void testInnerRecoverWithCompleteTargetFileUsingFilePath() throws Exception {
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    SimpleCompactionLogger logger = new SimpleCompactionLogger(new File(logFilePath));
    logger.logSourceFiles(sourceFiles);
    logger.logTargetFile(targetResource);
    logger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    InnerSpaceCompactionTask innerSpaceCompactionTask =
        new InnerSpaceCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
    innerSpaceCompactionTask.recover();
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
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
    compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
    compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    CompactionRecoverTask recoverTask =
        new CompactionRecoverTask(
            COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
    recoverTask.doCompaction();
    // all the source file should be deleted
    for (TsFileResource resource : sourceFiles) {
      Assert.assertTrue(resource.getTsFile().exists());
    }
    Assert.assertFalse(targetResource.getTsFile().exists());
  }

  @Test
  public void testInnerRecoverWithIncompleteTargetFileUsingFilePath() throws Exception {
    List<TsFileResource> sourceFiles = getSourceFiles();
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
    SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
    compactionLogger.logSourceFiles(sourceFiles);
    compactionLogger.logTargetFile(targetResource);
    compactionLogger.close();
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
    FileChannel channel = targetStream.getChannel();
    channel.truncate(targetResource.getTsFile().length() - 100);
    channel.close();
    InnerSpaceCompactionTask innerSpaceCompactionTask =
        new InnerSpaceCompactionTask(COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
    innerSpaceCompactionTask.recover();
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
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
      compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
      compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      CompactionRecoverTask recoverTask =
          new CompactionRecoverTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
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
              targetResource
                  .getTsFile()
                  .getName()
                  .replace(
                      IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
      Assert.assertFalse(targetFileAfterMoved.exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  @Test
  public void testInnerRecoverWithCompleteTargetFileUsingFileInfoAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      InnerSpaceCompactionTask recoverTask =
          new InnerSpaceCompactionTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
      recoverTask.recover();
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
              targetResource
                  .getTsFile()
                  .getName()
                  .replace(
                      IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
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
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
      compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
      compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      CompactionRecoverTask recoverTask =
          new CompactionRecoverTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
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
                  targetResource.getTsFile().getName())
              .exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  @Test
  public void testInnerRecoverWithIncompleteTargetFileUsingFileInfoAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      InnerSpaceCompactionTask recoverTask =
          new InnerSpaceCompactionTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
      recoverTask.recover();
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
                  targetResource.getTsFile().getName())
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
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
      compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
      compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      CompactionRecoverTask recoverTask =
          new CompactionRecoverTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
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
              targetResource
                  .getTsFile()
                  .getName()
                  .replace(
                      IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
      Assert.assertFalse(targetFileAfterMoved.exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  @Test
  public void testInnerRecoverWithCompleteTargetFileUsingFilePathAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      InnerSpaceCompactionTask recoverTask =
          new InnerSpaceCompactionTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
      recoverTask.recover();
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
              targetResource
                  .getTsFile()
                  .getName()
                  .replace(
                      IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
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
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      CompactionLogger compactionLogger = new CompactionLogger(new File(logFilePath));
      compactionLogger.logFiles(sourceFiles, STR_SOURCE_FILES);
      compactionLogger.logFiles(Collections.singletonList(targetResource), STR_TARGET_FILES);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      CompactionRecoverTask recoverTask =
          new CompactionRecoverTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath), true);
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
                  targetResource.getTsFile().getName())
              .exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }

  @Test
  public void testInnerRecoverWithIncompleteTargetFileUsingFilePathAndChangingDataDirs()
      throws Exception {
    try {
      List<TsFileResource> sourceFiles = getSourceFiles();
      List<String> sourceFileNames = new ArrayList<>();
      sourceFiles.forEach(f -> sourceFileNames.add(f.getTsFile().getName()));
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(new File(logFilePath));
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.close();
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(
          Collections.singletonList(targetResource),
          CompactionTaskType.INNER_SEQ,
          COMPACTION_TEST_SG);
      FileOutputStream targetStream = new FileOutputStream(targetResource.getTsFile(), true);
      FileChannel channel = targetStream.getChannel();
      channel.truncate(targetResource.getTsFile().length() - 100);
      channel.close();
      FileUtils.moveDirectory(
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data"),
          new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
      setDataDirs(new String[][] {{TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"}});
      InnerSpaceCompactionTask recoverTask =
          new InnerSpaceCompactionTask(
              COMPACTION_TEST_SG, "0", tsFileManager, new File(logFilePath));
      recoverTask.recover();
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
                  targetResource.getTsFile().getName())
              .exists());
    } finally {
      FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "data1"));
    }
  }
}
