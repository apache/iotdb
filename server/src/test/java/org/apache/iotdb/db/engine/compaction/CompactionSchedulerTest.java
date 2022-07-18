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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionSchedulerTest {
  private static final Logger logger = LoggerFactory.getLogger(CompactionSchedulerTest.class);
  static final String COMPACTION_TEST_SG = "root.compactionSchedulerTest_";
  private static final boolean oldEnableInnerSeqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  private static final boolean oldEnableInnerUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  private static final boolean oldEnableCrossCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
  static final long MAX_WAITING_TIME = 60_000;
  static final long SCHEDULE_AGAIN_TIME = 30_000;
  static final String[] fullPaths =
      new String[] {
        ".device0.sensor0",
        ".device0.sensor1",
        ".device0.sensor2",
        ".device0.sensor3",
        ".device0.sensor4",
        ".device0.sensor5",
        ".device0.sensor6",
        ".device0.sensor7",
        ".device0.sensor8",
        ".device0.sensor9",
        ".device1.sensor0",
        ".device1.sensor1",
        ".device1.sensor2",
        ".device1.sensor3",
        ".device1.sensor4",
      };

  @Before
  public void setUp() throws MetadataException, IOException {
    CompactionClearUtils.clearAllCompactionFiles();
    EnvironmentUtils.cleanAllDir();
    IoTDB.configManager.init();
    File basicOutputDir = new File(TestConstant.BASE_OUTPUT_PATH);
    IoTDBDescriptor.getInstance().getConfig().setCompactionPriority(CompactionPriority.INNER_CROSS);
    if (!basicOutputDir.exists()) {
      assertTrue(basicOutputDir.mkdirs());
    }
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    CompactionTaskManager.getInstance().start();
    while (CompactionTaskManager.getInstance().getExecutingTaskCount() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {

      }
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.configManager.clear();
    CompactionClearUtils.clearAllCompactionFiles();
    EnvironmentUtils.cleanAllDir();
    CompactionClearUtils.deleteEmptyDir(new File("target"));
    CompactionTaskManager.getInstance().stop();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(oldEnableInnerSeqCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(oldEnableInnerUnseqCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(oldEnableCrossCompaction);
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test1() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test1");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    String sgName = COMPACTION_TEST_SG + "test1";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      long totalWaitingTime = 0;

      while (tsFileManager.getTsFileList(true).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      }

      totalWaitingTime = 0;

      while (tsFileManager.getTsFileList(false).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);
          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);
      totalWaitingTime = 0;

      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);
          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test2() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test2");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCrossCompactionMemoryBudget(2 * 1024 * 1024L * 1024L);
    String sgName = COMPACTION_TEST_SG + "test2";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);
      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);
          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);
      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(10);
          totalWaitingTime += 10;
          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
          if (totalWaitingTime % 10_000 == 0) {
            logger.warn(
                "sequence file num is {}, unsequence file num is {}",
                tsFileManager.getTsFileList(true).size(),
                tsFileManager.getTsFileList(false).size());
          }
          if (totalWaitingTime % SCHEDULE_AGAIN_TIME == 0) {
            logger.warn("Has waited for {} s, Schedule again", totalWaitingTime / 1000);
            CompactionScheduler.scheduleCompaction(tsFileManager, 0);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      //      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test3() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test3");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test3";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(0, tsFileManager.getTsFileList(false).size());
      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(1, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test4() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test4");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    String sgName = COMPACTION_TEST_SG + "test4";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }
      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test5() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test5");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test5";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test6() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test6");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test6";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 1) {
        assertEquals(100, tsFileManager.getTsFileList(true).size());
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        assertEquals(100, tsFileManager.getTsFileList(true).size());
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test7() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test7");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test7";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 1) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test8() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test8");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test8";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test9() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test9");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test9";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 50) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 25) {
        Thread.sleep(100);
        totalWaitingTime += 100;
        CompactionScheduler.scheduleCompaction(tsFileManager, 0);

        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
        }
      }
      assertTrue(tsFileManager.getTsFileList(true).size() <= 25);
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {

      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test10() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test10");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    boolean prevEnableCrossCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test10";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 50) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 25) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableCrossSpaceCompaction(prevEnableCrossCompaction);
    }
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test11() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test11");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test11";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 50) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 25) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test12() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test12");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test12";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 98) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 96) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test14() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test14");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test13";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      assertEquals(100, tsFileManager.getTsFileList(true).size());
      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 99) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          assertEquals(100, tsFileManager.getTsFileList(true).size());
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 98) {
        try {
          Thread.sleep(100);
          assertEquals(100, tsFileManager.getTsFileList(true).size());
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test15() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test15");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test14";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 99) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(true).size() > 98) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      while (tsFileManager.getTsFileList(false).size() > 0) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test16() throws IOException, MetadataException, InterruptedException {
    logger.warn("Running test16");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test16";
    try {
      IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sgName));
    } catch (Exception e) {
      logger.error("exception occurs", e);
    }
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
        PartialPath path = new PartialPath(sgName + device);
        IoTDB.schemaProcessor.createTimeseries(
            path,
            TSDataType.INT64,
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }
      for (int i = 0; i < 100; i++) {

        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1, sgName);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
        tsFileManager.add(tsFileResource, false);
      }

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      long totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 98) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      CompactionScheduler.scheduleCompaction(tsFileManager, 0);

      totalWaitingTime = 0;
      while (tsFileManager.getTsFileList(false).size() > 96) {
        try {
          Thread.sleep(100);
          totalWaitingTime += 100;
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);

          if (totalWaitingTime > MAX_WAITING_TIME) {
            fail();
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      assertEquals(100, tsFileManager.getTsFileList(true).size());
      tsFileManager.setAllowCompaction(false);
      stopCompactionTaskManager();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setConcurrentCompactionThread(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  public void stopCompactionTaskManager() {
    CompactionTaskManager.getInstance().clearCandidateQueue();
    while (CompactionTaskManager.getInstance().getRunningCompactionTaskList().size() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
  }
}
