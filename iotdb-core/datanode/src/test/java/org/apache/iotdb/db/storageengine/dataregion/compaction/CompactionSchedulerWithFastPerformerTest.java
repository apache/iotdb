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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionSchedulerWithFastPerformerTest {
  private static final Logger logger =
      LoggerFactory.getLogger(CompactionSchedulerWithFastPerformerTest.class);
  static final String COMPACTION_TEST_SG = "root.compactionSchedulerTest_";
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
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    File basicOutputDir = new File(TestConstant.BASE_OUTPUT_PATH);

    IoTDBDescriptor.getInstance().getConfig().setCompactionPriority(CompactionPriority.BALANCE);
    if (!basicOutputDir.exists()) {
      assertTrue(basicOutputDir.mkdirs());
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCrossCompactionPerformer(CrossCompactionPerformer.FAST);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerSeqCompactionPerformer(InnerSeqCompactionPerformer.FAST);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerUnseqCompactionPerformer(InnerUnseqCompactionPerformer.FAST);
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
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
    CompactionTaskManager.getInstance().stop();
    new CompactionConfigRestorer().restoreCompactionConfig();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    CompactionClearUtils.clearAllCompactionFiles();
    EnvironmentUtils.cleanAllDir();
    CompactionClearUtils.deleteEmptyDir(new File("target"));
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    String sgName = COMPACTION_TEST_SG + "test1";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            2
                * 1024
                * 1024L
                * 1024L
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      String sgName = COMPACTION_TEST_SG + "test2";
      try {
        CompactionTaskManager.getInstance().restart();
        TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
        Set<String> fullPath = new HashSet<>();
        for (String device : fullPaths) {
          fullPath.add(sgName + device);
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
            .setCompactionThreadCount(prevCompactionConcurrentThread);
        IoTDBDescriptor.getInstance()
            .getConfig()
            .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
      }
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test3";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    String sgName = COMPACTION_TEST_SG + "test4";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test5";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test6";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test7";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(100);
    String sgName = COMPACTION_TEST_SG + "test8";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test9";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test10";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test11";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test12";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test13";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test14";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
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
        IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    String sgName = COMPACTION_TEST_SG + "test16";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
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
          .setCompactionThreadCount(prevCompactionConcurrentThread);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    }
  }

  @Test
  public void testLargeFileInLowerLevel() throws Exception {
    logger.warn("Running test16");
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    long originTargetSize = IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1024 * 1024);
    String sgName = COMPACTION_TEST_SG + "test17";
    try {
      CompactionTaskManager.getInstance().restart();
      TsFileManager tsFileManager = new TsFileManager(sgName, "0", "target");
      Set<String> fullPath = new HashSet<>();
      for (String device : fullPaths) {
        fullPath.add(sgName + device);
      }
      for (int i = 0; i < 10; i++) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(100L);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource tsFileResource =
            new TsFileResource(
                new File(
                    TestConstant.BASE_OUTPUT_PATH
                        .concat(File.separator)
                        .concat("sequence")
                        .concat(File.separator)
                        .concat(sgName)
                        .concat(File.separator)
                        .concat("0")
                        .concat(File.separator)
                        .concat("0")
                        .concat(File.separator)
                        .concat(
                            (i + 1)
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + (i + 1)
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 1
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile")));
        CompactionFileGeneratorUtils.writeTsFile(
            fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
        tsFileManager.add(tsFileResource, true);
      }

      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100000L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          new TsFileResource(
              new File(
                  TestConstant.BASE_OUTPUT_PATH
                      .concat(File.separator)
                      .concat("sequence")
                      .concat(File.separator)
                      .concat(sgName)
                      .concat(File.separator)
                      .concat("0")
                      .concat(File.separator)
                      .concat("0")
                      .concat(File.separator)
                      .concat(
                          11
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 11
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + IoTDBConstant.FILE_NAME_SEPARATOR
                              + 0
                              + ".tsfile")));
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * 10 + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);

      CompactionScheduler.scheduleCompaction(tsFileManager, 0);
      Thread.sleep(100);
      long sleepTime = 0;
      while (tsFileManager.getTsFileList(true).size() >= 2) {
        CompactionScheduler.scheduleCompaction(tsFileManager, 0);
        tsFileManager.readLock();
        List<TsFileResource> resources = tsFileManager.getTsFileList(true);
        int previousFileLevel =
            TsFileNameGenerator.getTsFileName(resources.get(0).getTsFile().getName())
                .getInnerCompactionCnt();
        boolean canMerge = false;
        for (int i = 1; i < resources.size(); i++) {
          int currentFileLevel =
              TsFileNameGenerator.getTsFileName(resources.get(i).getTsFile().getName())
                  .getInnerCompactionCnt();
          if (currentFileLevel == previousFileLevel) {
            canMerge = true;
            break;
          }
          previousFileLevel = currentFileLevel;
        }
        tsFileManager.readUnlock();
        if (!canMerge) {
          break;
        }
        Thread.sleep(100);
        sleepTime += 100;
        if (sleepTime >= 20_000) {
          fail();
        }
      }

      stopCompactionTaskManager();
      tsFileManager.setAllowCompaction(false);
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
      IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(originTargetSize);
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
