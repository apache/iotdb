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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionSchedulerTest {
  private static final Logger logger = LoggerFactory.getLogger(CompactionSchedulerTest.class);
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final long MAX_WAITING_TIME = 240_000;
  static final long SCHEDULE_AGAIN_TIME = 30_000;
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

  @Before
  public void setUp() throws MetadataException {
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String fullPath : fullPaths) {
      PartialPath path = new PartialPath(fullPath);
      IoTDB.metaManager.createTimeseries(
          path,
          TSDataType.INT64,
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
    MergeManager.getINSTANCE().start();
    CompactionTaskManager.getInstance().start();
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {

      }
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    MergeManager.getINSTANCE().stop();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    try {
      Thread.sleep(10_000);
    } catch (InterruptedException e) {

    }
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test1() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 1) {
      try {
        Thread.sleep(10);
        totalWaitingTime += 10;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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
        logger.warn("Has waited for  {} s, Schedule again", totalWaitingTime / 1000);
        CompactionScheduler.scheduleCompaction(tsFileManager, 0);
      }
    }
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 1) {
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
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test2() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    CompactionTaskManager.getInstance().restart();
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 1) {
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
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test3() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    CompactionTaskManager.getInstance().restart();
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 1) {
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

    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
    assertEquals(1, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test4() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    CompactionTaskManager.getInstance().restart();
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test5() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 1) {
      try {
        Thread.sleep(10);
        totalWaitingTime += 10;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
        if (totalWaitingTime % 10_000 == 0) {
          logger.warn(
              "The number of sequence tsfile is {}, {} is wanted",
              tsFileManager.getTsFileList(true).size(),
              1);
        }
        if (totalWaitingTime % SCHEDULE_AGAIN_TIME == 0) {
          logger.warn("Has waited for {} s, Schedule again", totalWaitingTime / 1000);
          CompactionScheduler.scheduleCompaction(tsFileManager, 0);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 1) {
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

    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test6() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    CompactionTaskManager.getInstance().restart();
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 1) {
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
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test7() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    CompactionTaskManager.getInstance().restart();
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 1) {
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
    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test8() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    while (CompactionTaskManager.getInstance().getTaskCount() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test9() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 50) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 25) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {

      }
      totalWaitingTime += 100;
      if (totalWaitingTime > MAX_WAITING_TIME) {
        fail();
      }
    }
    assertTrue(tsFileManager.getTsFileList(true).size() <= 25);

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test10() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 50) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 25) {
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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(prevEnableCrossCompaction);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test11() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 50) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 25) {
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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test12() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();

    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 98) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 96) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test13() throws IOException, IllegalPathException {
    logger.warn("Running test13");
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 99) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 98) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test14() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 99) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 98) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test15() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 99) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(true).size() > 98) {
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
    assertEquals(100, tsFileManager.getTsFileList(false).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test16() throws IOException, IllegalPathException {
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
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);
    CompactionTaskManager.getInstance().restart();

    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    long totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 98) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());
    while (CompactionTaskManager.currentTaskNum.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {

      }
    }
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    totalWaitingTime = 0;
    while (tsFileManager.getTsFileList(false).size() > 96) {
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
    assertEquals(100, tsFileManager.getTsFileList(true).size());

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
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
}
