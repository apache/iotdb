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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
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
import static org.junit.Assert.fail;

public class CompactionSchedulerTest {
  static final Logger LOGGER = LoggerFactory.getLogger(CompactionSchedulerTest.class);
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final long MAX_WAITING_TIME = 240_000;
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
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    CompactionTaskManager.getInstance().stop();
    MergeManager.getINSTANCE().stop();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test1() throws IOException, IllegalPathException {
    LOGGER.warn("Running test1");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(2L * 1024L * 1024L * 1024L);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    LOGGER.warn("Schedule Compaction for time partition 0");
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (totalWaitingTime > MAX_WAITING_TIME) {
        fail();
        break;
      }
    }
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOGGER.warn("Try running cross space compaction");
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
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
    LOGGER.warn("Running test2");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOGGER.warn("Try running cross space compaction");
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        LOGGER.warn(
            "The size of unsequence file list is {}",
            tsFileResourceManager.getTsFileList(false).size());
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
    LOGGER.warn("Running test3");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(1, tsFileResourceManager.getTsFileList(true).size());

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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    LOGGER.warn("Waiting for cross space compaction");
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
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
    LOGGER.warn("Running test6");
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
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 1) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    LOGGER.warn("Waiting for cross space compaction");
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
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
    LOGGER.warn("Running test8");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 0) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
    LOGGER.warn("Running test9");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 50) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 75) {
      if (totalWaitingTime % 2000 == 0) {
        LOGGER.warn(
            "The size of unsequence file list is {}",
            tsFileResourceManager.getTsFileList(false).size());
        LOGGER.warn(
            "Current task num in compaction task manager is {}",
            CompactionTaskManager.getInstance().getTaskCount());
      }
      try {
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(25, tsFileResourceManager.getTsFileList(true).size());

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
    LOGGER.warn("Running test10");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 50) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 25) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
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
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test11() throws IOException, IllegalPathException {
    LOGGER.warn("Running test11");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 50) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 25) {
      if (totalWaitingTime % 2000 == 0) {
        LOGGER.warn(
            "The size of unsequence file list is {}",
            tsFileResourceManager.getTsFileList(false).size());
        LOGGER.warn(
            "Current task num in compaction task manager is {}",
            CompactionTaskManager.getInstance().getTaskCount());
      }
      try {
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
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
    LOGGER.warn("Running test12");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    LOGGER.warn("Try to schedule compaction");
    LOGGER.warn(
        "{} {}, current task num is {}",
        COMPACTION_TEST_SG,
        CompactionScheduler.isPartitionCompacting(COMPACTION_TEST_SG + "-0", 0)
            ? "is compacting"
            : "not compacting",
        CompactionTaskManager.getInstance().getTaskCount());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);

    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 98) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 96) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
    LOGGER.warn("Running test13");
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 99) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 98) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());

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
    LOGGER.warn("Running test14");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 99) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 98) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
    LOGGER.warn("Running test15");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 99) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(true).size() > 98) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of sequence file list is {}",
              tsFileResourceManager.getTsFileList(true).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());

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
    LOGGER.warn("Running test16");
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

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    long totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 98) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    totalWaitingTime = 0;
    while (tsFileResourceManager.getTsFileList(false).size() > 96) {
      try {
        if (totalWaitingTime % 2000 == 0) {
          LOGGER.warn(
              "The size of unsequence file list is {}",
              tsFileResourceManager.getTsFileList(false).size());
          LOGGER.warn(
              "Current task num in compaction task manager is {}",
              CompactionTaskManager.getInstance().getTaskCount());
        }
        Thread.sleep(100);
        totalWaitingTime += 100;
        if (totalWaitingTime > MAX_WAITING_TIME) {
          fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

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
