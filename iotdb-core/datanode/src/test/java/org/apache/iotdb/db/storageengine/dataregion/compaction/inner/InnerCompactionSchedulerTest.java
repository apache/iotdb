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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class InnerCompactionSchedulerTest extends AbstractCompactionTest {

  private long originFileSize;
  long MAX_WAITING_TIME = 120_000L;
  boolean oldEnableSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  boolean oldEnableUnSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  int oldConcurrentCompactionThread =
      IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
  int oldMaxCompactionCandidateFileNum =
      IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    CompactionTaskManager.getInstance().start();
    super.setUp();
    originFileSize = IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(90);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionTaskManager.getInstance().stop();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(originFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(oldEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(oldEnableUnSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionThreadCount(oldConcurrentCompactionThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionCandidateFileNum(oldMaxCompactionCandidateFileNum);
    super.tearDown();
  }

  @Test
  public void testFileSelector1()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(4);
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1000000);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    registerTimeseriesInMManger(2, 3, false);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    registerTimeseriesInMManger(3, 5, false);
    createFiles(2, 5, 5, 50, 600, 800, 50, 50, false, true);
    registerTimeseriesInMManger(5, 5, false);
    TsFileManager tsFileManager = new TsFileManager("testSG", "0", "tmp");
    tsFileManager.addAll(seqResources, true);

    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask(
        tsFileManager, 0L, true, new CompactionScheduleContext());
    try {
      Thread.sleep(5000);
    } catch (Exception e) {

    }
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testFileSelector2()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(50);
    TsFileResourceList tsFileResources = new TsFileResourceList();
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    seqResources.get(0).setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    seqResources.get(0).setStatusForTest(TsFileResourceStatus.COMPACTING);
    TsFileManager tsFileManager = new TsFileManager("testSG", "0", "tmp");
    tsFileManager.addAll(seqResources, true);
    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask(
        tsFileManager, 0L, true, new CompactionScheduleContext());

    long waitingTime = 0;
    while (CompactionTaskManager.getInstance().getExecutingTaskCount() != 0) {
      try {
        Thread.sleep(100);
        waitingTime += 100;
        if (waitingTime > MAX_WAITING_TIME) {
          Assert.fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Assert.assertEquals(4, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testFileSelectorWithUnclosedFile()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(50);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(50);
    TsFileResourceList tsFileResources = new TsFileResourceList();
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    seqResources.get(3).setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileManager tsFileManager = new TsFileManager("testSG", "0", "tmp");
    tsFileManager.addAll(seqResources, true);
    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask(
        tsFileManager, 0L, true, new CompactionScheduleContext());
    long waitingTime = 0;
    while (CompactionTaskManager.getInstance().getExecutingTaskCount() != 0) {
      try {
        Thread.sleep(100);
        waitingTime += 100;
        if (waitingTime > MAX_WAITING_TIME) {
          Assert.fail();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Assert.assertEquals(4, tsFileManager.getTsFileList(true).size());
  }
}
