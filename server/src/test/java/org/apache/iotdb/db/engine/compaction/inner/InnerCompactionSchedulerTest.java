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
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.task.FakedInnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class InnerCompactionSchedulerTest {

  private long originFileSize;
  long MAX_WAITING_TIME = 120_000L;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    CompactionTaskManager.getInstance().start();
    originFileSize = IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(90);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionTaskManager.getInstance().stop();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(originFileSize);
  }

  @Test
  public void testFileSelector1() {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(50);
    TsFileResourceList tsFileResources = new TsFileResourceList();
    tsFileResources.add(new FakedTsFileResource(30, "0-0-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(30, "1-1-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(30, "2-2-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(100, "3-3-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(30, "4-4-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(40, "5-5-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(40, "6-6-0-0.tsfile"));

    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask(
        "testSG",
        "0",
        0L,
        new TsFileManager("testSG", "0", "tmp"),
        tsFileResources,
        true,
        new FakedInnerSpaceCompactionTaskFactory());
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();

    try {
      Thread.sleep(1000);
    } catch (Exception e) {

    }
    Assert.assertEquals(3, tsFileResources.size());

    List<TsFileResource> resources = tsFileResources.getArrayList();
    Assert.assertEquals(90L, resources.get(0).getTsFileSize());
    Assert.assertEquals(100L, resources.get(1).getTsFileSize());
    Assert.assertEquals(110L, resources.get(2).getTsFileSize());
  }

  @Test
  public void testFileSelector2() {
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(50);
    TsFileResourceList tsFileResources = new TsFileResourceList();
    tsFileResources.add(new FakedTsFileResource(30, "0-0-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(40, true, true, "1-1-0-0.tsfile"));
    tsFileResources.add(new FakedTsFileResource(40, "2-2-0-0.tsfile"));
    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask(
        "testSG",
        "0",
        0L,
        new TsFileManager("testSG", "0", "tmp"),
        tsFileResources,
        true,
        new FakedInnerSpaceCompactionTaskFactory());
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();

    long waitingTime = 0;
    while (CompactionTaskManager.getInstance().getTaskCount() != 0) {
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
    Assert.assertEquals(3, tsFileResources.size());

    List<TsFileResource> resources = tsFileResources.getArrayList();
    Assert.assertEquals(30L, resources.get(0).getTsFileSize());
    Assert.assertEquals(40L, resources.get(1).getTsFileSize());
    Assert.assertEquals(40L, resources.get(2).getTsFileSize());
  }
}
