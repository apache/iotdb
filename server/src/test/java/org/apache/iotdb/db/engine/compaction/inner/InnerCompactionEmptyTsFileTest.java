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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class InnerCompactionEmptyTsFileTest extends InnerCompactionTest {

  File tempSGDir;

  @Before
  public void setUp() throws Exception {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    seqFileNum = 0;
    unseqFileNum = 4;
    super.setUp();
    CompactionTaskManager.getInstance().restart();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Override
  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    // create some empty tsfiles
    super.prepareFile(tsFileResource, 0, 0, 0);
  }

  @Test
  public void testCompactWithPartialEmptyUnseqFiles() throws Exception {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // Here we compact file 0-2
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources.subList(0, 3),
            false,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0),
            0);
    unseqResources.get(0).readLock();
    CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
    Future<CompactionTaskSummary> future =
        CompactionTaskManager.getInstance().getCompactionTaskFutureMayBlock(task);
    unseqResources.get(0).readUnlock();
    Assert.assertTrue(future.get().isSuccess());
  }
}
