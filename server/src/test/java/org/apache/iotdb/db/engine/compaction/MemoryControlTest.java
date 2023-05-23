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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryControlTest {
  @Test
  public void testFailedToAllocateMemoryInCrossTask() throws Exception {
    List<TsFileResource> sequenceFiles = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      sequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)), TsFileResourceStatus.NORMAL));
    }
    List<TsFileResource> unsequenceFiles = new ArrayList<>();
    for (int i = 11; i <= 20; i++) {
      unsequenceFiles.add(
          new TsFileResource(
              new File(String.format("%d-%d-0-0.tsfile", i, i)), TsFileResourceStatus.NORMAL));
    }
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.getStorageGroupName()).thenReturn("root.sg");
    Mockito.when(tsFileManager.getDataRegionId()).thenReturn("1");
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0L,
            tsFileManager,
            sequenceFiles,
            unsequenceFiles,
            null,
            new AtomicInteger(0),
            1024L * 1024L * 1024L * 50L,
            0);
    boolean success = task.checkValidAndSetMerging();
    Assert.assertFalse(success);
    for (TsFileResource tsFileResource : sequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
    for (TsFileResource tsFileResource : unsequenceFiles) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
      Assert.assertTrue(tsFileResource.tryWriteLock());
    }
  }
}
