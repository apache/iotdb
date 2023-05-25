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

package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SizeTieredCompactionSelectorTest {
  @Test
  public void testSubmitWhenNextTimePartitionExists() {
    long originPartitionInterval =
        IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setTimePartitionInterval(1000000);
    List<TsFileResource> resources = new ArrayList<>();

    for (int i = 0; i < 100; ++i) {
      FakedTsFileResource resource =
          new FakedTsFileResource(1024, String.format("%d-%d-0-0.tsfile", i + 1, i + 1));
      resource.timeIndex.updateStartTime("root.test.d", i * 100);
      resource.timeIndex.updateEndTime("root.test.d", (i + 1) * 100);
      resource.timePartition = i / 10;
      resources.add(resource);
    }

    TsFileManager manager = new TsFileManager("root.test", "0", "");
    manager.addAll(resources, true);

    for (long i = 0; i < 9; ++i) {
      Assert.assertEquals(
          1,
          new SizeTieredCompactionSelector("root.test", "0", i, true, manager)
              .selectInnerSpaceTask(manager.getOrCreateSequenceListByTimePartition(i))
              .size());
    }

    Assert.assertEquals(
        0,
        new SizeTieredCompactionSelector("root.test", "0", 9, true, manager)
            .selectInnerSpaceTask(manager.getOrCreateSequenceListByTimePartition(9))
            .size());
  }

  @Test
  public void testSubmitWhenSequenceFileIsEmpty() throws Exception {
    DataRegion region = new DataRegion("root.test", "1");
    TsFileManager manager = region.getTsFileManager();
    int originCandidate =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(30);
    boolean enableUnseqCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    CompactionTaskManager.getInstance().start();
    try {
      for (int i = 1; i < 91; ++i) {
        TsFileResource resource = Mockito.mock(TsFileResource.class);
        Mockito.when(resource.getTimePartition()).thenReturn(0L);
        Mockito.when(resource.getTsFileSize()).thenReturn(100L);
        Mockito.when(resource.getTsFile())
            .thenReturn(new File(String.format("%d-%d-0-0.tsfile", i, i)));
        Mockito.when(resource.getStatus()).thenReturn(TsFileResourceStatus.NORMAL);
        manager.add(resource, false);
      }
      Assert.assertEquals(3, region.compact());
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setMaxInnerCompactionCandidateFileNum(originCandidate);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableUnseqSpaceCompaction(enableUnseqCompaction);
      CompactionTaskManager.getInstance().shutdown(60_000L);
    }
  }
}
