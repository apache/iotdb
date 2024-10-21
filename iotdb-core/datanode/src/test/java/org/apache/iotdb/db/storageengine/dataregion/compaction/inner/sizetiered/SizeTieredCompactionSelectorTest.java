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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner.sizetiered;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.FakedTsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SizeTieredCompactionSelectorTest {
  @Test
  public void testSubmitWhenNextTimePartitionExists() {
    long originPartitionInterval =
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(1000000);
    List<TsFileResource> resources = new ArrayList<>();

    for (int i = 0; i < 100; ++i) {
      FakedTsFileResource resource =
          new FakedTsFileResource(1024, String.format("%d-%d-0-0.tsfile", i + 1, i + 1));
      resource.timeIndex.updateStartTime(
          IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d"), i * 100);
      resource.timeIndex.updateEndTime(
          IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d"), (i + 1) * 100);
      resource.timePartition = i / 10;
      resources.add(resource);
    }

    TsFileManager manager = new TsFileManager("root.test", "0", "");
    manager.addAll(resources, true);

    for (long i = 0; i < 9; ++i) {
      Assert.assertEquals(
          1,
          new SizeTieredCompactionSelector(
                  "root.test", "0", i, true, manager, new CompactionScheduleContext())
              .selectInnerSpaceTask(manager.getOrCreateSequenceListByTimePartition(i))
              .size());
    }

    Assert.assertEquals(
        0,
        new SizeTieredCompactionSelector(
                "root.test", "0", 9, true, manager, new CompactionScheduleContext())
            .selectInnerSpaceTask(manager.getOrCreateSequenceListByTimePartition(9))
            .size());
  }
}
