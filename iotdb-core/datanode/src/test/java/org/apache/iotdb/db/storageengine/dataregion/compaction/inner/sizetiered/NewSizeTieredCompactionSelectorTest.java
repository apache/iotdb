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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;

import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class NewSizeTieredCompactionSelectorTest extends AbstractCompactionTest {

  private long defaultTargetCompactionFileSize = IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
  private long defaultTotalCompactionFileSize = IoTDBDescriptor.getInstance().getConfig().getInnerCompactionTotalFileSizeThreshold();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(defaultTargetCompactionFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileSizeThreshold(defaultTotalCompactionFileSize);
  }

  @Test
  public void testSelectAllFiles() throws IOException {
    for (int i = 0; i < 10; i++) {
      TsFileResource resource = generateSingleNonAlignedSeriesFile(String.format("%d-%d-0-0.tsfile", i, i), "d" + i, new TimeRange[]{new TimeRange(100 * i, 100 * (i + 1))}, true);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(10, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(10, task.getAllSourceTsFiles().size());
  }

  @Test
  public void testSkipSomeFiles() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(100t);
    for (int i = 0; i < 10; i++) {
      if (i == 9) {
        TsFileResource resource = generateSingleNonAlignedSeriesFile(String.format("%d-%d-0-0.tsfile", i, i), "d" + 0, new TimeRange[]{new TimeRange(100 * i, 100 * (i + 1))}, true);
        seqResources.add(resource);
        continue;
      }
      TsFileResource resource = generateSingleNonAlignedSeriesFile(String.format("%d-%d-0-0.tsfile", i, i), "d" + i, new TimeRange[]{new TimeRange(100 * i, 100 * (i + 1))}, true);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(2, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(10, task.getAllSourceTsFiles().size());
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String fileName,
      String device,
      TimeRange[] chunkTimeRanges,
      boolean isSeq)
      throws IOException {
    TsFileResource resource = createEmptyFileAndResourceWithName(fileName, 0, isSeq);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup(device);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", chunkTimeRanges, TSEncoding.RLE, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    tsFileManager.keepOrderInsert(resource, isSeq);
    return resource;
  }
}
