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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCrossSpaceCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.ReadChunkInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;
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
import java.util.ArrayList;
import java.util.List;

public class CompactionTaskMemCostEstimatorTest extends AbstractCompactionTest {

  int compactionBatchSize;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    compactionBatchSize =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(compactionBatchSize);
    super.tearDown();
  }

  @Test
  public void testEstimateReadChunkInnerSpaceCompactionTaskMemCost()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100000, 0, 0, 50, 50, true, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    System.out.println(tsFileList.get(0).getTsFile().getAbsolutePath());
    long cost = new ReadChunkInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateReadChunkInnerSpaceCompactionTaskMemCost2()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    long cost = new ReadChunkInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateFastCompactionInnerSpaceCompactionTaskMemCost()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(5, 10, 5, 10000, 0, 0, 50, 50, true, false);
    createFiles(10, 4, 5, 10000, 1000, 0, 30, 90, true, false);

    tsFileManager.addAll(unseqResources, false);
    long cost =
        new FastCompactionInnerCompactionEstimator().estimateInnerCompactionMemory(unseqResources);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateFastCompactionInnerSpaceCompactionTaskMemCost2()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    long cost =
        new FastCompactionInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateFastCompactionCrossSpaceCompactionTaskMemCost1()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    createFiles(4, 10, 5, 400, 0, 0, 30, 50, false, false);
    long cost =
        new FastCrossSpaceCompactionEstimator()
            .estimateCrossCompactionMemory(seqResources, unseqResources);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testRoughEstimate() throws IOException {
    boolean cacheEnabled = AbstractCompactionEstimator.isGlobalFileInfoCacheEnabled();
    if (!cacheEnabled) {
      AbstractCompactionEstimator.enableFileInfoCacheForTest(100, 100);
    }
    try {
      for (int i = 0; i < 10; i++) {
        TsFileResource resource = createEmptyFileAndResource(false);
        try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
          writer.startChunkGroup("d1");
          List<String> measurements = new ArrayList<>();
          for (int j = 0; j < 10; j++) {
            measurements.add("s" + j);
          }
          writer.generateSimpleAlignedSeriesToCurrentDevice(
              measurements,
              new TimeRange[] {new TimeRange(0, 10000)},
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED);
          writer.endChunkGroup();

          writer.startChunkGroup("d2");
          for (int j = 0; j < 10; j++) {
            writer.generateSimpleNonAlignedSeriesToCurrentDevice(
                "s" + j,
                new TimeRange[] {new TimeRange(0, 10000)},
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED);
          }
          writer.endChunkGroup();
          writer.endFile();
        }
        seqResources.add(resource);
      }
      NewSizeTieredCompactionSelector selector =
          new NewSizeTieredCompactionSelector(
              COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
      List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
          selector.selectInnerSpaceTask(seqResources);
      Assert.assertEquals(1, innerSpaceCompactionTasks.size());
      Assert.assertEquals(-1, innerSpaceCompactionTasks.get(0).getRoughMemoryCost());
      long estimatedMemoryCost = innerSpaceCompactionTasks.get(0).getEstimatedMemoryCost();
      Assert.assertTrue(estimatedMemoryCost > 0);

      innerSpaceCompactionTasks = selector.selectInnerSpaceTask(seqResources);
      Assert.assertEquals(1, innerSpaceCompactionTasks.size());
      Assert.assertTrue(innerSpaceCompactionTasks.get(0).getRoughMemoryCost() > 0);
    } finally {
      if (!cacheEnabled) {
        AbstractCompactionEstimator.disableFileInfoCacheForTest();
      }
    }
  }
}
