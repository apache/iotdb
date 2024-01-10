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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class RepairUnsortedFileCompactionTest extends AbstractCompactionTest {

  private boolean enableSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  private boolean enableUnSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  private boolean enableCrossSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    super.tearDown();
  }

  @Test
  public void testRepairUnsortedDataBetweenPageWithNonAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(5, 30)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(resource));
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, resource.isSeq(), 0);
    task.start();
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
  }

  @Test
  public void testRepairUnsortedDataBetweenPageWithAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(5, 30)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(resource));
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, resource.isSeq(), 0);
    task.start();
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
  }

  @Test
  public void testRepairUnsortedDataInOnePageWithNonAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(resource));
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, resource.isSeq(), 0);
    task.start();
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
  }

  @Test
  public void testRepairUnsortedDataInOnePageWithUnseqFile() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(false);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(resource));
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, resource.isSeq(), 0);
    task.start();
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
  }

  @Test
  public void testRepairUnsortedDataInOnePageWithAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(resource));
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, resource.isSeq(), 0);
    task.start();
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0)));
  }

  @Test
  public void testMarkFileAndRepairWithInnerSeqSpaceCompactionTask() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    TsFileResource seqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(5, 30)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            Arrays.asList(seqResource1, seqResource2),
            true,
            new ReadChunkCompactionPerformer(),
            0);
    Assert.assertFalse(task.start());

    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      Assert.assertEquals(resource.getTsFileRepairStatus(), TsFileRepairStatus.NEED_TO_REPAIR);
    }

    long initialFinishedCompactionTaskNum =
        CompactionTaskManager.getInstance().getFinishedTaskNum();
    CompactionScheduleSummary summary = new CompactionScheduleSummary();
    CompactionScheduler.scheduleCompaction(tsFileManager, 0, summary);
    Assert.assertEquals(2, summary.getSubmitSeqInnerSpaceCompactionTaskNum());

    int waitSecond = 20;
    while (CompactionTaskManager.getInstance().getFinishedTaskNum()
            - initialFinishedCompactionTaskNum
        < 2) {
      if (waitSecond == 0) {
        Assert.fail("Exceed the max time to wait repair compaction");
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        waitSecond--;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      TsFileResourceUtils.validateTsFileDataCorrectness(resource);
    }
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0)));
  }

  @Test
  public void testMarkFileAndRepairWithInnerUnSeqSpaceCompactionTask() throws IOException {
    TsFileResource unSeqResource1 = createEmptyFileAndResource(false);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unSeqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    TsFileResource unSeqResource2 = createEmptyFileAndResource(false);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unSeqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(5, 30)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            Arrays.asList(unSeqResource1, unSeqResource2),
            false,
            new FastCompactionPerformer(false),
            0);
    Assert.assertFalse(task.start());

    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      Assert.assertEquals(resource.getTsFileRepairStatus(), TsFileRepairStatus.NEED_TO_REPAIR);
    }

    long initialFinishedCompactionTaskNum =
        CompactionTaskManager.getInstance().getFinishedTaskNum();
    CompactionScheduleSummary summary = new CompactionScheduleSummary();
    CompactionScheduler.scheduleCompaction(tsFileManager, 0, summary);
    Assert.assertEquals(2, summary.getSubmitUnseqInnerSpaceCompactionTaskNum());

    int waitSecond = 20;
    while (CompactionTaskManager.getInstance().getFinishedTaskNum()
            - initialFinishedCompactionTaskNum
        < 2) {
      if (waitSecond == 0) {
        Assert.fail("Exceed the max time to wait repair compaction");
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        waitSecond--;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      TsFileResourceUtils.validateTsFileDataCorrectness(resource);
    }
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0)));
  }

  @Test
  public void testMarkFileAndRepairWithCrossSpaceCompactionTask() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)}
            }
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    TsFileResource unSeqResource1 = createEmptyFileAndResource(false);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unSeqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(5, 30)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            Collections.singletonList(seqResource1),
            Collections.singletonList(unSeqResource1),
            new FastCompactionPerformer(true),
            0,
            0);
    Assert.assertFalse(task.start());

    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      Assert.assertEquals(resource.getTsFileRepairStatus(), TsFileRepairStatus.NEED_TO_REPAIR);
    }

    long initialFinishedCompactionTaskNum =
        CompactionTaskManager.getInstance().getFinishedTaskNum();
    CompactionScheduleSummary summary = new CompactionScheduleSummary();
    CompactionScheduler.scheduleCompaction(tsFileManager, 0, summary);
    Assert.assertEquals(1, summary.getSubmitSeqInnerSpaceCompactionTaskNum());
    Assert.assertEquals(1, summary.getSubmitUnseqInnerSpaceCompactionTaskNum());

    int waitSecond = 20;
    while (CompactionTaskManager.getInstance().getFinishedTaskNum()
            - initialFinishedCompactionTaskNum
        < 2) {
      if (waitSecond == 0) {
        Assert.fail("Exceed the max time to wait repair compaction");
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        waitSecond--;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      TsFileResourceUtils.validateTsFileDataCorrectness(resource);
    }
  }
}
