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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RepairUnsortedFileCompactionTest extends AbstractRepairDataTest {

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
    try {
      CompactionScheduleTaskManager.getInstance().start();
    } catch (StartupException e) {
      throw new RuntimeException(e);
    }
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
  public void testRepairUnsortedDataInOnePageWithMultiNonAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      for (int i = 0; i < 1000; i++) {
        writer.generateSimpleNonAlignedSeriesToCurrentDevice(
            "s" + i,
            new TimeRange[][][] {
              new TimeRange[][] {
                new TimeRange[] {
                  new TimeRange(10, 20), new TimeRange(29, 30), new TimeRange(21, 25)
                }
              }
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4);
      }
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
  public void testMarkFileAndRepairWithInnerSeqSpaceCompactionTask()
      throws IOException, InterruptedException {
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
  public void testMarkFileAndRepairWithInnerUnSeqSpaceCompactionTask()
      throws IOException, InterruptedException {
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
  public void testMarkFileAndRepairWithCrossSpaceCompactionTask()
      throws IOException, InterruptedException {
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

  @Test
  public void testRepairOverlapBetweenFile() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(25, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(20, 30), new TimeRange(35, 40)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, seqResource2, true, false, 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileDataCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourceCorrectness(
            tsFileManager.getTsFileList(false).get(0)));
  }

  @Test
  public void testRepairOverlapBetweenFileWithModFile() throws IOException, IllegalPathException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(25, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(20, 30), new TimeRange(35, 40)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    ModificationFile modFile = seqResource2.getModFile();
    Deletion writedModification =
        new Deletion(new PartialPath("root.testsg.d1.s1"), Long.MAX_VALUE, 15);
    modFile.write(writedModification);
    modFile.close();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, seqResource2, true, false, 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    TsFileResource targetResource = tsFileManager.getTsFileList(false).get(0);
    Assert.assertTrue(TsFileResourceUtils.validateTsFileDataCorrectness(targetResource));
    Assert.assertTrue(TsFileResourceUtils.validateTsFileResourceCorrectness(targetResource));
    Assert.assertTrue(targetResource.modFileExists());
    Assert.assertEquals(1, targetResource.getModFile().getModifications().size());
    Deletion modification =
        (Deletion) targetResource.getModFile().getModifications().iterator().next();
    Assert.assertEquals(writedModification.getFileOffset(), modification.getFileOffset());
    Assert.assertEquals(writedModification.getEndTime(), modification.getEndTime());
  }

  @Test
  public void testScheduleRepairInternalUnsortedFile() throws IOException {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn("0");
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(15, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(40, 50), new TimeRange(55, 60)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    tsFileManager.addAll(seqResources, true);
    Assert.assertTrue(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    File tempDir = getEmptyRepairDataLogDir();

    CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
    UnsortedFileRepairTaskScheduler scheduler =
        new UnsortedFileRepairTaskScheduler(
            Collections.singletonList(mockDataRegion), false, tempDir);
    scheduler.run();
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void testScheduleRepairOverlapFile() throws IOException {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn("0");
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(25, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 50), new TimeRange(55, 60)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    File tempDir = getEmptyRepairDataLogDir();

    CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
    UnsortedFileRepairTaskScheduler scheduler =
        new UnsortedFileRepairTaskScheduler(
            Collections.singletonList(mockDataRegion), false, tempDir);
    scheduler.run();
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void testScheduleRepairOverlapFileAndInternalUnsortedFile() throws IOException {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn("0");
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(15, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 50), new TimeRange(55, 60)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(40, 80)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    File tempDir = getEmptyRepairDataLogDir();

    CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
    UnsortedFileRepairTaskScheduler scheduler =
        new UnsortedFileRepairTaskScheduler(
            Collections.singletonList(mockDataRegion), false, tempDir);
    scheduler.run();
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void testRecoverRepairScheduleSkipRepairedTimePartitionAndMarkFile() throws IOException {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn("0");
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(15, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 50), new TimeRange(55, 60)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(40, 80)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResource3.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    File tempDir = getEmptyRepairDataLogDir();
    try (RepairLogger logger = new RepairLogger(tempDir, false)) {
      logger.recordRepairTaskStartTimeIfLogFileEmpty(System.currentTimeMillis());
      RepairTimePartition timePartition =
          new RepairTimePartition(mockDataRegion, 0, System.currentTimeMillis());
      // record seqResource3 as cannot recover
      logger.recordRepairedTimePartition(timePartition);
    }
    // reset the repair status
    seqResource3.setTsFileRepairStatus(TsFileRepairStatus.NORMAL);

    CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
    UnsortedFileRepairTaskScheduler scheduler =
        new UnsortedFileRepairTaskScheduler(
            Collections.singletonList(mockDataRegion), true, tempDir);
    scheduler.run();
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    // check whether the repair status is marked correctly
    Assert.assertEquals(TsFileRepairStatus.NEED_TO_REPAIR, seqResource3.getTsFileRepairStatus());
  }

  @Test
  public void testRecoverRepairScheduleSkipRepairedTimePartitionWithDeletedFile()
      throws IOException {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn("0");
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(15, 30)}},
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
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 50), new TimeRange(55, 60)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(40, 80)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResource3.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    tsFileManager.addAll(seqResources, true);
    Assert.assertFalse(TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(seqResources));

    File tempDir = getEmptyRepairDataLogDir();
    try (RepairLogger logger = new RepairLogger(tempDir, false)) {
      logger.recordRepairTaskStartTimeIfLogFileEmpty(System.currentTimeMillis());
      RepairTimePartition timePartition =
          new RepairTimePartition(mockDataRegion, 0, System.currentTimeMillis());
      // record seqResource3 as cannot recover
      logger.recordRepairedTimePartition(timePartition);
    }
    // reset the repair status
    seqResource3.setTsFileRepairStatus(TsFileRepairStatus.NORMAL);
    // resource3 is deleted
    tsFileManager.replace(
        Collections.singletonList(seqResource3),
        Collections.emptyList(),
        Collections.emptyList(),
        0);

    CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
    UnsortedFileRepairTaskScheduler scheduler =
        new UnsortedFileRepairTaskScheduler(
            Collections.singletonList(mockDataRegion), true, tempDir);
    scheduler.run();
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testTimePartitionFilterFiles() {
    DataRegion mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(mockDataRegion.getTimePartitions()).thenReturn(Collections.singletonList(0L));

    TsFileResource seqResource1 = createEmptyFileAndResourceWithName("100-1-0-0.tsfile", 0, true);
    TsFileResource seqResource2 = createEmptyFileAndResourceWithName("200-3-0-0.tsfile", 0, true);
    TsFileResource seqResource3 = createEmptyFileAndResourceWithName("300-5-0-0.tsfile", 0, true);
    TsFileResource unseqResource1 =
        createEmptyFileAndResourceWithName("101-2-0-0.tsfile", 0, false);
    TsFileResource unseqResource2 =
        createEmptyFileAndResourceWithName("201-4-0-0.tsfile", 0, false);
    TsFileResource unseqResource3 =
        createEmptyFileAndResourceWithName("301-6-0-0.tsfile", 0, false);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RepairTimePartition timePartition = new RepairTimePartition(mockDataRegion, 0, 250);
    Assert.assertEquals(4, timePartition.getAllFileSnapshot().size());
    Assert.assertEquals(2, timePartition.getSeqFileSnapshot().size());
    Assert.assertEquals(2, timePartition.getUnSeqFileSnapshot().size());
  }

  @Test
  public void testEstimateRepairCompactionMemory() throws IOException {
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
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, true, true, 0);
    Assert.assertTrue(task.getEstimatedMemoryCost() > 0);
    task = new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, true, false, 0);
    Assert.assertEquals(0, task.getEstimatedMemoryCost());
  }

  @Test
  public void testMergeAlignedSeriesPointWithSameTimestamp() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, false, false));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, true, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, true, true, 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(false).get(0);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(target.getTsFilePath())) {
      List<AlignedChunkMetadata> chunkMetadataList =
          reader.getAlignedChunkMetadata(new PlainDeviceID("root.testsg.d1"));
      for (AlignedChunkMetadata alignedChunkMetadata : chunkMetadataList) {
        ChunkMetadata timeChunkMetadata =
            (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
        Chunk timeChunk = reader.readMemChunk(timeChunkMetadata);
        List<Chunk> valueChunks = new ArrayList<>();
        for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
          Chunk valueChunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
          valueChunks.add(valueChunk);
        }
        AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks, null);
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          IPointReader pointReader = batchData.getBatchDataIterator();
          while (pointReader.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
            for (Object value : timeValuePair.getValues()) {
              if (value == null) {
                Assert.fail();
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testSplitChunk() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(100000, 300000)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairUnsortedFileCompactionTask task =
        new RepairUnsortedFileCompactionTask(0, tsFileManager, resource, true, true, 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(false).get(0);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(target.getTsFilePath())) {
      List<AlignedChunkMetadata> chunkMetadataList =
          reader.getAlignedChunkMetadata(new PlainDeviceID("root.testsg.d1"));
      Assert.assertEquals(3, chunkMetadataList.size());
    }
  }
}
