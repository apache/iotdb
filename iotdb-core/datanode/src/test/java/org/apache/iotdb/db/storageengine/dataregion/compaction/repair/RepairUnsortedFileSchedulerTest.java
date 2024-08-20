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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class RepairUnsortedFileSchedulerTest extends AbstractRepairDataTest {

  private boolean enableSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  private boolean enableUnSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  private boolean enableCrossSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
  private String threadName = Thread.currentThread().getName();

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
    Thread.currentThread().setName(threadName);
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    CompactionScheduleTaskManager.getInstance().stop();
    super.tearDown();
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
}
