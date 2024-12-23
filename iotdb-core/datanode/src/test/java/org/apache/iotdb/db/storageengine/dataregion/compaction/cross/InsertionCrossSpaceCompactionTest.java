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

package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionWorker;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;

public class InsertionCrossSpaceCompactionTest extends AbstractCompactionTest {

  private final FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      new FixedPriorityBlockingQueue<>(50, new DefaultCompactionTaskComparatorImpl());
  private final CompactionWorker worker = new CompactionWorker(0, candidateCompactionTaskQueue);

  private boolean enableInsertionCrossSpaceCompaction;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    enableInsertionCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    TsFileResourceManager.getInstance().clear();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableInsertionCrossSpaceCompaction);
    super.tearDown();
    TsFileResourceManager.getInstance().clear();
  }

  @Test
  public void test1() throws IOException, InterruptedException {
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(40, 50)}, true);
    seqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            new Phaser(1),
            0,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) tasks.get(0),
            0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(seqResource2, tsFileManager.getTsFileList(true).get(2));
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void test2() throws IOException, InterruptedException {
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    seqResources.add(seqResource1);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            new Phaser(1),
            0,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) tasks.get(0),
            0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(0));
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void test3() throws IOException, InterruptedException {
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    seqResources.add(seqResource1);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            new Phaser(1),
            0,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) tasks.get(0),
            0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(1));
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(0, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void test4() throws IOException, InterruptedException {
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "6-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(40, 50)}, true);
    seqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            new Phaser(1),
            0,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) tasks.get(0),
            0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(seqResource2, tsFileManager.getTsFileList(true).get(2));
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(4, timestamp);
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void test5() throws IOException, InterruptedException {
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            new Phaser(1),
            0,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) tasks.get(0),
            0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void testInsertionCompactionSchedule() throws IOException, InterruptedException {
    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "8-8-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(100, 400)}, true);
    seqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 40)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(60, 90)}, false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "4-4-0-0.tsfile",
            new String[] {"d1"},
            new TimeRange[] {new TimeRange(110, 1400)},
            false);
    unseqResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);

    DataRegionForCompactionTest dataRegion = createDataRegion();
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    Assert.assertEquals(2, dataRegion.executeInsertionCompaction());
    Assert.assertEquals(4, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    TsFileResource targetFile1 = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile1.getTsFile().getName()).getTime();
    Assert.assertEquals(4, timestamp);
    TsFileResource targetFile2 = tsFileManager.getTsFileList(true).get(2);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile2.getTsFile().getName()).getTime();
    Assert.assertEquals(6, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void testInsertionCompactionScheduleWithEmptySeqSpace()
      throws IOException, InterruptedException {
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(6, 9)}, false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "4-4-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(11, 14)}, false);
    unseqResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);

    DataRegionForCompactionTest dataRegion = createDataRegion();
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    Assert.assertEquals(3, dataRegion.executeInsertionCompaction());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());

    TsFileResource targetFile1 = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile1.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    TsFileResource targetFile2 = tsFileManager.getTsFileList(true).get(1);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile2.getTsFile().getName()).getTime();
    Assert.assertEquals(3, timestamp);
    TsFileResource targetFile3 = tsFileManager.getTsFileList(true).get(2);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile3.getTsFile().getName()).getTime();
    Assert.assertEquals(4, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void testInsertionCompactionScheduleWithEmptySeqSpace2()
      throws IOException, InterruptedException {
    TsFileResource unseqResource0 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(3, 14)}, false);
    unseqResource0.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(6, 9)}, false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "4-4-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(11, 14)}, false);
    unseqResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource5 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "5-5-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(21, 24)}, false);
    unseqResource5.setStatusForTest(TsFileResourceStatus.NORMAL);
    unseqResources.add(unseqResource0);
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);
    unseqResources.add(unseqResource5);

    DataRegionForCompactionTest dataRegion = createDataRegion();
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    Assert.assertEquals(2, dataRegion.executeInsertionCompaction());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    TsFileResource targetFile1 = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile1.getTsFile().getName()).getTime();
    Assert.assertEquals(1, timestamp);
    TsFileResource targetFile2 = tsFileManager.getTsFileList(true).get(1);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile2.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void testInsertionCompactionScheduleWithMultiTimePartitions()
      throws IOException, InterruptedException {
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(6, 9)}, false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    createTimePartitionDirIfNotExist(2808L);
    TsFileResource unseqResource3 =
        generateSingleNonAlignedSeriesFileWithDevicesWithTimePartition(
            "4-4-0-0.tsfile",
            new String[] {"d1"},
            new TimeRange[] {new TimeRange(1698301490306L, 1698301490406L)},
            2808L,
            false);
    unseqResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);

    DataRegionForCompactionTest dataRegion = createDataRegion();
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    TsFileResourceManager.getInstance().registerSealedTsFileResource(unseqResource1);
    TsFileResourceManager.getInstance().registerSealedTsFileResource(unseqResource2);
    TsFileResourceManager.getInstance().registerSealedTsFileResource(unseqResource3);
    tsFileManager.getOrCreateUnsequenceListByTimePartition(0).keepOrderInsert(unseqResource1);
    tsFileManager.getOrCreateUnsequenceListByTimePartition(0).keepOrderInsert(unseqResource2);
    tsFileManager.getOrCreateUnsequenceListByTimePartition(2808).keepOrderInsert(unseqResource3);

    Assert.assertEquals(3, dataRegion.executeInsertionCompaction());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getOrCreateSequenceListByTimePartition(0).size());
    Assert.assertEquals(1, tsFileManager.getOrCreateSequenceListByTimePartition(2808).size());

    TsFileResource targetFile1 = tsFileManager.getOrCreateSequenceListByTimePartition(0).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile1.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
    TsFileResource targetFile2 = tsFileManager.getOrCreateSequenceListByTimePartition(0).get(1);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile2.getTsFile().getName()).getTime();
    Assert.assertEquals(3, timestamp);
    TsFileResource targetFile3 = tsFileManager.getOrCreateSequenceListByTimePartition(2808).get(0);
    timestamp = TsFileNameGenerator.getTsFileName(targetFile3.getTsFile().getName()).getTime();
    Assert.assertEquals(4, timestamp);
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  @Test
  public void testInsertionCompactionUpdateFileMetrics() throws IOException {
    TsFileResource unseqResource1 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    FileMetrics.getInstance()
        .addTsFile(
            unseqResource1.getDatabaseName(),
            unseqResource1.getDataRegionId(),
            unseqResource1.getTsFileSize(),
            false,
            unseqResource1.getTsFile().getName());

    long seqFileNumBeforeCompaction = FileMetrics.getInstance().getFileCount(true);
    long unseqFileNumBeforeCompaction = FileMetrics.getInstance().getFileCount(false);

    InsertionCrossCompactionTaskResource taskResource = new InsertionCrossCompactionTaskResource();
    taskResource.setToInsertUnSeqFile(unseqResource1);
    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(null, 0, tsFileManager, taskResource, 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(
        seqFileNumBeforeCompaction + 1, FileMetrics.getInstance().getFileCount(true));
    Assert.assertEquals(
        unseqFileNumBeforeCompaction - 1, FileMetrics.getInstance().getFileCount(false));

    TsFileResource unseqResource2 =
        generateSingleNonAlignedSeriesFileWithDevices(
            "3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(5, 6)}, false);
    FileMetrics.getInstance()
        .addTsFile(
            unseqResource2.getDatabaseName(),
            unseqResource2.getDataRegionId(),
            unseqResource2.getTsFileSize(),
            false,
            unseqResource2.getTsFile().getName());

    seqFileNumBeforeCompaction = FileMetrics.getInstance().getFileCount(true);
    unseqFileNumBeforeCompaction = FileMetrics.getInstance().getFileCount(false);

    taskResource = new InsertionCrossCompactionTaskResource();
    taskResource.setToInsertUnSeqFile(unseqResource2);
    task = new InsertionCrossSpaceCompactionTask(null, 0, tsFileManager, taskResource, 0);
    // .resource file not found
    Files.deleteIfExists(
        Paths.get(unseqResource2.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
    // rollback
    Assert.assertFalse(task.start());
    Assert.assertEquals(seqFileNumBeforeCompaction, FileMetrics.getInstance().getFileCount(true));
    Assert.assertEquals(
        unseqFileNumBeforeCompaction, FileMetrics.getInstance().getFileCount(false));
    Assert.assertEquals(
        tsFileManager.size(true) + tsFileManager.size(false),
        TsFileResourceManager.getInstance().getPriorityQueueSize());
  }

  public TsFileResource generateSingleNonAlignedSeriesFileWithDevices(
      String fileName, String[] devices, TimeRange[] timeRanges, boolean seq) throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResourceWithName(fileName, seq, 0);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    for (int i = 0; i < devices.length; i++) {
      String device = devices[i];
      TimeRange timeRange = timeRanges[i];
      writer1.startChunkGroup(device);
      writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {timeRange}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer1.endChunkGroup();
    }
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public TsFileResource generateSingleNonAlignedSeriesFileWithDevicesWithTimePartition(
      String fileName, String[] devices, TimeRange[] timeRanges, long timePartition, boolean seq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResourceWithName(fileName, timePartition, seq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    for (int i = 0; i < devices.length; i++) {
      String device = devices[i];
      TimeRange timeRange = timeRanges[i];
      writer1.startChunkGroup(device);
      writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {timeRange}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer1.endChunkGroup();
    }
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private DataRegionForCompactionTest createDataRegion() {
    return new DataRegionForCompactionTest(COMPACTION_TEST_SG, "0");
  }

  private static class DataRegionForCompactionTest extends DataRegion {

    public DataRegionForCompactionTest(String databaseName, String id) {
      super(databaseName, id);
    }

    public int executeInsertionCompaction() throws InterruptedException {
      return super.executeInsertionCompaction(
          new ArrayList<>(this.getTsFileManager().getTimePartitions()),
          new CompactionScheduleContext());
    }
  }
}
