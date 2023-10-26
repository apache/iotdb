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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
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
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Phaser;

public class InsertionCrossSpaceCompactionTest extends AbstractCompactionTest {

  private final FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      new FixedPriorityBlockingQueue<>(50, new DefaultCompactionTaskComparatorImpl());
  private final CompactionWorker worker = new CompactionWorker(0, candidateCompactionTaskQueue);

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws IOException, InterruptedException {
    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFileWithDevices("1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource seqResource2 = generateSingleNonAlignedSeriesFileWithDevices("3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(40, 50)}, true);
    seqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector
        = new RewriteCrossSpaceCompactionSelector(
        COMPACTION_TEST_SG,
        "0",
        0,
        tsFileManager
    );
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks = selector.selectInsertionCrossSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0), tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task = new InsertionCrossSpaceCompactionTask(new Phaser(1), 0, tsFileManager, (InsertionCrossCompactionTaskResource) tasks.get(0), 0);
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
  }

  @Test
  public void test2() throws IOException, InterruptedException {
    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFileWithDevices("1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector
        = new RewriteCrossSpaceCompactionSelector(
        COMPACTION_TEST_SG,
        "0",
        0,
        tsFileManager
    );
    seqResources.add(seqResource1);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks = selector.selectInsertionCrossSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0), tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task = new InsertionCrossSpaceCompactionTask(new Phaser(1), 0, tsFileManager, (InsertionCrossCompactionTaskResource) tasks.get(0), 0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(0));
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(2, timestamp);
  }

  @Test
  public void test3() throws IOException, InterruptedException {
    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFileWithDevices("1-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector
        = new RewriteCrossSpaceCompactionSelector(
        COMPACTION_TEST_SG,
        "0",
        0,
        tsFileManager
    );
    seqResources.add(seqResource1);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks = selector.selectInsertionCrossSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0), tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task = new InsertionCrossSpaceCompactionTask(new Phaser(1), 0, tsFileManager, (InsertionCrossCompactionTaskResource) tasks.get(0), 0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(1));
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(0, timestamp);
  }

  @Test
  public void test4() throws IOException, InterruptedException {
    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-1-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(10, 20)}, true);
    seqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource seqResource2 = generateSingleNonAlignedSeriesFileWithDevices("6-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(40, 50)}, true);
    seqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("3-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(30, 34)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector
        = new RewriteCrossSpaceCompactionSelector(
        COMPACTION_TEST_SG,
        "0",
        0,
        tsFileManager
    );
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks = selector.selectInsertionCrossSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0), tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task = new InsertionCrossSpaceCompactionTask(new Phaser(1), 0, tsFileManager, (InsertionCrossCompactionTaskResource) tasks.get(0), 0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(seqResource1, tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(seqResource2, tsFileManager.getTsFileList(true).get(2));
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(1);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(3, timestamp);
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
  }

  @Test
  public void test5() throws IOException, InterruptedException {
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    RewriteCrossSpaceCompactionSelector selector
        = new RewriteCrossSpaceCompactionSelector(
        COMPACTION_TEST_SG,
        "0",
        0,
        tsFileManager
    );
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<CrossCompactionTaskResource> tasks = selector.selectInsertionCrossSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0), tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, tasks.size());
    InsertionCrossSpaceCompactionTask task = new InsertionCrossSpaceCompactionTask(new Phaser(1), 0, tsFileManager, (InsertionCrossCompactionTaskResource) tasks.get(0), 0);
    task.setSourceFilesToCompactionCandidate();
    candidateCompactionTaskQueue.put(task);
    Assert.assertTrue(worker.processOneCompactionTask(candidateCompactionTaskQueue.take()));
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertTrue(tsFileManager.getTsFileList(false).isEmpty());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    long timestamp = TsFileNameGenerator.getTsFileName(targetFile.getTsFile().getName()).getTime();
    Assert.assertEquals(1, timestamp);
  }

  @Test
  public void testInsertionCompactionSchedule() throws IOException {
    TsFileResource unseqResource1 = generateSingleNonAlignedSeriesFileWithDevices("2-2-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(1, 4)}, false);
    unseqResource1.setStatusForTest(TsFileResourceStatus.NORMAL);

    TsFileResource unseqResource2 = generateSingleNonAlignedSeriesFileWithDevices("3-3-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(6, 9)}, false);
    unseqResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    TsFileResource unseqResource3 = generateSingleNonAlignedSeriesFileWithDevices("4-4-0-0.tsfile", new String[] {"d1"}, new TimeRange[] {new TimeRange(11, 14)}, false);
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
  }

  public TsFileResource generateSingleNonAlignedSeriesFileWithDevices(String fileName, String[] devices, TimeRange[] timeRanges, boolean seq) throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResourceWithName(fileName, seq, 0);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    for (int i = 0; i < devices.length; i++) {
      String device = devices[i];
      TimeRange timeRange = timeRanges[i];
      writer1.startChunkGroup(device);
      writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] { timeRange }, TSEncoding.PLAIN, CompressionType.LZ4);
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

    @Override
    public int executeInsertionCompaction() {
      return super.executeInsertionCompaction();
    }
  }
}
