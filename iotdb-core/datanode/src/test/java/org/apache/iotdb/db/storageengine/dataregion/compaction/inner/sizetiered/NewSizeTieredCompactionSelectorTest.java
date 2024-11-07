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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class NewSizeTieredCompactionSelectorTest extends AbstractCompactionTest {

  private long defaultTargetCompactionFileSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
  private long defaultTotalCompactionFileSize =
      IoTDBDescriptor.getInstance().getConfig().getInnerCompactionTotalFileSizeThresholdInByte();
  private int defaultFileNumLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
  private int defaultFileNumLimit =
      IoTDBDescriptor.getInstance().getConfig().getTotalFileLimitForCompactionTask();
  private InnerSeqCompactionPerformer defaultPerformer =
      IoTDBDescriptor.getInstance().getConfig().getInnerSeqCompactionPerformer();
  private String performer;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"read_chunk"}, {"fast"},
        });
  }

  public NewSizeTieredCompactionSelectorTest(String performer) {
    this.performer = performer;
  }

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerSeqCompactionPerformer(
            InnerSeqCompactionPerformer.getInnerSeqCompactionPerformer(performer));
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(defaultTargetCompactionFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileSizeThresholdInByte(defaultTotalCompactionFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileNumThreshold(defaultFileNumLimit);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionCandidateFileNum(defaultFileNumLowerBound);
    IoTDBDescriptor.getInstance().getConfig().setInnerSeqCompactionPerformer(defaultPerformer);
  }

  @Test
  public void testSelectAllFiles() throws IOException {
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              "d" + i);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(10, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(10, task.getAllSourceTsFiles().size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testSelectWithFileNumLimit() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTotalFileNumThreshold(8);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(8);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              "d" + 1);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task1.start());
    Assert.assertEquals(8, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(8, task1.getAllSourceTsFiles().size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    InnerSpaceCompactionTask task2 = innerSpaceCompactionTasks.get(1);
    Assert.assertEquals(2, task2.getSelectedTsFileResourceList().size());
    Assert.assertEquals(2, task2.getAllSourceTsFiles().size());
    Assert.assertTrue(task2.start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testSelectWithActivePartition() throws IOException {
    for (int i = 0; i < 10; i++) {
      TsFileResource resource;
      if (i == 9) {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", System.currentTimeMillis(), i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + i);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + i);
      }
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertTrue(innerSpaceCompactionTasks.isEmpty());
    innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources.subList(0, seqResources.size() - 1));
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
  }

  @Test
  public void testSkipSomeFilesAndRenamePreviousFiles() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(100);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource;
      if (i == 9) {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + 0);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + i);
      }
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(2, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(10, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(9, filesAfterCompaction.size());
    Assert.assertEquals(0, filesAfterCompaction.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(101L, filesAfterCompaction.get(0).getFileStartTime());
    Assert.assertEquals(200L, filesAfterCompaction.get(0).getFileEndTime());
  }

  @Test
  public void testSkipSomeFilesAndRenamePreviousFiles2() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1);
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile", new TimeRange[] {new TimeRange(100, 200)}, true, "d1", "d2");
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile", new TimeRange[] {new TimeRange(300, 400)}, true, "d3", "d4");
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile", new TimeRange[] {new TimeRange(500, 600)}, true, "d1", "d3");
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile", new TimeRange[] {new TimeRange(700, 800)}, true, "d4", "d5");
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile", new TimeRange[] {new TimeRange(900, 1000)}, true, "d1", "d4");
    seqResources.add(resource5);

    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    // select resource1, resource3, resource5
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(5, filesAfterCompaction.size());
    Assert.assertEquals(
        5, filesAfterCompaction.get(filesAfterCompaction.size() - 1).getTsFileID().fileVersion);
    // expected result:
    // renamed resource2[d3,d4], renamed resource4[d4,d5], target1[d1], target2[d2], target3[d3,d4]
    for (int i = 0; i < filesAfterCompaction.size(); i++) {
      TsFileResource targetFile = filesAfterCompaction.get(i);
      if (i == 4 || i < 2) {
        Assert.assertEquals(2, targetFile.getDevices().size());
      } else {
        Assert.assertEquals(1, targetFile.getDevices().size());
      }
    }
  }

  @Test
  public void testSkipSomeFiles() throws IOException {
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile", new TimeRange[] {new TimeRange(100, 200)}, true, "d1", "d2");
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile", new TimeRange[] {new TimeRange(300, 400)}, true, "d3", "d4");
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile", new TimeRange[] {new TimeRange(500, 600)}, true, "d1", "d3");
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile", new TimeRange[] {new TimeRange(700, 800)}, true, "d4", "d5");
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile", new TimeRange[] {new TimeRange(900, 1000)}, true, "d1", "d4");
    seqResources.add(resource5);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            resource1.getTsFileSize() + resource3.getTsFileSize() + resource5.getTsFileSize() + 1);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(3, filesAfterCompaction.size());
    Assert.assertEquals(
        5, filesAfterCompaction.get(filesAfterCompaction.size() - 1).getTsFileID().fileVersion);
    for (int i = 0; i < filesAfterCompaction.size(); i++) {
      TsFileResource targetFile = filesAfterCompaction.get(i);
      if (i == 2) {
        Assert.assertEquals(4, targetFile.getDevices().size());
      } else {
        Assert.assertEquals(2, targetFile.getDevices().size());
      }
    }
  }

  @Test
  public void testSkipSomeFilesAndRenamePreviousFilesWithCompactionMods()
      throws IOException, IllegalPathException {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(100);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource;
      if (i == 9) {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + 0);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true,
                "d" + i);
      }
      resource
          .getCompactionModFile()
          .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
      resource.getCompactionModFile().close();
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(2, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(10, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(9, filesAfterCompaction.size());
    Assert.assertEquals(0, filesAfterCompaction.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(101L, filesAfterCompaction.get(0).getFileStartTime());
    Assert.assertEquals(200L, filesAfterCompaction.get(0).getFileEndTime());
    for (int i = 0; i < filesAfterCompaction.size(); i++) {
      TsFileResource resource = filesAfterCompaction.get(i);
      if (i == 8) {
        Assert.assertTrue(resource.anyModFileExists());
      } else {
        Assert.assertFalse(resource.anyModFileExists());
      }
      Assert.assertFalse(resource.compactionModFileExists());
    }
  }

  @Test
  public void testAllTargetFilesEmpty() throws IOException, IllegalPathException {
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile", new TimeRange[] {new TimeRange(100, 200)}, true, "d1", "d2");
    resource1
        .getModFileForWrite()
        .write(
            new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MIN_VALUE, Long.MAX_VALUE));
    resource1.getModFileForWrite().close();
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile", new TimeRange[] {new TimeRange(300, 400)}, true, "d3", "d4");
    resource2
        .getModFileForWrite()
        .write(
            new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MIN_VALUE, Long.MAX_VALUE));
    resource2.getModFileForWrite().close();
    seqResources.add(resource2);

    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(2, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(2, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(0, filesAfterCompaction.size());
  }

  @Test
  public void testAllTargetFilesEmptyWithSkippedSourceFiles()
      throws IOException, IllegalPathException {
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile", new TimeRange[] {new TimeRange(100, 200)}, true, "d1", "d2");
    resource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    resource1.getModFileForWrite().close();
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile", new TimeRange[] {new TimeRange(300, 400)}, true, "d3", "d4");
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile", new TimeRange[] {new TimeRange(500, 600)}, true, "d1", "d3");
    resource3
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    resource3.getModFileForWrite().close();
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile", new TimeRange[] {new TimeRange(700, 800)}, true, "d4", "d5");
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile", new TimeRange[] {new TimeRange(900, 1000)}, true, "d1", "d4");
    resource5
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    resource5.getModFileForWrite().close();
    seqResources.add(resource5);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            resource1.getTsFileSize() + resource3.getTsFileSize() + resource5.getTsFileSize() + 1);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(2, filesAfterCompaction.size());
    Assert.assertEquals(
        4, filesAfterCompaction.get(filesAfterCompaction.size() - 1).getTsFileID().fileVersion);
    for (int i = 0; i < filesAfterCompaction.size(); i++) {
      TsFileResource resource = filesAfterCompaction.get(i);
      Assert.assertEquals(2, resource.getDevices().size());
    }
  }

  @Test
  public void testSelectFilesInOtherLevel() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setMaxLevelGapInInnerCompaction(5);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-%d-0.tsfile", i, i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              "d" + i);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(6, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(5, filesAfterCompaction.size());
    Assert.assertEquals(0, filesAfterCompaction.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(1L, filesAfterCompaction.get(0).getFileStartTime());
    Assert.assertEquals(600L, filesAfterCompaction.get(0).getFileEndTime());
  }

  @Test
  public void testSkipToPreviousIndexAndSelectSkippedFiles() throws IOException {
    for (int i = 0; i < 10; i++) {
      String device;
      if (i >= 5) {
        device = "d5";
      } else {
        device = "d" + i;
      }
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              device);
      seqResources.add(resource);
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(seqResources.get(0).getTsFileSize() + 1);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(5, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> filesAfterCompaction = tsFileManager.getTsFileList(true);
    Assert.assertEquals(6, filesAfterCompaction.size());
    Assert.assertEquals(5, filesAfterCompaction.get(5).getTsFileID().fileVersion);
    Assert.assertEquals(501L, filesAfterCompaction.get(5).getFileStartTime());
    Assert.assertEquals(1000L, filesAfterCompaction.get(5).getFileEndTime());
  }

  @Test
  public void testSkipToPreviousIndexAndSelectSkippedFiles2() throws IOException {
    for (int i = 0; i < 10; i++) {
      String device;
      if (i >= 4) {
        device = "d4";
      } else {
        device = "d" + i;
      }
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              device);
      seqResources.add(resource);
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            seqResources.get(0).getTsFileSize()
                + seqResources.get(1).getTsFileSize()
                + seqResources.get(2).getTsFileSize()
                + seqResources.get(3).getTsFileSize());
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    Assert.assertEquals(4, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(4, task1.getAllSourceTsFiles().size());
    InnerSpaceCompactionTask task2 = innerSpaceCompactionTasks.get(1);
    Assert.assertEquals(6, task2.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task2.getAllSourceTsFiles().size());
  }

  @Test
  public void testSkipToPreviousIndexAndSelectSkippedFiles3() throws IOException {
    // TsFiles: [d0], [d1], [d0], [d3], [d4], [d4], [d4], [d4], [d4], [d4]
    for (int i = 0; i < 10; i++) {
      String device;
      if (i == 2) {
        device = "d0";
      } else if (i >= 4) {
        device = "d4";
      } else {
        device = "d" + i;
      }
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              device);
      seqResources.add(resource);
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            seqResources.get(0).getTsFileSize()
                + seqResources.get(1).getTsFileSize()
                + seqResources.get(2).getTsFileSize()
                + seqResources.get(3).getTsFileSize());
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    // task1: [d0], [d1], [d0], [d3]
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    Assert.assertEquals(4, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(4, task1.getAllSourceTsFiles().size());
    // task2: [d4], [d4], [d4], [d4], [d4], [d4]
    InnerSpaceCompactionTask task2 = innerSpaceCompactionTasks.get(1);
    Assert.assertEquals(6, task2.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task2.getAllSourceTsFiles().size());
  }

  @Test
  public void testSkipToPreviousIndexAndSelectSkippedFiles4() throws IOException {
    // TsFiles: [d0], [d1], [d2], [d3](level=100), [d3], [d3], [d3], [d3], [d3], [d3]
    for (int i = 0; i < 10; i++) {
      String device;
      if (i >= 4) {
        device = "d3";
      } else {
        device = "d" + i;
      }
      int level = 0;
      if (i == 3) {
        level = 100;
      }
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-%d-0.tsfile", i, i, level),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              device);
      seqResources.add(resource);
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            seqResources.get(0).getTsFileSize()
                + seqResources.get(1).getTsFileSize()
                + seqResources.get(2).getTsFileSize()
                + seqResources.get(3).getTsFileSize());
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    // select file0, file1, file2
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    Assert.assertEquals(3, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(3, task1.getAllSourceTsFiles().size());
    InnerSpaceCompactionTask task2 = innerSpaceCompactionTasks.get(1);
    // select file4 - file9
    Assert.assertEquals(6, task2.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task2.getAllSourceTsFiles().size());
  }

  @Test
  public void testSkipToPreviousIndexAndSelectSkippedFiles5() throws IOException {
    // TsFiles: [d0], [d1], [d2], [d3](compacting), [d3], [d3], [d3], [d3], [d3], [d3]
    for (int i = 0; i < 10; i++) {
      String device;
      if (i >= 4) {
        device = "d3";
      } else {
        device = "d" + i;
      }
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-%d-0.tsfile", i, i, 0),
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true,
              device);
      if (i == 3) {
        resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
      }
      seqResources.add(resource);
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            seqResources.get(0).getTsFileSize()
                + seqResources.get(1).getTsFileSize()
                + seqResources.get(2).getTsFileSize()
                + seqResources.get(3).getTsFileSize());
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(
            COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext());
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    // select file0, file1, file2
    Assert.assertEquals(3, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(3, task1.getAllSourceTsFiles().size());
    InnerSpaceCompactionTask task2 = innerSpaceCompactionTasks.get(1);
    // select file4 - file9
    Assert.assertEquals(6, task2.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task2.getAllSourceTsFiles().size());
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String fileName, TimeRange[] chunkTimeRanges, boolean isSeq, String... devices)
      throws IOException {
    TsFileResource resource = createEmptyFileAndResourceWithName(fileName, 0, isSeq);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      for (String device : devices) {
        writer.startChunkGroup(device);
        writer.generateSimpleNonAlignedSeriesToCurrentDevice(
            "s1", chunkTimeRanges, TSEncoding.RLE, CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    tsFileManager.keepOrderInsert(resource, isSeq);
    return resource;
  }
}
