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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
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
import java.util.Arrays;
import java.util.List;

public class NewSizeTieredCompactionSelectorTest extends AbstractCompactionTest {

  private long defaultTargetCompactionFileSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
  private long defaultTotalCompactionFileSize =
      IoTDBDescriptor.getInstance().getConfig().getInnerCompactionTotalFileSizeThreshold();
  private int defaultFileNumLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getFileLimitPerInnerTask();
  private int defaultFileNumLimit =
      IoTDBDescriptor.getInstance().getConfig().getTotalFileLimitForCompactionTask();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(defaultTargetCompactionFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileSizeThreshold(defaultTotalCompactionFileSize);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileNumThreshold(defaultFileNumLimit);
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(defaultFileNumLowerBound);
  }

  @Test
  public void testSelectAllFiles() throws IOException {
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              "d" + i,
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true);
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
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testSelectWithFileNumLimit() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTotalFileNumThreshold(8);
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(8);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-0-0.tsfile", i, i),
              "d" + 1,
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(2, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task1 = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task1.start());
    Assert.assertEquals(8, task1.getSelectedTsFileResourceList().size());
    Assert.assertEquals(8, task1.getAllSourceTsFiles().size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testSelectWithActivePartition() throws IOException {
    for (int i = 0; i < 10; i++) {
      TsFileResource resource;
      if (i == 9) {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", System.currentTimeMillis(), i),
                "d" + i,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                "d" + i,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      }
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
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
                "d" + 0,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                "d" + i,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      }
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
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(9, targetFiles.size());
    Assert.assertEquals(0, targetFiles.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(101L, targetFiles.get(0).getFileStartTime());
    Assert.assertEquals(200L, targetFiles.get(0).getFileEndTime());
  }

  @Test
  public void testSkipSomeFilesAndRenamePreviousFiles2() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1);
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile",
            Arrays.asList("d1", "d2"),
            new TimeRange[] {new TimeRange(100, 200)},
            true);
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile",
            Arrays.asList("d3", "d4"),
            new TimeRange[] {new TimeRange(300, 400)},
            true);
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile",
            Arrays.asList("d1", "d3"),
            new TimeRange[] {new TimeRange(500, 600)},
            true);
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile",
            Arrays.asList("d4", "d5"),
            new TimeRange[] {new TimeRange(700, 800)},
            true);
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile",
            Arrays.asList("d1", "d4"),
            new TimeRange[] {new TimeRange(900, 1000)},
            true);
    seqResources.add(resource5);

    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(5, targetFiles.size());
    Assert.assertEquals(5, targetFiles.get(targetFiles.size() - 1).getTsFileID().fileVersion);
    for (int i = 0; i < targetFiles.size(); i++) {
      TsFileResource targetFile = targetFiles.get(i);
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
            "1-1-0-0.tsfile",
            Arrays.asList("d1", "d2"),
            new TimeRange[] {new TimeRange(100, 200)},
            true);
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile",
            Arrays.asList("d3", "d4"),
            new TimeRange[] {new TimeRange(300, 400)},
            true);
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile",
            Arrays.asList("d1", "d3"),
            new TimeRange[] {new TimeRange(500, 600)},
            true);
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile",
            Arrays.asList("d4", "d5"),
            new TimeRange[] {new TimeRange(700, 800)},
            true);
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile",
            Arrays.asList("d1", "d4"),
            new TimeRange[] {new TimeRange(900, 1000)},
            true);
    seqResources.add(resource5);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            resource1.getTsFileSize() + resource3.getTsFileSize() + resource5.getTsFileSize() + 1);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(3, targetFiles.size());
    Assert.assertEquals(5, targetFiles.get(targetFiles.size() - 1).getTsFileID().fileVersion);
    for (int i = 0; i < targetFiles.size(); i++) {
      TsFileResource targetFile = targetFiles.get(i);
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
                "d" + 0,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      } else {
        resource =
            generateSingleNonAlignedSeriesFile(
                String.format("%d-%d-0-0.tsfile", i, i),
                "d" + i,
                new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
                true);
      }
      resource
          .getCompactionModFile()
          .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
      resource.getCompactionModFile().close();
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
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(9, targetFiles.size());
    Assert.assertEquals(0, targetFiles.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(101L, targetFiles.get(0).getFileStartTime());
    Assert.assertEquals(200L, targetFiles.get(0).getFileEndTime());
    for (int i = 0; i < targetFiles.size(); i++) {
      TsFileResource targetFile = targetFiles.get(i);
      if (i == 8) {
        Assert.assertTrue(targetFile.modFileExists());
      } else {
        Assert.assertFalse(targetFile.modFileExists());
      }
      Assert.assertFalse(targetFile.compactionModFileExists());
    }
  }

  @Test
  public void testAllTargetFilesEmpty() throws IOException, IllegalPathException {
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile",
            Arrays.asList("d1", "d2"),
            new TimeRange[] {new TimeRange(100, 200)},
            true);
    resource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    resource1.getModFile().close();
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile",
            Arrays.asList("d3", "d4"),
            new TimeRange[] {new TimeRange(300, 400)},
            true);
    resource2
        .getModFile()
        .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    resource2.getModFile().close();
    seqResources.add(resource2);

    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(2, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(2, task.getAllSourceTsFiles().size());
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(0, targetFiles.size());
  }

  @Test
  public void testAllTargetFilesEmptyWithSkippedSourceFiles()
      throws IOException, IllegalPathException {
    TsFileResource resource1 =
        generateSingleNonAlignedSeriesFile(
            "1-1-0-0.tsfile",
            Arrays.asList("d1", "d2"),
            new TimeRange[] {new TimeRange(100, 200)},
            true);
    resource1
        .getModFile()
        .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    resource1.getModFile().close();
    seqResources.add(resource1);
    TsFileResource resource2 =
        generateSingleNonAlignedSeriesFile(
            "2-2-0-0.tsfile",
            Arrays.asList("d3", "d4"),
            new TimeRange[] {new TimeRange(300, 400)},
            true);
    seqResources.add(resource2);
    TsFileResource resource3 =
        generateSingleNonAlignedSeriesFile(
            "3-3-0-0.tsfile",
            Arrays.asList("d1", "d3"),
            new TimeRange[] {new TimeRange(500, 600)},
            true);
    resource3
        .getModFile()
        .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    resource3.getModFile().close();
    seqResources.add(resource3);
    TsFileResource resource4 =
        generateSingleNonAlignedSeriesFile(
            "4-4-0-0.tsfile",
            Arrays.asList("d4", "d5"),
            new TimeRange[] {new TimeRange(700, 800)},
            true);
    seqResources.add(resource4);
    TsFileResource resource5 =
        generateSingleNonAlignedSeriesFile(
            "5-5-0-0.tsfile",
            Arrays.asList("d1", "d4"),
            new TimeRange[] {new TimeRange(900, 1000)},
            true);
    resource5
        .getModFile()
        .write(new Deletion(new PartialPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    resource5.getModFile().close();
    seqResources.add(resource5);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(
            resource1.getTsFileSize() + resource3.getTsFileSize() + resource5.getTsFileSize() + 1);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(3, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(5, task.getAllSourceTsFiles().size());
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(2, targetFiles.size());
    Assert.assertEquals(4, targetFiles.get(targetFiles.size() - 1).getTsFileID().fileVersion);
    for (int i = 0; i < targetFiles.size(); i++) {
      TsFileResource targetFile = targetFiles.get(i);
      Assert.assertEquals(2, targetFile.getDevices().size());
    }
  }

  @Test
  public void testSelectFilesInOtherLevel() throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setMaxLevelGapInInnerCompaction(5);
    for (int i = 0; i < 10; i++) {
      TsFileResource resource =
          generateSingleNonAlignedSeriesFile(
              String.format("%d-%d-%d-0.tsfile", i, i, i),
              "d" + i,
              new TimeRange[] {new TimeRange(100 * i + 1, 100 * (i + 1))},
              true);
      seqResources.add(resource);
    }
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    Assert.assertEquals(1, innerSpaceCompactionTasks.size());
    InnerSpaceCompactionTask task = innerSpaceCompactionTasks.get(0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(6, task.getSelectedTsFileResourceList().size());
    Assert.assertEquals(6, task.getAllSourceTsFiles().size());
    List<TsFileResource> targetFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(5, targetFiles.size());
    Assert.assertEquals(0, targetFiles.get(0).getTsFileID().fileVersion);
    Assert.assertEquals(1L, targetFiles.get(0).getFileStartTime());
    Assert.assertEquals(600L, targetFiles.get(0).getFileEndTime());
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String fileName, String device, TimeRange[] chunkTimeRanges, boolean isSeq)
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

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String fileName, List<String> devices, TimeRange[] chunkTimeRanges, boolean isSeq)
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
