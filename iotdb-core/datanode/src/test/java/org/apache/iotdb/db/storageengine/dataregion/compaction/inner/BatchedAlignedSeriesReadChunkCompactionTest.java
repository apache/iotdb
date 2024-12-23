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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BatchedAlignedSeriesReadChunkCompactionTest extends AbstractCompactionTest {

  long originTargetChunkSize;
  long originTargetChunkPointNum;
  int originTargetPageSize;
  int originTargetPagePointNum;
  int originMaxConcurrentAlignedSeriesInCompaction;

  @Before
  public void setUp()
      throws IOException, MetadataException, InterruptedException, WriteProcessException {
    super.setUp();
    originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    originTargetChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    originTargetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    originTargetPagePointNum =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    originMaxConcurrentAlignedSeriesInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();

    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1048576);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100000);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10000);
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(2);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originTargetPageSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originTargetPagePointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(originMaxConcurrentAlignedSeriesInCompaction);
  }

  @Test
  public void testSimpleCompactionByFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    Assert.assertEquals(16, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);

    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithNullColumnByFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithAllDeletedColumnByFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    seqResource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE));
    seqResource1.getModFileForWrite().close();

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithNotExistColumnByFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    seqResource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE));
    seqResource1.getModFileForWrite().close();

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithNullColumn() throws Exception {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d0");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(100000, 200000)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(300000, 500000)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedColumnByFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    seqResource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d0", "s2"), 250000));
    seqResource1.getModFileForWrite().close();

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);

    Assert.assertEquals(15, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithAllDeletedPageByFlushPage() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 50000)}
            },
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    seqResource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d0", "s2"), 25000));
    seqResource1.getModFileForWrite().close();

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(60000, 70000), new TimeRange(80000, 90000)}
            },
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);
    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(0, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(10, summary.getDeserializeChunkCount());
    Assert.assertEquals(19, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testCompactionWithEmptyBatch() throws Exception {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3", "s4", "s5"),
          new TimeRange[] {new TimeRange(100, 200)},
          TSEncoding.RLE,
          CompressionType.LZ4,
          Arrays.asList(false, false, false, false, false));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3", "s4", "s5"),
          new TimeRange[] {new TimeRange(300, 400)},
          TSEncoding.RLE,
          CompressionType.LZ4,
          Arrays.asList(true, true, false, false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(0, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(14, summary.getDeserializeChunkCount());
    Assert.assertEquals(0, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedPageByWritePoint() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 50000)}
            },
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);
    seqResource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d0", "s2"), 15000));
    seqResource1.getModFileForWrite().close();

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(60000, 70000), new TimeRange(80000, 90000)}
            },
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);
    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertTrue(summary.getDeserializePageCount() > 0);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionByFlushPage() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 120000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(160000, 170000), new TimeRange(180000, 190000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(18, summary.getDeserializeChunkCount());
    Assert.assertEquals(18, summary.getDirectlyFlushPageCount());
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testSimpleCompactionByWritePoint() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(1000, 2000), new TimeRange(3000, 4000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(6000, 7000), new TimeRange(8000, 9000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(20, summary.getDeserializeChunkCount());
    Assert.assertEquals(20, summary.getDeserializePageCount());
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testCompactionWithDifferentCompressionTypeOrEncoding() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource seqResource3 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(1600000, 1700000), new TimeRange(1800000, 1900000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource3);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(16, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDirectlyFlushPageCount());
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }

  @Test
  public void testFlushChunkMetadataToTempFile() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      devices.add("d" + i);
    }
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      for (String device : devices) {
        writer.startChunkGroup(device);
        writer.generateSimpleAlignedSeriesToCurrentDevice(
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(1000, 2000), new TimeRange(3000, 5000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    seqResources.add(seqResource1);

    TsFileResource seqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource2)) {
      for (String device : devices) {
        writer.startChunkGroup(device);
        writer.generateSimpleAlignedSeriesToCurrentDevice(
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(6000, 7000), new TimeRange(8000, 9000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }

    seqResources.add(seqResource2);

    TsFileResource seqResource3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource3)) {
      for (String device : devices) {
        writer.startChunkGroup(device);
        writer.generateSimpleAlignedSeriesToCurrentDevice(
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(16000, 17000), new TimeRange(18000, 19000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    seqResources.add(seqResource3);

    CompactionTaskSummary summary = new CompactionTaskSummary();
    TsFileResource targetResource = performCompaction(summary);
    Assert.assertEquals(devices.size(), targetResource.buildDeviceTimeIndex().getDevices().size());
  }

  private TsFileResource performCompaction(CompactionTaskSummary summary) throws Exception {
    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    return targetResource;
  }
}
