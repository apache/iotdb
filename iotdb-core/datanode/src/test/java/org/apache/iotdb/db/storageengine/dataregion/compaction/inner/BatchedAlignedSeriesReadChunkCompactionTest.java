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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  public void testSimpleCompactionByFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);

    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithNullColumnByFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithAllDeletedColumnByFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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
        .getModFile()
        .write(
            new Deletion(new PartialPath("root.testsg.d0", "s2"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqResource1.getModFile().close();

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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithNotExistColumnByFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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
        .getModFile()
        .write(
            new Deletion(new PartialPath("root.testsg.d0", "s2"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqResource1.getModFile().close();

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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithNullColumn()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedColumnByFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 250000));
    seqResource1.getModFile().close();

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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(15, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
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
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 25000));
    seqResource1.getModFile().close();

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
    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(0, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(8, summary.getDeserializeChunkCount());
    Assert.assertEquals(15, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedPageByWritePoint()
      throws IOException,
          MetadataException,
          StorageEngineException,
          InterruptedException,
          PageException {
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
        .getModFile()
        .write(new Deletion(new PartialPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 15000));
    seqResource1.getModFile().close();

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
    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertTrue(summary.getDeserializePageCount() > 0);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionByFlushPage()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDeserializeChunkCount());
    Assert.assertEquals(16, summary.getDirectlyFlushPageCount());
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testSimpleCompactionByWritePoint()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDeserializeChunkCount());
    Assert.assertEquals(16, summary.getDeserializePageCount());
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  public void testCompactionWithDifferentCompressionTypeOrEncoding()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDirectlyFlushPageCount());
    Assert.assertEquals(
        CompactionCheckerUtils.getDataByQuery(getPaths(seqResources), seqResources, unseqResources),
        CompactionCheckerUtils.getDataByQuery(
            getPaths(Collections.singletonList(targetResource)),
            Collections.singletonList(targetResource),
            Collections.emptyList()));
  }

  @Test
  @Ignore
  public void testFlushChunkMetadataToTempFile()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
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

    tsFileManager.addAll(seqResources, true);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    //    Assert.assertEquals(
    //        CompactionCheckerUtils.getDataByQuery(
    //            getPaths(seqResources), null, seqResources, unseqResources),
    //        CompactionCheckerUtils.getDataByQuery(
    //            getPaths(Collections.singletonList(targetResource)),
    //            null,
    //            Collections.singletonList(targetResource),
    //            Collections.emptyList()));
    Assert.assertEquals(devices.size(), targetResource.buildDeviceTimeIndex().getDevices().size());
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      List<Boolean> nullValues,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
        measurement, chunkTimeRanges, encoding, compressionType, nullValues);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      List<Boolean> nullValues,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
        measurement, chunkTimeRanges, encoding, compressionType, nullValues);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      List<Boolean> nullValues,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
        measurement, chunkTimeRanges, encoding, compressionType, nullValues);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private List<PartialPath> getPaths(List<TsFileResource> resources)
      throws IOException, IllegalPathException {
    Set<PartialPath> paths = new HashSet<>();
    try (MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(resources)) {
      while (deviceIterator.hasNextDevice()) {
        Pair<IDeviceID, Boolean> iDeviceIDBooleanPair = deviceIterator.nextDevice();
        IDeviceID deviceID = iDeviceIDBooleanPair.getLeft();
        boolean isAlign = iDeviceIDBooleanPair.getRight();
        Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
        IMeasurementSchema timeSchema = schemaMap.remove(TsFileConstant.TIME_COLUMN_ID);
        List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
        if (measurementSchemas.isEmpty()) {
          continue;
        }
        List<String> existedMeasurements =
            measurementSchemas.stream()
                .map(IMeasurementSchema::getMeasurementId)
                .collect(Collectors.toList());
        PartialPath seriesPath;
        if (isAlign) {
          seriesPath =
              new AlignedPath(
                  ((PlainDeviceID) deviceID).toStringID(), existedMeasurements, measurementSchemas);
        } else {
          seriesPath =
              new MeasurementPath(deviceID, existedMeasurements.get(0), measurementSchemas.get(0));
        }
        paths.add(seriesPath);
      }
    }
    return new ArrayList<>(paths);
  }
}
