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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.PageException;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NewReadChunkCompactionPerformerWithAlignedSeriesTest extends AbstractCompactionTest {

  long originTargetChunkSize;
  long originTargetChunkPointNum;
  int originTargetPageSize;
  int originTargetPagePointNum;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    originTargetChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    originTargetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    originTargetPagePointNum =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1048576);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100000);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10000);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originTargetPageSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originTargetPagePointNum);
  }

  @Test
  public void testSimpleCompactionByFlushChunk()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionWithNullColumnByFlushChunk()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionWithAllDeletedColumnByFlushChunk()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionWithNotExistColumnByFlushChunk()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(14, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedColumnByFlushChunk()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(15, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDeserializeChunkCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(0, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(8, summary.getDeserializeChunkCount());
    Assert.assertEquals(15, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    // this checker util may throw npe when the file contains any empty page
    // Assert.assertEquals(CompactionCheckerUtils.readFiles(seqResources),
    // CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionWithPartialDeletedPageByWritePoint()
      throws IOException, MetadataException, StorageEngineException, InterruptedException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertTrue(summary.getDeserializePageCount() > 0);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionByFlushPage()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
          PageException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 40000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(60000, 70000), new TimeRange(80000, 90000)},
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDeserializeChunkCount());
    Assert.assertEquals(16, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testSimpleCompactionByWritePoint()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(16, summary.getDeserializeChunkCount());
    Assert.assertEquals(16, summary.getDeserializePageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testCompactionWithDifferentCompressionTypeOrEncoding()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    Assert.assertEquals(8, summary.getDirectlyFlushChunkNum());
    Assert.assertEquals(0, summary.getDirectlyFlushPageCount());
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
  }

  @Test
  public void testFlushChunkMetadataToTempFile()
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
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
        Collections.singletonList(targetResource), true, COMPACTION_TEST_SG);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertEquals(
        CompactionCheckerUtils.readFiles(seqResources),
        CompactionCheckerUtils.readFiles(Collections.singletonList(targetResource)));
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
}
