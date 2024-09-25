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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.AlignedSeriesBatchCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactPagePlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactionAlignedPageLazyLoadPointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FirstBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FollowingBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BatchCompactionUtilsTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testSelectBatchColumn() {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      if (i % 2 == 0) {
        measurementSchemas.add(new MeasurementSchema("s" + i, TSDataType.TEXT));
      } else {
        measurementSchemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
      }
    }
    AlignedSeriesBatchCompactionUtils.BatchColumnSelection batchColumnSelection =
        new AlignedSeriesBatchCompactionUtils.BatchColumnSelection(measurementSchemas, 10);
    Assert.assertTrue(batchColumnSelection.hasNext());
    batchColumnSelection.next();
    Assert.assertEquals(10, batchColumnSelection.getCurrentSelectedColumnSchemaList().size());
  }

  @Test
  public void testBatchCompactionPointReader() throws IOException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(1000, 2000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, true, true),
            true);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(seqResource1.getTsFile().getAbsolutePath())) {
      AlignedChunkMetadata alignedChunkMetadata =
          reader
              .getAlignedChunkMetadata(
                  IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d0"), true)
              .get(0);
      ChunkMetadata timeChunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      Chunk timeChunk = reader.readMemChunk(timeChunkMetadata);
      List<Chunk> valueChunks = new ArrayList<>();
      for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
        if (valueChunkMetadata == null) {
          valueChunks.add(null);
          continue;
        }
        valueChunks.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
      }

      AlignedChunkReader alignedChunkReader = new AlignedChunkReader(timeChunk, valueChunks);
      AlignedPageReader iPageReader =
          (AlignedPageReader) alignedChunkReader.loadPageReaderList().get(0);
      CompactionAlignedPageLazyLoadPointReader batchCompactionPointReader =
          new CompactionAlignedPageLazyLoadPointReader(
              iPageReader.getTimePageReader(),
              iPageReader.getValuePageReaderList().subList(1, 2),
              false);
      int readPointNum = 0;
      while (batchCompactionPointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = batchCompactionPointReader.nextTimeValuePair();
        for (Object value : timeValuePair.getValues()) {
          Assert.assertNull(value);
        }
        readPointNum++;
      }

      Assert.assertEquals(readPointNum, 1001);
    }
  }

  @Test
  public void testFirstBatchChunkWriter() throws IOException, PageException {
    TsFileResource resource = createEmptyFileAndResource(true);
    CompactChunkPlan compactChunkPlan = null;
    try (TsFileIOWriter writer = new TsFileIOWriter(resource.getTsFile())) {
      IMeasurementSchema timeSchema = new MeasurementSchema("", TSDataType.TIMESTAMP);
      List<IMeasurementSchema> valueSchemas = new ArrayList<>();
      valueSchemas.add(new MeasurementSchema("s0", TSDataType.INT32));
      FirstBatchCompactionAlignedChunkWriter chunkWriter =
          new FirstBatchCompactionAlignedChunkWriter(timeSchema, valueSchemas);
      // write data point
      chunkWriter.write(1, new TsPrimitiveType[] {new TsPrimitiveType.TsInt(10)});

      // flush page
      TimeStatistics timeStatistics = new TimeStatistics();
      timeStatistics.update(10);
      timeStatistics.update(20);
      IntegerStatistics statistics = new IntegerStatistics();
      statistics.update(10, 10);
      statistics.update(20, 20);
      chunkWriter.sealCurrentPage();
      // repeat call seal current page
      chunkWriter.sealCurrentPage();
      chunkWriter.writePageHeaderAndDataIntoTimeBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, timeStatistics));
      chunkWriter.writePageHeaderAndDataIntoValueBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, statistics), 0);
      compactChunkPlan = chunkWriter.getCompactedChunkRecord();
      chunkWriter.writeToFileWriter(writer);
    }
    Assert.assertFalse(compactChunkPlan.isCompactedByDirectlyFlush());
    Assert.assertEquals(1, compactChunkPlan.getTimeRange().getMin());
    Assert.assertEquals(20, compactChunkPlan.getTimeRange().getMax());
    Assert.assertEquals(2, compactChunkPlan.getPageRecords().size());
  }

  @Test
  public void testFollowedBatchChunkWriter() throws PageException {
    List<CompactPagePlan> pages = new ArrayList<>();
    pages.add(new CompactPagePlan(10, 20, true));
    pages.add(new CompactPagePlan(30, 50, false));
    pages.add(new CompactPagePlan(60, 70, true));
    CompactChunkPlan firstChunk = new CompactChunkPlan(pages);
    CompactChunkPlan secondChunk = new CompactChunkPlan(100, 200);

    IMeasurementSchema timeSchema = new MeasurementSchema("", TSDataType.TIMESTAMP);
    List<IMeasurementSchema> valueSchemas = new ArrayList<>();
    valueSchemas.add(new MeasurementSchema("s0", TSDataType.INT32));
    FollowingBatchCompactionAlignedChunkWriter chunkWriter =
        new FollowingBatchCompactionAlignedChunkWriter(timeSchema, valueSchemas, firstChunk);
    Assert.assertTrue(chunkWriter.isEmpty());
    Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
    chunkWriter.sealCurrentPage();
    Assert.assertTrue(chunkWriter.isEmpty());
    Assert.assertEquals(0, chunkWriter.getCurrentPage());

    // flush page
    TimeStatistics timeStatistics = new TimeStatistics();
    timeStatistics.update(10);
    timeStatistics.update(20);
    IntegerStatistics statistics = new IntegerStatistics();
    statistics.update(10, 10);
    statistics.update(20, 20);
    chunkWriter.writePageHeaderAndDataIntoTimeBuff(
        ByteBuffer.allocate(1), new PageHeader(1, 1, timeStatistics));
    chunkWriter.writePageHeaderAndDataIntoValueBuff(
        ByteBuffer.allocate(1), new PageHeader(1, 1, statistics), 0);
    Assert.assertFalse(chunkWriter.isEmpty());
    Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
    Assert.assertEquals(1, chunkWriter.getCurrentPage());

    // write point
    for (int i = 30; i <= 50; i++) {
      chunkWriter.write(i, new TsPrimitiveType[] {new TsPrimitiveType.TsInt(i)});
    }
    Assert.assertEquals(2, chunkWriter.getCurrentPage());
    Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));

    // flush page
    timeStatistics = new TimeStatistics();
    timeStatistics.update(60);
    timeStatistics.update(70);
    statistics = new IntegerStatistics();
    statistics.update(60, 60);
    statistics.update(70, 70);
    chunkWriter.writePageHeaderAndDataIntoTimeBuff(
        ByteBuffer.allocate(1), new PageHeader(1, 1, timeStatistics));
    chunkWriter.writePageHeaderAndDataIntoValueBuff(
        ByteBuffer.allocate(1), new PageHeader(1, 1, statistics), 0);
    Assert.assertFalse(chunkWriter.isEmpty());
    Assert.assertTrue(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
    Assert.assertEquals(3, chunkWriter.getCurrentPage());

    // next chunk
    chunkWriter.setCompactChunkPlan(secondChunk);
    Assert.assertTrue(chunkWriter.isEmpty());
    Assert.assertTrue(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
    try {
      chunkWriter.writePageHeaderAndDataIntoTimeBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, timeStatistics));
      chunkWriter.writePageHeaderAndDataIntoValueBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, statistics), 0);
    } catch (Exception ignored) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testFollowedBatchChunkWriter2() throws PageException {
    List<CompactPagePlan> pages = new ArrayList<>();
    pages.add(new CompactPagePlan(30, 50, false));
    pages.add(new CompactPagePlan(70, 70, false));
    CompactChunkPlan chunk = new CompactChunkPlan(pages);

    IMeasurementSchema timeSchema = new MeasurementSchema("", TSDataType.TIMESTAMP);
    List<IMeasurementSchema> valueSchemas = new ArrayList<>();
    valueSchemas.add(new MeasurementSchema("s0", TSDataType.INT32));
    FollowingBatchCompactionAlignedChunkWriter chunkWriter =
        new FollowingBatchCompactionAlignedChunkWriter(timeSchema, valueSchemas, chunk);
    Assert.assertTrue(chunkWriter.isEmpty());
    Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
    chunkWriter.sealCurrentPage();
    Assert.assertTrue(chunkWriter.isEmpty());
    Assert.assertEquals(0, chunkWriter.getCurrentPage());

    // write point
    for (int i = 30; i <= 50; i++) {
      if (i == 40) {
        Assert.assertFalse(chunkWriter.checkIsUnsealedPageOverThreshold(0, 0, false));
        Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));
      }
      chunkWriter.write(i, new TsPrimitiveType[] {new TsPrimitiveType.TsInt(i)});
    }
    Assert.assertEquals(1, chunkWriter.getCurrentPage());
    Assert.assertFalse(chunkWriter.checkIsUnsealedPageOverThreshold(0, 0, false));
    Assert.assertFalse(chunkWriter.checkIsChunkSizeOverThreshold(0, 0, false));

    chunkWriter.write(60, new TsPrimitiveType[] {new TsPrimitiveType.TsInt(60)});
    try {
      chunkWriter.sealCurrentPage();
    } catch (Exception ignored) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testMapAlignedChunkMetadata1() {
    List<IChunkMetadata> valueChunkMetadatas =
        Arrays.asList(
            new ChunkMetadata("s0", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            new ChunkMetadata("s1", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            new ChunkMetadata("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            null,
            new ChunkMetadata(
                "s4", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null));
    AlignedChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT32),
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s4", TSDataType.INT32));
    AlignedChunkMetadata newAlignedChunkMetadata =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata.getValueChunkMetadataList().stream()
            .map(IChunkMetadata::getMeasurementUid)
            .collect(Collectors.toList()),
        Arrays.asList("s0", "s1", "s2", "s4"));
  }

  @Test
  public void testMapAlignedChunkMetadata2() {
    List<IChunkMetadata> valueChunkMetadatas =
        Arrays.asList(
            new ChunkMetadata("s4", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            null);
    AlignedChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT32),
            new MeasurementSchema("s4", TSDataType.INT32));
    AlignedChunkMetadata newAlignedChunkMetadata =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata.getValueChunkMetadataList().stream()
            .map(chunkMetadata -> chunkMetadata == null ? null : chunkMetadata.getMeasurementUid())
            .collect(Collectors.toList()),
        Arrays.asList(null, "s4"));
  }

  @Test
  public void testMapAlignedChunkMetadata3() {
    List<IChunkMetadata> valueChunkMetadatas =
        Arrays.asList(
            new ChunkMetadata("s0", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            new ChunkMetadata("s1", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            new ChunkMetadata(
                "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null));
    AlignedChunkMetadata alignedChunkMetadata1 =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);

    valueChunkMetadatas =
        Arrays.asList(
            new ChunkMetadata("s3", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null),
            new ChunkMetadata(
                "s4", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null));
    AlignedChunkMetadata alignedChunkMetadata2 =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT32),
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32),
            new MeasurementSchema("s4", TSDataType.INT32));
    AlignedChunkMetadata newAlignedChunkMetadata1 =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata1, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata1.getValueChunkMetadataList().stream()
            .map(chunkMetadata -> chunkMetadata == null ? null : chunkMetadata.getMeasurementUid())
            .collect(Collectors.toList()),
        Arrays.asList("s0", "s1", "s2", null, null));

    AlignedChunkMetadata newAlignedChunkMetadata2 =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata2, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata2.getValueChunkMetadataList().stream()
            .map(chunkMetadata -> chunkMetadata == null ? null : chunkMetadata.getMeasurementUid())
            .collect(Collectors.toList()),
        Arrays.asList(null, null, null, "s3", "s4"));
  }

  @Test
  public void testMapAlignedChunkMetadata4() {
    List<IChunkMetadata> valueChunkMetadatas =
        Arrays.asList(
            null,
            new ChunkMetadata(
                "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null));
    AlignedChunkMetadata alignedChunkMetadata1 =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);

    valueChunkMetadatas =
        Arrays.asList(
            null,
            null,
            null,
            null,
            new ChunkMetadata(
                "s4", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4, 0, null));
    AlignedChunkMetadata alignedChunkMetadata2 =
        new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s4", TSDataType.INT32));
    AlignedChunkMetadata newAlignedChunkMetadata1 =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata1, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata1.getValueChunkMetadataList().stream()
            .map(chunkMetadata -> chunkMetadata == null ? null : chunkMetadata.getMeasurementUid())
            .collect(Collectors.toList()),
        Arrays.asList("s2", null));

    AlignedChunkMetadata newAlignedChunkMetadata2 =
        AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
            alignedChunkMetadata2, measurementSchemas);
    Assert.assertEquals(
        newAlignedChunkMetadata2.getValueChunkMetadataList().stream()
            .map(chunkMetadata -> chunkMetadata == null ? null : chunkMetadata.getMeasurementUid())
            .collect(Collectors.toList()),
        Arrays.asList(null, "s4"));
  }
}
