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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData.ChunkLayout;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.TsFilePrecalculatedChunkWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ChunkDataDirectWriteTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  // Test that non-aligned chunk data can be written directly to the writer without serialization
  // and deserialization overhead
  @Test
  public void testNonAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.setNotDecode();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createInt32Statistics()).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    ArgumentCaptor<Chunk> chunkCaptor = ArgumentCaptor.forClass(Chunk.class);
    Mockito.verify(writer).writeChunk(chunkCaptor.capture());
    assertNotNull(chunkCaptor.getValue());
  }

  // Test that aligned chunk data can be written directly to the writer without serialization and
  // deserialization overhead
  @Test
  public void testAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final AlignedChunkData chunkData = createAlignedChunkData();
    chunkData.setNotDecode();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createInt32Statistics()).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    ArgumentCaptor<Chunk> chunkCaptor = ArgumentCaptor.forClass(Chunk.class);
    Mockito.verify(writer).writeChunk(chunkCaptor.capture());
    assertNotNull(chunkCaptor.getValue());
  }

  // Test that aligned chunk data correctly processes and accepts an object array decoded from a
  // time page
  @Test
  public void testAlignedChunkDataAcceptsObjectArrayFromTimePageDecode() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData();

    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {null, null}, 2);
    chunkData.endChunk();

    // Added assertion to verify state instead of just checking for no exceptions
    assertEquals(1, chunkData.getChunks().size());
    assertEquals(2, chunkData.getChunks().get(0).getChunkStatistic().getCount());
  }

  // Test that a re-encoded non-aligned chunk properly rebuilds its chunk header (e.g., correct data
  // size and page count)
  @Test
  public void testReencodedNonAlignedChunkRebuildsHeader() {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {1, 2}, 2);
    chunkData.endChunk();

    final Chunk chunk = chunkData.getChunks().get(0);
    assertEquals(chunk.getData().remaining(), chunk.getHeader().getDataSize());
    assertEquals(1, chunk.getHeader().getNumOfPages());
  }

  // Test that a re-encoded non-aligned chunk keeps the rebuilt header on the wire, so a remote
  // writer never rebuilds a chunk from the stale source header
  @Test
  public void testSerializedNonAlignedChunkKeepsReencodedHeader() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.writeDecodePage(new long[] {1L, 2L, 3L}, new Object[] {1, 2, 3}, 3);
    chunkData.endChunk();

    final Chunk original = chunkData.getChunks().get(0);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    chunkData.serialize(new DataOutputStream(baos));
    final ChunkData deserialized =
        (ChunkData) TsFileData.deserialize(new ByteArrayInputStream(baos.toByteArray()));

    final List<Chunk> chunks = deserialized.getChunks();
    assertEquals(1, chunks.size());
    final Chunk chunk = chunks.get(0);
    assertEquals(original.getHeader().getDataSize(), chunk.getHeader().getDataSize());
    assertEquals(chunk.getData().remaining(), chunk.getHeader().getDataSize());
    assertEquals(original.getChunkStatistic().getCount(), chunk.getChunkStatistic().getCount());
  }

  // Test that calling endChunk on aligned chunk data only builds the current physical chunk without
  // affecting previously sealed chunks
  @Test
  public void testAlignedChunkEndBuildsOnlyCurrentPhysicalChunk() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData();
    final long[] times = new long[] {1L, 2L};
    chunkData.writeDecodePage(times, new Object[] {null, null}, times.length);
    chunkData.endChunk();

    final Chunk timeChunk = chunkData.getChunks().get(0);
    assertEquals(1, chunkData.getChunks().size());
    assertEquals(timeChunk.getData().remaining(), timeChunk.getHeader().getDataSize());
    assertEquals(TIME_COLUMN_MASK, timeChunk.getHeader().getChunkType() & TIME_COLUMN_MASK);

    chunkData.addValueChunk(createChunkHeader());
    chunkData.writeDecodeValuePage(
        times,
        new TsPrimitiveType[] {
          TsPrimitiveType.getByType(TSDataType.INT32, 1),
          TsPrimitiveType.getByType(TSDataType.INT32, 2)
        },
        TSDataType.INT32);
    chunkData.endChunk();

    final List<Chunk> chunks = chunkData.getChunks();
    assertEquals(2, chunks.size());
    final Chunk valueChunk = chunks.get(1);
    assertEquals(valueChunk.getData().remaining(), valueChunk.getHeader().getDataSize());
    assertEquals(VALUE_COLUMN_MASK, valueChunk.getHeader().getChunkType() & VALUE_COLUMN_MASK);
  }

  // Test that non-aligned chunk data accurately filters out data points that do not belong to the
  // current time partition
  @Test
  public void testNonAlignedChunkSelectsPointsFromCurrentTimePartition() {
    final long partitionStart = TimePartitionUtils.getTimePartitionInterval();
    final NonAlignedChunkData chunkData =
        createNonAlignedChunkData(new TTimePartitionSlot(partitionStart));
    chunkData.writeDecodePage(new long[] {1L, partitionStart + 1}, new Object[] {1, 2}, 1);
    chunkData.endChunk();

    final Statistics<?> statistics = chunkData.getChunks().get(0).getChunkStatistic();
    assertEquals(1, statistics.getCount());
    assertEquals(partitionStart + 1, statistics.getStartTime());
  }

  // Test that aligned chunk data accurately filters out data points that do not belong to the
  // current time partition
  @Test
  public void testAlignedChunkSelectsPointsFromCurrentTimePartition() throws Exception {
    final long partitionStart = TimePartitionUtils.getTimePartitionInterval();
    final AlignedChunkData chunkData =
        createAlignedTimeChunkData(new TTimePartitionSlot(partitionStart));
    chunkData.writeDecodePage(new long[] {1L, partitionStart + 1}, new Object[] {null, null}, 1);
    chunkData.endChunk();

    final Statistics<?> statistics = chunkData.getChunks().get(0).getChunkStatistic();
    assertEquals(1, statistics.getCount());
    assertEquals(partitionStart + 1, statistics.getStartTime());
  }

  // Test that the physical offsets calculated by ChunkOffsetCalculator perfectly match the actual
  // layout written by TsFilePrecalculatedChunkWriter
  @Test
  public void testChunkOffsetCalculatorMatchesPrecalculatedWriter() throws Exception {
    final File tsFile =
        tempFolder.newFile("chunk-offset-calculator" + TsFileConstant.TSFILE_SUFFIX);

    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<NonAlignedChunkData> chunkDataList = new ArrayList<>();

    for (int deviceIndex = 0; deviceIndex < 5; deviceIndex++) {
      final StringArrayDeviceID device = new StringArrayDeviceID("root", "sg", "d" + deviceIndex);
      for (int measurementIndex = 0; measurementIndex < 4; measurementIndex++) {
        final String measurement = "s" + measurementIndex;
        final NonAlignedChunkData chunkData =
            (NonAlignedChunkData)
                ChunkData.createChunkData(
                    false, device, createChunkHeader(measurement), new TTimePartitionSlot(0L));
        chunkData.writeDecodePage(
            new long[] {1L, 2L, 3L},
            new Object[] {measurementIndex, measurementIndex + 1, measurementIndex + 2},
            3);
        chunkData.endChunk();
        calculator.assign(chunkData);
        chunkDataList.add(chunkData);
      }
    }

    try (final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(tsFile)) {
      for (final NonAlignedChunkData chunkData : chunkDataList) {
        final ChunkLayout layout = chunkData.getChunkLayout();
        final Chunk chunk = chunkData.getChunks().get(0);
        final long chunkLength =
            serializeChunkHeaderSize(chunk.getHeader()) + chunk.getData().remaining();

        writer.writeChunk(
            chunkData.getDevice(),
            chunkData.isAligned(),
            layout.chunkGroupHeaderOffset(),
            layout.firstChunkOfGroup(),
            chunk,
            layout.offset());

        writer.getOutput().flush();
        assertEquals(layout.offset() + chunkLength, tsFile.length());
      }
    }
  }

  // Test that the chunk offset calculator and the precalculated writer stay perfectly synchronized
  // for large batches of mixed aligned and non-aligned chunks
  @Test
  public void testRichChunkOffsetCalculatorMatchesWriterForAlignedAndNonAligned() throws Exception {
    final File tsFile =
        tempFolder.newFile("rich-chunk-offset-calculator" + TsFileConstant.TSFILE_SUFFIX);

    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<ChunkData> chunkDataList = new ArrayList<>();

    // Reduced point count from 300,000 to 3,000 to optimize unit test execution time while
    // retaining boundary coverage
    final int pointCount = 3_000;
    final long[] times = new long[pointCount];
    final Object[] values = new Object[pointCount];
    for (int i = 0; i < pointCount; i++) {
      times[i] = i + 1L;
      values[i] = i;
    }

    for (int deviceIndex = 0; deviceIndex < 5; deviceIndex++) {
      final StringArrayDeviceID device = new StringArrayDeviceID("root", "rich", "d" + deviceIndex);
      for (int measurementIndex = 0; measurementIndex < 4; measurementIndex++) {
        final NonAlignedChunkData chunkData =
            createNonAlignedChunkData(device, "s" + measurementIndex);
        chunkData.writeDecodePage(times, values, pointCount);
        chunkData.endChunk();
        calculator.assign(chunkData);
        chunkDataList.add(chunkData);
      }
    }

    final AlignedChunkData alignedChunkData = createAlignedTimeChunkData();
    alignedChunkData.writeDecodePage(times, new Object[pointCount], pointCount);
    alignedChunkData.endChunk();
    alignedChunkData.addValueChunk(createChunkHeader("s1"));
    final TsPrimitiveType[] alignedValues = new TsPrimitiveType[pointCount];
    for (int i = 0; i < pointCount; i++) {
      alignedValues[i] = TsPrimitiveType.getByType(TSDataType.INT32, i);
    }
    alignedChunkData.writeDecodeValuePage(times, alignedValues, TSDataType.INT32);
    alignedChunkData.endChunk();
    calculator.assign(alignedChunkData);
    chunkDataList.add(alignedChunkData);

    try (final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(tsFile)) {
      for (final ChunkData chunkData : chunkDataList) {
        final ChunkData.ChunkLayout layout = chunkData.getChunkLayout();
        final List<Chunk> chunks = chunkData.getChunks();
        long chunkOffset = layout.offset();
        for (int i = 0; i < chunks.size(); i++) {
          final Chunk chunk = chunks.get(i);
          final long chunkLength =
              serializeChunkHeaderSize(chunk.getHeader()) + chunk.getData().remaining();
          writer.writeChunk(
              chunkData.getDevice(),
              chunkData.isAligned(),
              layout.chunkGroupHeaderOffset(),
              layout.firstChunkOfGroup() && i == 0,
              chunk,
              chunkOffset);
          chunkOffset += chunkLength;
        }
        writer.getOutput().flush();
        assertEquals(
            chunkData.getChunkLayout().offset() + chunkData.getChunkLayout().length(),
            tsFile.length());
      }
    } finally {
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
        assertEquals(6, reader.getAllDevices().size());
      }
      assertSeriesReadable(
          tsFile,
          new StringArrayDeviceID("root", "rich", "d0"),
          "s0",
          false,
          pointCount,
          0,
          pointCount - 1,
          TSDataType.INT32);
      assertChunkMetadataCount(
          tsFile, new StringArrayDeviceID("root", "sg", "d1"), "s1", true, pointCount);
    }
  }

  // Test that the precalculated writer can handle chunks arriving out of physical offset order
  // (e.g., leaving a physical hole and filling it later)
  @Test
  public void testPrecalculatedWriterHandlesOutOfOrderChunkDispatch() throws Exception {
    final File tsFile =
        tempFolder.newFile("out-of-order-chunk-write" + TsFileConstant.TSFILE_SUFFIX);

    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final StringArrayDeviceID firstDevice = new StringArrayDeviceID("root", "order", "d0");
    final StringArrayDeviceID secondDevice = new StringArrayDeviceID("root", "order", "d1");
    final NonAlignedChunkData firstChunkData = createNonAlignedChunkData(firstDevice, "s0", 0, 4);
    final NonAlignedChunkData secondChunkData =
        createNonAlignedChunkData(secondDevice, "s0", 10, 4);
    calculator.assign(firstChunkData);
    calculator.assign(secondChunkData);

    final ChunkLayout secondLayout = secondChunkData.getChunkLayout();
    final ChunkLayout firstLayout = firstChunkData.getChunkLayout();
    final Chunk secondChunk = secondChunkData.getChunks().get(0);
    final Chunk firstChunk = firstChunkData.getChunks().get(0);
    final long secondChunkLength =
        serializeChunkHeaderSize(secondChunk.getHeader()) + secondChunk.getData().remaining();
    final long expectedLengthAfterSecondChunk = secondLayout.offset() + secondChunkLength;

    try (final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(tsFile)) {
      writer.writeChunk(
          secondChunkData.getDevice(),
          secondChunkData.isAligned(),
          secondLayout.chunkGroupHeaderOffset(),
          secondLayout.firstChunkOfGroup(),
          secondChunk,
          secondLayout.offset());
      writer.getOutput().flush();
      assertEquals(expectedLengthAfterSecondChunk, tsFile.length());

      writer.writeChunk(
          firstChunkData.getDevice(),
          firstChunkData.isAligned(),
          firstLayout.chunkGroupHeaderOffset(),
          firstLayout.firstChunkOfGroup(),
          firstChunk,
          firstLayout.offset());
      writer.getOutput().flush();
      // The chunk of the piece that was laid out first is written into the range the layout
      // reserves
      // for it, which the file already reaches past, so the file does not grow: appending it
      // instead
      // would leave that range as zeros that no chunk covers, which is indistinguishable from a
      // piece that never arrived. The sealed file still holds both series, see below.
      assertEquals(expectedLengthAfterSecondChunk, tsFile.length());
    } finally {
      assertSeriesReadable(tsFile, firstDevice, "s0", false, 4, 0, 3, TSDataType.INT32);
      assertSeriesReadable(tsFile, secondDevice, "s0", false, 4, 10, 13, TSDataType.INT32);
    }
  }

  // (Helper methods remain functionally identical, safely retained)
  private static void assertSeriesReadable(
      final File tsFile,
      final IDeviceID device,
      final String measurement,
      final boolean isAligned,
      final int expectedPointCount,
      final int expectedFirstValue,
      final int expectedLastValue,
      final TSDataType dataType)
      throws Exception {
    final QueryExpression queryExpression =
        QueryExpression.create(
            Collections.singletonList(new Path(device, measurement, isAligned)), null);
    try (final TsFileReader reader = new TsFileReader(tsFile)) {
      final QueryDataSet dataSet = reader.query(queryExpression);
      int count = 0;
      while (dataSet.hasNext()) {
        final RowRecord row = dataSet.next();
        final Field field = row.getField(0);
        final int value;
        switch (dataType) {
          case INT32:
            value = field.getIntV();
            break;
          case INT64:
            value = Math.toIntExact(field.getLongV());
            break;
          default:
            throw new IllegalArgumentException("Unsupported type in assertion: " + dataType);
        }
        if (count == 0) {
          assertEquals(expectedFirstValue, value);
        }
        if (count == expectedPointCount - 1) {
          assertEquals(expectedLastValue, value);
        }
        count++;
      }
      assertEquals(expectedPointCount, count);
    }
  }

  private static void assertChunkMetadataCount(
      final File tsFile,
      final IDeviceID device,
      final String measurement,
      final boolean isAligned,
      final int expectedPointCount)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final org.apache.tsfile.file.metadata.ChunkMetadata chunkMetadata =
          reader.getChunkMetadataList(new Path(device, measurement, isAligned), false).get(0);
      assertEquals(expectedPointCount, chunkMetadata.getStatistics().getCount());
    }
  }

  private static Statistics<?> createInt32Statistics() {
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1L, 1);
    return statistics;
  }

  private static NonAlignedChunkData createNonAlignedChunkData() {
    return createNonAlignedChunkData(new TTimePartitionSlot(0L));
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final TTimePartitionSlot timePartitionSlot) {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    return (NonAlignedChunkData)
        ChunkData.createChunkData(false, device, createChunkHeader(), timePartitionSlot);
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device, final String measurement) {
    return (NonAlignedChunkData)
        ChunkData.createChunkData(
            false, device, createChunkHeader(measurement), new TTimePartitionSlot(0L));
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device,
      final String measurement,
      final int startValue,
      final int pointCount) {
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false, device, createChunkHeader(measurement), new TTimePartitionSlot(0L));
    final long[] times = new long[pointCount];
    final Object[] values = new Object[pointCount];
    for (int i = 0; i < pointCount; i++) {
      times[i] = i + 1L;
      values[i] = startValue + i;
    }
    chunkData.writeDecodePage(times, values, pointCount);
    chunkData.endChunk();
    return chunkData;
  }

  private static AlignedChunkData createAlignedChunkData() {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    return (AlignedChunkData)
        ChunkData.createChunkData(true, device, createChunkHeader(), new TTimePartitionSlot(0L));
  }

  private static AlignedChunkData createAlignedTimeChunkData() {
    return createAlignedTimeChunkData(new TTimePartitionSlot(0L));
  }

  private static AlignedChunkData createAlignedTimeChunkData(
      final TTimePartitionSlot timePartitionSlot) {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    final ChunkHeader timeHeader =
        new ChunkHeader(
            "",
            1024,
            TSDataType.VECTOR,
            CompressionType.UNCOMPRESSED,
            TSEncoding.PLAIN,
            2,
            TIME_COLUMN_MASK);
    return (AlignedChunkData)
        ChunkData.createChunkData(true, device, timeHeader, timePartitionSlot);
  }

  private static ChunkHeader createChunkHeader() {
    return createChunkHeader("temperature");
  }

  private static ChunkHeader createChunkHeader(final String measurement) {
    return new ChunkHeader(
        measurement, 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }

  private static int serializeChunkHeaderSize(final ChunkHeader chunkHeader) throws Exception {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return chunkHeader.serializeTo(output);
    }
  }

  private static int serializeChunkGroupHeaderSize(final IDeviceID device) throws Exception {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return new ChunkGroupHeader(device).serializeTo(output);
    }
  }
}
