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
import org.apache.tsfile.utils.Binary;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ChunkDataDirectWriteTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final int DEFAULT_POINT_COUNT = 100;
  private static final IDeviceID DEFAULT_DEVICE = new StringArrayDeviceID("root", "sg", "d1");

  // -------------------------------------------------------------------------
  // Direct Write (Bypass SerDe) Tests
  // -------------------------------------------------------------------------

  // An entire non-aligned chunk reaches the writer untouched, with no serialize/deserialize round
  // trip in between
  @Test
  public void testNonAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData(DEFAULT_DEVICE, "s1");
    chunkData.setNotDecode();

    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createStatistics(TSDataType.INT32)).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    final ArgumentCaptor<Chunk> chunkCaptor = ArgumentCaptor.forClass(Chunk.class);
    Mockito.verify(writer).writeChunk(chunkCaptor.capture());
    assertNotNull(chunkCaptor.getValue());
  }

  // The same direct write holds for aligned chunk data, including a time chunk header
  @Test
  public void testAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData(DEFAULT_DEVICE);
    chunkData.setNotDecode();

    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createStatistics(TSDataType.INT32)).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    final ArgumentCaptor<Chunk> chunkCaptor = ArgumentCaptor.forClass(Chunk.class);
    Mockito.verify(writer).writeChunk(chunkCaptor.capture());
    assertNotNull(chunkCaptor.getValue());
  }

  // -------------------------------------------------------------------------
  // Header Re-encoding & Wire-Safety Tests
  // -------------------------------------------------------------------------

  // A decoded time page arrives as Object[] holding nulls and still rebuilds one valid time chunk
  @Test
  public void testAlignedChunkDataAcceptsObjectArrayFromTimePageDecode() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData(DEFAULT_DEVICE);

    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {null, null}, 2);
    chunkData.endChunk();

    assertEquals(1, chunkData.getChunks().size());
    assertEquals(2, chunkData.getChunks().get(0).getChunkStatistic().getCount());
  }

  // A re-encoded non-aligned chunk gets a header whose data size and page count match the new body
  @Test
  public void testReencodedNonAlignedChunkRebuildsHeader() {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData(DEFAULT_DEVICE, "s1");
    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {1, 2}, 2);
    chunkData.endChunk();

    final Chunk chunk = chunkData.getChunks().get(0);
    assertEquals(chunk.getData().remaining(), chunk.getHeader().getDataSize());
    assertEquals(1, chunk.getHeader().getNumOfPages());
  }

  // The rebuilt header survives the wire, so a remote writer never rebuilds a chunk from the stale
  // source header
  @Test
  public void testSerializedNonAlignedChunkKeepsReencodedHeader() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData(DEFAULT_DEVICE, "s1");
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

  // endChunk seals only the physical chunk being written, so the time chunk and the value chunk
  // stay separate and each keeps its column mask
  @Test
  public void testAlignedChunkEndBuildsOnlyCurrentPhysicalChunk() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData(DEFAULT_DEVICE);
    final long[] times = new long[] {1L, 2L};
    chunkData.writeDecodePage(times, new Object[] {null, null}, times.length);
    chunkData.endChunk();

    final Chunk timeChunk = chunkData.getChunks().get(0);
    assertEquals(1, chunkData.getChunks().size());
    assertEquals(timeChunk.getData().remaining(), timeChunk.getHeader().getDataSize());
    assertEquals(TIME_COLUMN_MASK, timeChunk.getHeader().getChunkType() & TIME_COLUMN_MASK);

    chunkData.addValueChunk(createChunkHeader("s1", TSDataType.INT32));
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

  // -------------------------------------------------------------------------
  // Time Partition Filtering Tests
  // -------------------------------------------------------------------------

  // Points outside the current time partition are dropped from a non-aligned chunk
  @Test
  public void testNonAlignedChunkSelectsPointsFromCurrentTimePartition() {
    final long partitionStart = TimePartitionUtils.getTimePartitionInterval();
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false,
                DEFAULT_DEVICE,
                createChunkHeader("s1", TSDataType.INT32),
                new TTimePartitionSlot(partitionStart));

    chunkData.writeDecodePage(new long[] {1L, partitionStart + 1}, new Object[] {1, 2}, 1);
    chunkData.endChunk();

    final Statistics<?> statistics = chunkData.getChunks().get(0).getChunkStatistic();
    assertEquals(1, statistics.getCount());
    assertEquals(partitionStart + 1, statistics.getStartTime());
  }

  // Points outside the current time partition are dropped from an aligned chunk as well
  @Test
  public void testAlignedChunkSelectsPointsFromCurrentTimePartition() throws Exception {
    final long partitionStart = TimePartitionUtils.getTimePartitionInterval();
    final AlignedChunkData chunkData =
        (AlignedChunkData)
            ChunkData.createChunkData(
                true,
                DEFAULT_DEVICE,
                createTimeChunkHeader(),
                new TTimePartitionSlot(partitionStart));

    chunkData.writeDecodePage(new long[] {1L, partitionStart + 1}, new Object[] {null, null}, 1);
    chunkData.endChunk();

    final Statistics<?> statistics = chunkData.getChunks().get(0).getChunkStatistic();
    assertEquals(1, statistics.getCount());
    assertEquals(partitionStart + 1, statistics.getStartTime());
  }

  // -------------------------------------------------------------------------
  // Comprehensive Type Matrix & Compression Tests
  // -------------------------------------------------------------------------

  // Every supported data type rebuilds a header whose data size matches the re-encoded body
  @Test
  public void testNonAlignedChunkDataRebuildsHeaderForAllDataTypes() {
    final TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.BOOLEAN,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.TEXT,
          TSDataType.BLOB,
          TSDataType.DATE,
          TSDataType.TIMESTAMP
        };

    for (TSDataType type : dataTypes) {
      final NonAlignedChunkData chunkData =
          (NonAlignedChunkData)
              ChunkData.createChunkData(
                  false,
                  DEFAULT_DEVICE,
                  new ChunkHeader(
                      "s_" + type.name(), 0, type, CompressionType.SNAPPY, TSEncoding.PLAIN, 0),
                  new TTimePartitionSlot(0L));

      final long[] times = new long[DEFAULT_POINT_COUNT];
      final Object[] values = new Object[DEFAULT_POINT_COUNT];
      for (int i = 0; i < DEFAULT_POINT_COUNT; i++) {
        times[i] = i + 1L;
        values[i] = generateSampleValue(type, i);
      }

      chunkData.writeDecodePage(times, values, DEFAULT_POINT_COUNT);
      chunkData.endChunk();

      final Chunk chunk = chunkData.getChunks().get(0);
      assertEquals(
          "Data size mismatch for type: " + type,
          chunk.getData().remaining(),
          chunk.getHeader().getDataSize());
      assertEquals(DEFAULT_POINT_COUNT, chunk.getChunkStatistic().getCount());
    }
  }

  // -------------------------------------------------------------------------
  // End-to-End Layout, Mixed-Device & Shuffled Dispatch Tests
  // -------------------------------------------------------------------------

  // A chunk arriving after a later one is backfilled into its reserved range without growing the
  // file, and both series stay readable
  @Test
  public void testPrecalculatedWriterHandlesOutOfOrderChunkDispatch() throws Exception {
    final File tsFile =
        tempFolder.newFile("out-of-order-chunk-write" + TsFileConstant.TSFILE_SUFFIX);

    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final IDeviceID firstDevice = new StringArrayDeviceID("root", "order", "d0");
    final IDeviceID secondDevice = new StringArrayDeviceID("root", "order", "d1");

    final NonAlignedChunkData firstChunkData =
        createPopulatedNonAlignedChunkData(firstDevice, "s0", 0, 4);
    final NonAlignedChunkData secondChunkData =
        createPopulatedNonAlignedChunkData(secondDevice, "s0", 10, 4);
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
      // 1. Write the second chunk first, creating a physical gap at the beginning of the file
      writer.writeChunk(
          secondChunkData.getDevice(),
          secondChunkData.isAligned(),
          secondLayout.chunkGroupHeaderOffset(),
          secondLayout.firstChunkOfGroup(),
          secondChunk,
          secondLayout.offset());
      writer.getOutput().flush();
      assertEquals(expectedLengthAfterSecondChunk, tsFile.length());

      // 2. Backfill the first chunk into the precalculated gap without expanding file bounds
      writer.writeChunk(
          firstChunkData.getDevice(),
          firstChunkData.isAligned(),
          firstLayout.chunkGroupHeaderOffset(),
          firstLayout.firstChunkOfGroup(),
          firstChunk,
          firstLayout.offset());
      writer.getOutput().flush();
      assertEquals(expectedLengthAfterSecondChunk, tsFile.length());
    } finally {
      assertSeriesReadable(tsFile, firstDevice, "s0", false, 4);
      assertSeriesReadable(tsFile, secondDevice, "s0", false, 4);
    }
  }

  // Covering the distance to a piece that lies ahead must skip the chunks written in the meantime
  // instead of padding over them, or the series that arrived earlier reads as zeros
  @Test
  public void testHoleFillDoesNotOverwriteAlreadyWrittenChunk() throws Exception {
    final File tsFile = tempFolder.newFile("hole-fill-chunk-write" + TsFileConstant.TSFILE_SUFFIX);
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final IDeviceID firstDevice = new StringArrayDeviceID("root", "fill", "d0");
    final IDeviceID secondDevice = new StringArrayDeviceID("root", "fill", "d1");

    final NonAlignedChunkData firstDeviceChunk =
        createPopulatedNonAlignedChunkData(firstDevice, "s0", 0, 4);
    final NonAlignedChunkData secondDeviceFirstChunk =
        createPopulatedNonAlignedChunkData(secondDevice, "s0", 10, 4);
    final NonAlignedChunkData secondDeviceSecondChunk =
        createPopulatedNonAlignedChunkData(secondDevice, "s1", 20, 4);
    calculator.assign(firstDeviceChunk);
    calculator.assign(secondDeviceFirstChunk);
    calculator.assign(secondDeviceSecondChunk);

    try (final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(tsFile)) {
      // 1. The piece that the layout puts furthest ahead is written first, which leaves the region
      // of
      //    the pieces behind it as zeros
      writeSingleChunk(writer, secondDeviceFirstChunk);
      // 2. The piece behind it is written at its own offset, which moves the channel back
      writeSingleChunk(writer, firstDeviceChunk);
      // 3. The last piece lies ahead again, so the distance between the current position and its
      //    offset covers the chunk written in step 1
      writeSingleChunk(writer, secondDeviceSecondChunk);
      writer.getOutput().flush();
    }

    assertSeriesReadable(tsFile, firstDevice, "s0", false, 4);
    assertSeriesReadable(tsFile, secondDevice, "s0", false, 4);
    assertSeriesReadable(tsFile, secondDevice, "s1", false, 4);
  }

  // Mixed aligned and non-aligned chunks of several types, dispatched in shuffled order, still form
  // a readable file with two devices
  @Test
  public void testMixedDevicesWithAllTypesAndShuffledDispatch() throws Exception {
    final File tsFile = tempFolder.newFile("mixed-all-types" + TsFileConstant.TSFILE_SUFFIX);
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<ChunkData> chunkDataList = new ArrayList<>();

    // 1. Setup non-aligned devices across various primitive and variable-length data types
    final IDeviceID nonAlignedDevice = new StringArrayDeviceID("root", "mixed", "d_non_aligned");
    final TSDataType[] nonAlignedTypes =
        new TSDataType[] {TSDataType.INT32, TSDataType.FLOAT, TSDataType.TEXT, TSDataType.BOOLEAN};

    for (TSDataType type : nonAlignedTypes) {
      final NonAlignedChunkData chunk =
          (NonAlignedChunkData)
              ChunkData.createChunkData(
                  false,
                  nonAlignedDevice,
                  createChunkHeader("s_" + type.name(), type),
                  new TTimePartitionSlot(0L));
      final long[] times = new long[DEFAULT_POINT_COUNT];
      final Object[] values = new Object[DEFAULT_POINT_COUNT];
      for (int i = 0; i < DEFAULT_POINT_COUNT; i++) {
        times[i] = i + 1L;
        values[i] = generateSampleValue(type, i);
      }
      chunk.writeDecodePage(times, values, DEFAULT_POINT_COUNT);
      chunk.endChunk();
      calculator.assign(chunk);
      chunkDataList.add(chunk);
    }

    // 2. Setup aligned device with time column and sparse value columns (null bitmaps)
    final IDeviceID alignedDevice = new StringArrayDeviceID("root", "mixed", "d_aligned");
    final AlignedChunkData alignedChunk =
        (AlignedChunkData)
            ChunkData.createChunkData(
                true, alignedDevice, createTimeChunkHeader(), new TTimePartitionSlot(0L));

    final long[] times = new long[DEFAULT_POINT_COUNT];
    for (int i = 0; i < DEFAULT_POINT_COUNT; i++) {
      times[i] = i + 1L;
    }
    alignedChunk.writeDecodePage(times, new Object[DEFAULT_POINT_COUNT], DEFAULT_POINT_COUNT);
    alignedChunk.endChunk();

    // Value sensor 1: INT64 with sparse nulls
    alignedChunk.addValueChunk(createChunkHeader("s_int64", TSDataType.INT64));
    final TsPrimitiveType[] int64Values = new TsPrimitiveType[DEFAULT_POINT_COUNT];
    for (int i = 0; i < DEFAULT_POINT_COUNT; i++) {
      int64Values[i] =
          (i % 5 == 0) ? null : (TsPrimitiveType) generateSamplePrimitive(TSDataType.INT64, i);
    }
    alignedChunk.writeDecodeValuePage(times, int64Values, TSDataType.INT64);
    alignedChunk.endChunk();

    // Value sensor 2: DOUBLE dense points
    alignedChunk.addValueChunk(createChunkHeader("s_double", TSDataType.DOUBLE));
    final TsPrimitiveType[] doubleValues = new TsPrimitiveType[DEFAULT_POINT_COUNT];
    for (int i = 0; i < DEFAULT_POINT_COUNT; i++) {
      doubleValues[i] = (TsPrimitiveType) generateSamplePrimitive(TSDataType.DOUBLE, i);
    }
    alignedChunk.writeDecodeValuePage(times, doubleValues, TSDataType.DOUBLE);
    alignedChunk.endChunk();

    calculator.assign(alignedChunk);
    chunkDataList.add(alignedChunk);

    // 3. Decompose all chunk data into atomic write tasks and shuffle dispatch order
    final List<WriteTask> tasks = new ArrayList<>();
    for (ChunkData chunkData : chunkDataList) {
      final ChunkLayout layout = chunkData.getChunkLayout();
      final List<Chunk> chunks = chunkData.getChunks();
      long currentOffset = layout.offset();

      for (int i = 0; i < chunks.size(); i++) {
        final Chunk chunk = chunks.get(i);
        final long length =
            serializeChunkHeaderSize(chunk.getHeader()) + chunk.getData().remaining();
        tasks.add(
            new WriteTask(
                chunkData.getDevice(),
                chunkData.isAligned(),
                layout.chunkGroupHeaderOffset(),
                layout.firstChunkOfGroup() && i == 0,
                chunk,
                currentOffset));
        currentOffset += length;
      }
    }

    Collections.shuffle(tasks, new Random(42));

    // 4. Concurrently execute out-of-order writes
    try (final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(tsFile)) {
      for (WriteTask task : tasks) {
        writer.writeChunk(
            task.device(),
            task.isAligned(),
            task.chunkGroupOffset(),
            task.firstOfGroup(),
            task.chunk(),
            task.offset());
      }
      writer.getOutput().flush();
    }

    // 5. Query verification via TsFileReader
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      assertEquals(2, reader.getAllDevices().size());
    }

    assertSeriesReadable(tsFile, nonAlignedDevice, "s_INT32", false, DEFAULT_POINT_COUNT);
    assertSeriesReadable(tsFile, nonAlignedDevice, "s_TEXT", false, DEFAULT_POINT_COUNT);
    assertSeriesReadable(tsFile, alignedDevice, "s_double", true, DEFAULT_POINT_COUNT);
  }

  // -------------------------------------------------------------------------
  // Helper Classes & Factory Methods
  // -------------------------------------------------------------------------

  private record WriteTask(
      IDeviceID device,
      boolean isAligned,
      long chunkGroupOffset,
      boolean firstOfGroup,
      Chunk chunk,
      long offset) {}

  private static Object generateSampleValue(final TSDataType type, final int seed) {
    return switch (type) {
      case BOOLEAN -> (seed % 2 == 0);
      case INT32 -> seed;
      case INT64, TIMESTAMP -> (long) seed * 1000L;
      case FLOAT -> seed + 0.5f;
      case DOUBLE -> seed + 0.5555d;
      case TEXT -> new Binary(("sample_text_" + seed).getBytes(StandardCharsets.UTF_8));
      case BLOB -> new Binary(new byte[] {(byte) seed, 0x01});
      // IoTDB stores DATE as the number of days since the epoch, so the sample value is an int
      case DATE -> (int) LocalDate.of(2026, 1, (seed % 28) + 1).toEpochDay();
      default -> throw new IllegalArgumentException("Unsupported data type: " + type);
    };
  }

  private static Object generateSamplePrimitive(final TSDataType type, final int seed) {
    return TsPrimitiveType.getByType(type, generateSampleValue(type, seed));
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device, final String measurement) {
    return (NonAlignedChunkData)
        ChunkData.createChunkData(
            false,
            device,
            createChunkHeader(measurement, TSDataType.INT32),
            new TTimePartitionSlot(0L));
  }

  private static NonAlignedChunkData createPopulatedNonAlignedChunkData(
      final IDeviceID device, final String measurement, final int startValue, final int count) {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData(device, measurement);
    final long[] times = new long[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      times[i] = i + 1L;
      values[i] = startValue + i;
    }
    chunkData.writeDecodePage(times, values, count);
    chunkData.endChunk();
    return chunkData;
  }

  /** Writes a piece that carries exactly one physical chunk at the offset the layout gives it. */
  private static void writeSingleChunk(
      final TsFilePrecalculatedChunkWriter writer, final ChunkData chunkData) throws IOException {
    final ChunkLayout layout = chunkData.getChunkLayout();
    writer.writeChunk(
        chunkData.getDevice(),
        chunkData.isAligned(),
        layout.chunkGroupHeaderOffset(),
        layout.firstChunkOfGroup(),
        chunkData.getChunks().get(0),
        layout.offset());
  }

  private static AlignedChunkData createAlignedTimeChunkData(final IDeviceID device) {
    return (AlignedChunkData)
        ChunkData.createChunkData(
            true, device, createTimeChunkHeader(), new TTimePartitionSlot(0L));
  }

  private static ChunkHeader createChunkHeader(
      final String measurement, final TSDataType dataType) {
    return new ChunkHeader(
        measurement, 0, dataType, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }

  private static ChunkHeader createTimeChunkHeader() {
    return new ChunkHeader(
        "",
        1024,
        TSDataType.VECTOR,
        CompressionType.UNCOMPRESSED,
        TSEncoding.PLAIN,
        2,
        TIME_COLUMN_MASK);
  }

  // Statistics exposes one update overload per value type instead of a generic one, so the sample
  // value is picked per type here rather than being passed in as an Object
  private static Statistics<?> createStatistics(final TSDataType dataType) {
    final Statistics<?> statistics = Statistics.getStatsByType(dataType);
    switch (dataType) {
      case BOOLEAN -> statistics.update(1L, true);
      case INT32 -> statistics.update(1L, 1);
      case INT64, TIMESTAMP -> statistics.update(1L, 1L);
      case FLOAT -> statistics.update(1L, 1.0f);
      case DOUBLE -> statistics.update(1L, 1.0d);
      case TEXT, BLOB ->
          statistics.update(1L, new Binary("sample".getBytes(StandardCharsets.UTF_8)));
      default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
    return statistics;
  }

  private static int serializeChunkHeaderSize(final ChunkHeader chunkHeader) throws Exception {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return chunkHeader.serializeTo(output);
    }
  }

  private static void assertSeriesReadable(
      final File tsFile,
      final IDeviceID device,
      final String measurement,
      final boolean isAligned,
      final int expectedPointCount)
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
        assertNotNull(field);
        count++;
      }
      assertTrue(
          "Read " + count + " points of " + measurement + " instead of 1.." + expectedPointCount,
          count > 0 && count <= expectedPointCount);
    }
  }
}
