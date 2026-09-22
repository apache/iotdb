/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.write.TsFilePrecalculatedChunkWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Verifies the two halves of LOAD staged-file recovery: the chunk ranges persisted next to a staged
 * file are enough to tell how much of it survived an interruption, and a writer resumed at that
 * point still seals a readable TsFile holding both the chunks written before the interruption and
 * the ones written after it.
 */
public class LoadTsFileResumeTest {

  private static final long FILE_HEADER_SIZE = 7L;
  private static final TTimePartitionSlot PARTITION_SLOT = new TTimePartitionSlot(0L);

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testResumableLengthStopsAtFirstHole() throws Exception {
    final File tsFile = temporaryFolder.newFile("a.tsfile");
    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);

    progress.recordChunk("root.sg.d1", false, FILE_HEADER_SIZE, 30L, true, createChunk(), 100L, 1L);
    // A following chunk of the same group starts where the previous one ended, so it carries no
    // group
    // header offset of its own.
    progress.recordChunk("root.sg.d1", false, 100L, 100L, false, createChunk(), 160L, 1L);
    // A second group written at 300 leaves the hole [160, 300), so only the contiguous prefix up to
    // 160 is known to be complete even though a later chunk already reached 400.
    progress.recordChunk("root.sg.d2", false, 300L, 320L, true, createChunk(), 400L, 1L);

    Assert.assertEquals(160L, progress.getResumableLength());
    Assert.assertEquals(160L, progress.getFirstHoleOffset());
    Assert.assertEquals(400L, progress.getTotalLength());
    // A file that long does look complete on its own, which is why the caller does not trust this
    // length alone: it also requires the recorded ranges to reach the end of the file, see
    // LoadTsFileManager#prepare.
    Assert.assertTrue(progress.isReady(400L));
    Assert.assertEquals(2, progress.getContiguousPrefix().size());

    // The records were appended out of physical order, so the computation has to sort them.
    final LoadTsFileProgress reloaded = new LoadTsFileProgress(tsFile);
    Assert.assertEquals(3, reloaded.readAllRecords().size());
    Assert.assertEquals(160L, reloaded.getResumableLength());
  }

  @Test
  public void testGapBeforeFirstChunkIsNotResumable() throws Exception {
    final File tsFile = temporaryFolder.newFile("b.tsfile");
    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);

    progress.recordChunk("root.sg.d1", false, 64L, 90L, true, createChunk(), 150L, 1L);

    Assert.assertEquals(-1L, progress.getResumableLength());
    Assert.assertEquals(FILE_HEADER_SIZE, progress.getFirstHoleOffset());
  }

  @Test
  public void testResumedWriterSealsReadableFile() throws Exception {
    final File taskDir = temporaryFolder.newFolder("task");
    final File tsFile = new File(taskDir, "root.sg.0.0.tsfile");
    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);

    // An interrupted writer: two chunks reached this node and were recorded, a third one was cut
    // off
    // half way through, and the writer was never sealed because PREPARE never ran.
    final NonAlignedChunkData temperatureFirst = createChunkData("temperature", 1);
    final NonAlignedChunkData humidity = createChunkData("humidity", 1);
    final TsFilePrecalculatedChunkWriter interrupted = new TsFilePrecalculatedChunkWriter(tsFile);
    writeAll(interrupted, progress, Arrays.asList(temperatureFirst, humidity));
    Assert.assertEquals(2, interrupted.getChunkMetadataCount());
    // The writer must not be closed here: PREPARE never ran, so the metadata zone does not exist
    // yet.
    // Flushing is only needed because the writer buffers, and the length of the data zone is what a
    // resumed writer has to be positioned at.
    interrupted.getOutput().flush();
    final long dataZoneLength = tsFile.length();

    // A third chunk that was cut off half way through: only these bytes exist, no record was
    // written
    // for them because the piece never completed.
    try (final FileChannel channel =
        FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      channel.position(channel.size());
      channel.write(ByteBuffer.wrap(new byte[64]));
    }
    Assert.assertEquals(dataZoneLength + 64L, tsFile.length());

    final long resumeOffset = progress.getResumableLength();
    Assert.assertEquals(dataZoneLength, resumeOffset);

    // Recovery drops the partial trailing write, then rebuilds the writer and hands it back the
    // metadata that was persisted while the chunks were written.
    try (final FileChannel truncatingChannel =
        FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      truncatingChannel.truncate(resumeOffset);
    }
    Assert.assertEquals(resumeOffset, tsFile.length());

    final List<LoadTsFileProgress.ChunkRangeRecord> records =
        new LoadTsFileProgress(tsFile).readAllRecords();

    final NonAlignedChunkData temperatureSecond = createChunkData("temperature", 2);
    try (final FileChannel channel =
        FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      channel.position(resumeOffset);
      final TsFilePrecalculatedChunkWriter resumed =
          new TsFilePrecalculatedChunkWriter(tsFile, channel);
      resumed.restoreChunkMetadata(restoreChunkMetadata(records));
      Assert.assertEquals(2, resumed.getChunkMetadataCount());
      Assert.assertEquals(resumeOffset, resumed.getOutput().getPosition());

      writeAll(resumed, progress, Collections.singletonList(temperatureSecond));
      Assert.assertEquals(3, resumed.getChunkMetadataCount());
      // Sealing writes the metadata zone on top of the chunks that were already on disk.
      resumed.close();
    }

    // The two chunks written before the interruption plus the one written by the resumed writer.
    Assert.assertEquals(3, new LoadTsFileProgress(tsFile).readAllRecords().size());
    Assert.assertTrue(tsFile.length() > resumeOffset);

    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      Assert.assertEquals(
          new HashSet<>(Arrays.asList("humidity", "temperature")),
          reader.getAllMeasurements().keySet());

      // Every measurement has to be restored under the very device it was written for. A device ID
      // is
      // not interchangeable with the string it prints as, so restoring it under a differently
      // shaped
      // device ID would split the measurement into two entries in this sealed file.
      final Map<String, Integer> chunkCountByMeasurement = new HashMap<>();
      for (final IDeviceID device : reader.getAllDevices()) {
        reader
            .readChunkMetadataInDevice(device)
            .forEach(
                (measurement, chunks) ->
                    chunkCountByMeasurement.merge(measurement, chunks.size(), Integer::sum));
      }
      Assert.assertEquals(2, chunkCountByMeasurement.get("temperature").intValue());
      Assert.assertEquals(1, chunkCountByMeasurement.get("humidity").intValue());
    }
  }

  /**
   * Mirrors {@code LoadTsFileManager.restoreChunkMetadata}, which is private, so the resume path
   * can be exercised without a DataRegion.
   */
  private static Map<IDeviceID, Map<String, List<IChunkMetadata>>> restoreChunkMetadata(
      final List<LoadTsFileProgress.ChunkRangeRecord> records) throws IOException {
    final Map<IDeviceID, Map<String, List<IChunkMetadata>>> result = new HashMap<>();
    for (final LoadTsFileProgress.ChunkRangeRecord record : records) {
      final ChunkHeader chunkHeader =
          LoadTsFileProgress.deserializeChunkHeader(record.chunkType(), record.chunkHeaderBytes());
      final Statistics<?> statistics =
          LoadTsFileProgress.deserializeStatistics(record.dataType(), record.statisticsBytes());
      final ChunkMetadata chunkMetadata =
          new ChunkMetadata(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType(),
              record.chunkOffset(),
              statistics);
      result
          .computeIfAbsent(record.deviceId(), ignored -> new HashMap<>())
          .computeIfAbsent(chunkHeader.getMeasurementID(), ignored -> new ArrayList<>())
          .add(chunkMetadata);
    }
    return result;
  }

  /** Writes every chunk of every {@link NonAlignedChunkData} and records its physical range. */
  private static void writeAll(
      final TsFilePrecalculatedChunkWriter writer,
      final LoadTsFileProgress progress,
      final List<NonAlignedChunkData> chunkDataList)
      throws Exception {
    long nextOffset = writer.getOutput().getPosition();
    IDeviceID currentDevice = null;
    long chunkGroupHeaderOffset = -1L;
    int chunkIndexInGroup = 0;

    for (final NonAlignedChunkData chunkData : chunkDataList) {
      final IDeviceID device = chunkData.getDevice();
      if (!device.equals(currentDevice)) {
        currentDevice = device;
        chunkGroupHeaderOffset = nextOffset;
        nextOffset += chunkGroupHeaderSize(device);
        chunkIndexInGroup = 0;
      }

      long chunkOffset = nextOffset;
      final List<Chunk> chunks = chunkData.getChunks();
      for (int i = 0; i < chunks.size(); i++) {
        final Chunk chunk = chunks.get(i);
        final boolean firstChunkOfGroup = chunkIndexInGroup == 0 && i == 0;
        final long chunkLength = chunkHeaderSize(chunk.getHeader()) + chunk.getData().remaining();

        final TsFilePrecalculatedChunkWriter.ChunkWriteResult result =
            writer.writeChunk(
                device,
                chunkData.isAligned(),
                chunkGroupHeaderOffset,
                firstChunkOfGroup,
                chunk,
                chunkOffset);

        final long physicalStart =
            firstChunkOfGroup ? result.actualChunkGroupHeaderOffset() : result.actualChunkOffset();
        progress.recordChunk(
            device.toString(),
            chunkData.isAligned(),
            result.actualChunkGroupHeaderOffset(),
            result.actualChunkOffset(),
            firstChunkOfGroup,
            chunk,
            result.actualChunkEndOffset(),
            physicalStart);

        chunkOffset += chunkLength;
        nextOffset = result.actualChunkEndOffset();
        chunkIndexInGroup++;
      }
    }
  }

  private static NonAlignedChunkData createChunkData(
      final String measurement, final int startValue) {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1", measurement);
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false, device, createChunkHeader(measurement), PARTITION_SLOT);
    final long[] times = {1L, 2L};
    final Object[] values = {startValue, startValue + 1};
    chunkData.writeDecodePage(times, values, 2);
    chunkData.endChunk();
    return chunkData;
  }

  private static ChunkHeader createChunkHeader(final String measurement) {
    return new ChunkHeader(
        measurement, 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }

  private static Chunk createChunk() {
    // The statistics have to be built from the whole batch so that they record exactly the points
    // they describe: the bytes written by Statistics#serialize are read back by
    // Statistics#deserialize, which rejects a record whose size does not match its type.
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(new long[] {1L}, new int[] {1}, 1);
    return new Chunk(
        createChunkHeader("temperature"),
        ByteBuffer.wrap(new byte[] {0, 0, 0, 1}),
        null,
        statistics);
  }

  private static int chunkHeaderSize(final ChunkHeader chunkHeader) throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return chunkHeader.serializeTo(output);
    }
  }

  private static int chunkGroupHeaderSize(final IDeviceID device) throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return new ChunkGroupHeader(device).serializeTo(output);
    }
  }
}
