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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

public class LoadTsFileProgressTest {

  @Test
  public void testTotalLengthKeepsMaximumWhenFillingEarlierHole() throws Exception {
    final File tsFile = Files.createTempFile("load-progress", ".tsfile").toFile();
    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
    final Chunk chunk = createChunk();

    progress.recordChunk(
        new StringArrayDeviceID("root", "sg", "d1"), false, 7L, 30L, true, chunk, 100L, 1L);
    progress.recordChunk(
        new StringArrayDeviceID("root", "sg", "d2"), false, 7L, 30L, true, chunk, 50L, 1L);

    Assert.assertEquals(100L, progress.getTotalLength());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
  }

  @Test
  public void testRecordAndReadChunkRange() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-record", ".tsfile").toFile();
    final Chunk chunk = createChunk();

    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
    progress.recordChunk(
        new StringArrayDeviceID("root", "sg", "d1"), false, 7L, 30L, true, chunk, 40L, 1L);

    Assert.assertEquals(40L, progress.getTotalLength());
    Assert.assertTrue(progress.isReady(40L));

    final LoadTsFileProgress recovered = new LoadTsFileProgress(tsFile);
    final LoadTsFileProgress.ChunkRangeRecord record = recovered.readAllRecords().get(0);
    Assert.assertEquals("root.sg.d1", record.device());
    Assert.assertEquals(7L, record.physicalStart());
    Assert.assertEquals(40L, record.physicalEnd());
    Assert.assertEquals(30L, record.chunkOffset());
    Assert.assertTrue(record.firstChunkOfGroup());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
  }

  @Test
  public void testAnEntryOfAnUnknownVersionIsSteppedOver() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-unknown", ".tsfile").toFile();
    try {
      final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
      progress.recordChunk(
          new StringArrayDeviceID("root", "sg", "d1"),
          false,
          7L,
          30L,
          true,
          createChunk(),
          40L,
          1L);

      // An entry a later version may introduce: this build has to step over it and keep reading.
      final byte[] body = {1, 2, 3, 4};
      try (final DataOutputStream out =
          new DataOutputStream(new FileOutputStream(progress.getProgressFile(), true))) {
        ReadWriteIOUtils.write(body.length + 1, out);
        out.writeByte(99);
        out.write(body);
      }
      new LoadTsFileProgress(tsFile).recordTerminal(LoadTsFileConsensusOp.COMMIT, 5L);

      Assert.assertEquals(1, new LoadTsFileProgress(tsFile).readAllRecords().size());
      final LoadTsFileProgress.TerminalRecord terminal =
          LoadTsFileProgress.readTerminal(progress.getProgressFile());
      Assert.assertNotNull("the scan must reach the terminal after the unknown entry", terminal);
      Assert.assertEquals(5L, terminal.searchIndex());
    } finally {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
    }
  }

  @Test
  public void testATrailingFieldOfAChunkEntryIsSteppedOver() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-field", ".tsfile").toFile();
    try {
      final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
      progress.recordChunk(
          new StringArrayDeviceID("root", "sg", "d1"),
          false,
          7L,
          30L,
          true,
          createChunk(),
          40L,
          1L);

      // Grow the entry the way a later version would when it gains a field: same fields, a longer
      // declared length, and the extra bytes inside the entry.
      final byte[] extra = {7, 7, 7};
      final int entryStart =
          4
              + "LTPROG1".length()
              + Integer.BYTES
              + tsFile.getParentFile().getName().getBytes(StandardCharsets.UTF_8).length;
      final File progressFile = progress.getProgressFile();
      try (final RandomAccessFile file = new RandomAccessFile(progressFile, "rw")) {
        file.seek(entryStart);
        final int declaredLength = file.readInt();
        file.seek(entryStart);
        file.writeInt(declaredLength + extra.length);
        file.seek(file.length());
        file.write(extra);
      }
      new LoadTsFileProgress(tsFile).recordTerminal(LoadTsFileConsensusOp.COMMIT, 6L);

      Assert.assertEquals(1, new LoadTsFileProgress(tsFile).readAllRecords().size());
      final LoadTsFileProgress.TerminalRecord terminal =
          LoadTsFileProgress.readTerminal(progressFile);
      Assert.assertNotNull("the reader must step over the extra field", terminal);
      Assert.assertEquals(6L, terminal.searchIndex());
    } finally {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
    }
  }

  @Test
  public void testChunkBodiesAreSizedBeforeTheyAreSerialized() throws Exception {
    final Chunk chunk = createChunk();

    final ByteBuffer headerBody = LoadTsFileProgress.serializeChunkHeader(chunk);
    final ByteBuffer statisticsBody = LoadTsFileProgress.serializeStatistics(chunk);

    // The announced size is the capacity the buffer was created with, so serializing never had to
    // grow it, and the record keeps a view over that buffer instead of a copy of it.
    Assert.assertEquals(chunk.getHeader().getSerializedSize(), headerBody.array().length);
    Assert.assertEquals(
        chunk.getChunkStatistic().getSerializedSize(), statisticsBody.array().length);
    Assert.assertEquals(chunk.getHeader().getSerializedSize() - 1, headerBody.remaining());
    Assert.assertEquals(chunk.getChunkStatistic().getSerializedSize(), statisticsBody.remaining());
  }

  @Test
  public void testEveryIncompleteHeaderIsRepairedBeforeAppending() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-header", ".tsfile").toFile();
    final File progressFile = LoadTsFileProgress.progressFileFor(tsFile);
    try {
      recordChunk(tsFile, 1L);
      final byte[] complete = Files.readAllBytes(progressFile.toPath());
      final int headerLength =
          Integer.BYTES
              + "LTPROG1".length()
              + Integer.BYTES
              + tsFile.getParentFile().getName().getBytes(StandardCharsets.UTF_8).length;
      for (int length = 0; length < headerLength; length++) {
        Files.write(progressFile.toPath(), Arrays.copyOf(complete, length));

        Assert.assertTrue(
            "incomplete header at byte " + length,
            new LoadTsFileProgress(tsFile).readAllRecordsRepairingTornTail().isEmpty());
        Assert.assertEquals(0L, progressFile.length());
        Assert.assertTrue(new LoadTsFileProgress(tsFile).readAllRecords().isEmpty());
        Assert.assertNull(LoadTsFileProgress.readTerminal(progressFile));

        // A restored task must be able to append again after its empty header was repaired.
        recordChunk(tsFile, 2L);
        Assert.assertEquals(1, new LoadTsFileProgress(tsFile).readAllRecords().size());
      }
    } finally {
      Files.deleteIfExists(progressFile.toPath());
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void testEveryTornEntryKeepsTheCompletePrefix() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-torn-entry", ".tsfile").toFile();
    final File progressFile = LoadTsFileProgress.progressFileFor(tsFile);
    try {
      recordChunk(tsFile, 1L);
      final long firstEnd = progressFile.length();
      recordChunk(tsFile, 2L);
      final long secondEnd = progressFile.length();
      new LoadTsFileProgress(tsFile).recordTerminal(LoadTsFileConsensusOp.COMMIT, 3L);
      final byte[] complete = Files.readAllBytes(progressFile.toPath());

      final int headerLength =
          Integer.BYTES
              + "LTPROG1".length()
              + Integer.BYTES
              + tsFile.getParentFile().getName().getBytes(StandardCharsets.UTF_8).length;
      for (int length = headerLength; length < complete.length; length++) {
        Files.write(progressFile.toPath(), Arrays.copyOf(complete, length));
        final int expectedRecords = length < firstEnd ? 0 : length < secondEnd ? 1 : 2;
        final long expectedLength =
            length < firstEnd ? headerLength : length < secondEnd ? firstEnd : secondEnd;
        Assert.assertEquals(
            "torn entry at byte " + length,
            expectedRecords,
            new LoadTsFileProgress(tsFile).readAllRecordsRepairingTornTail().size());
        Assert.assertEquals(expectedLength, progressFile.length());
        Assert.assertNull(LoadTsFileProgress.readTerminal(progressFile));
      }
    } finally {
      Files.deleteIfExists(progressFile.toPath());
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void testTornEntryLargerThanTheReadBufferIsRepaired() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-large-torn", ".tsfile").toFile();
    final File progressFile = LoadTsFileProgress.progressFileFor(tsFile);
    try {
      recordChunk(tsFile, 1L);
      final long completeLength = progressFile.length();
      try (final DataOutputStream out =
          new DataOutputStream(new FileOutputStream(progressFile, true))) {
        out.writeInt(32769);
        out.writeByte(99);
        out.write(new byte[24576]);
      }

      Assert.assertEquals(
          1, new LoadTsFileProgress(tsFile).readAllRecordsRepairingTornTail().size());
      Assert.assertEquals(completeLength, progressFile.length());
    } finally {
      Files.deleteIfExists(progressFile.toPath());
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void testCompleteEntryLargerThanTheReadBufferIsPreserved() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-large-complete", ".tsfile").toFile();
    final File progressFile = LoadTsFileProgress.progressFileFor(tsFile);
    try {
      recordChunk(tsFile, 1L);
      try (final DataOutputStream out =
          new DataOutputStream(new FileOutputStream(progressFile, true))) {
        out.writeInt(32769);
        out.writeByte(99);
        out.write(new byte[32768]);
      }
      new LoadTsFileProgress(tsFile).recordTerminal(LoadTsFileConsensusOp.COMMIT, 3L);
      final long completeLength = progressFile.length();

      Assert.assertEquals(
          1, new LoadTsFileProgress(tsFile).readAllRecordsRepairingTornTail().size());
      Assert.assertEquals(completeLength, progressFile.length());
      Assert.assertEquals(3L, LoadTsFileProgress.readTerminal(progressFile).searchIndex());
    } finally {
      Files.deleteIfExists(progressFile.toPath());
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void testInvalidMagicIsNotRepairedAsATornHeader() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-invalid-magic", ".tsfile").toFile();
    final File progressFile = LoadTsFileProgress.progressFileFor(tsFile);
    try {
      recordChunk(tsFile, 1L);
      final byte[] corrupt = Files.readAllBytes(progressFile.toPath());
      corrupt[Integer.BYTES] = 'X';
      Files.write(progressFile.toPath(), corrupt);

      Assert.assertThrows(
          java.io.IOException.class,
          () -> new LoadTsFileProgress(tsFile).readAllRecordsRepairingTornTail());
      Assert.assertArrayEquals(corrupt, Files.readAllBytes(progressFile.toPath()));
    } finally {
      Files.deleteIfExists(progressFile.toPath());
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  private static void recordChunk(final File tsFile, final long searchIndex) throws Exception {
    new LoadTsFileProgress(tsFile)
        .recordChunk(
            new StringArrayDeviceID("root", "sg", "d1"),
            false,
            7L,
            30L,
            true,
            createChunk(),
            40L,
            searchIndex);
  }

  private static Chunk createChunk() {
    final ChunkHeader chunkHeader =
        new ChunkHeader(
            "s1", 4, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 1);
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1L, 1);
    statistics.update(2L, 2);
    return new Chunk(chunkHeader, ByteBuffer.wrap(new byte[] {0, 0, 0, 1}), null, statistics);
  }
}
