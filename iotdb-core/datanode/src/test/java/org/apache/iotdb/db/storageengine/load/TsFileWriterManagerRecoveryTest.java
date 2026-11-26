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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TsFileWriterManagerRecoveryTest {

  private static final String DATABASE = "root.load_recovery_test";
  private static final IDeviceID DEVICE =
      new StringArrayDeviceID("root", "load_recovery_test", "d1");

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private DataRegion dataRegion;

  @Before
  public void setUp() {
    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
  }

  @Test
  public void testResumeNegativeTimePartition() throws Exception {
    assertTimePartitionResumes(-604_800_000L);
  }

  @Test
  public void testResumeMinimumTimePartition() throws Exception {
    assertTimePartitionResumes(Long.MIN_VALUE);
  }

  @Test
  public void testCommitPreparedFileAfterRecovery() throws Exception {
    final File taskDir = temporaryFolder.newFolder("prepared-task");
    final NonAlignedChunkData chunk = chunk(0L, 1);
    new ChunkOffsetCalculator().assign(chunk);
    final ProgressIndex progressIndex = new SimpleProgressIndex(1, 10L);
    TsFileWriterManager manager = new TsFileWriterManager(dataRegion, taskDir);
    try {
      manager.writePiece(List.of(chunk));
      manager.prepare(
          true,
          Map.of(chunk.getTimePartitionSlot(), progressIndex),
          Map.of(chunk.getTimePartitionSlot(), chunkEnd(chunk)));
      final File staged = stagedFile(taskDir, 0L);
      final byte[] preparedBytes = Files.readAllBytes(staged.toPath());
      manager.close(true);

      manager = new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();

      assertArrayEquals(
          "Recovery must preserve a prepared TsFile",
          preparedBytes,
          Files.readAllBytes(staged.toPath()));
      // The coordinator already received the PREPARE acknowledgement and only sends COMMIT.
      manager.loadAll(true, false);
      final ArgumentCaptor<TsFileResource> imported = ArgumentCaptor.forClass(TsFileResource.class);
      Mockito.verify(dataRegion)
          .loadNewTsFile(
              imported.capture(),
              Mockito.eq(false),
              Mockito.eq(true),
              Mockito.eq(false),
              Mockito.any());
      assertTrue(imported.getValue().isGeneratedByPipe());
      assertEquals(progressIndex, imported.getValue().getMaxProgressIndexAfterClose());
      assertPoints(staged, 0L, 1);
    } finally {
      manager.close();
    }
  }

  private void assertTimePartitionResumes(final long partitionStart) throws Exception {
    final File taskDir = temporaryFolder.newFolder("task");
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final NonAlignedChunkData first = chunk(partitionStart, 1);
    final NonAlignedChunkData second = chunk(partitionStart, 2);
    calculator.assign(first);
    calculator.assign(second);

    TsFileWriterManager manager = new TsFileWriterManager(dataRegion, taskDir);
    try {
      manager.writePiece(List.of(first));
      manager.close(true);
      manager = new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();
      manager.writePiece(List.of(second));
      manager.prepare(
          false, Collections.emptyMap(), Map.of(second.getTimePartitionSlot(), chunkEnd(second)));

      // Both the chunk staged before recovery and the one appended afterwards must survive.
      assertPoints(stagedFile(taskDir, partitionStart), partitionStart, 1, 2);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testPrepareRejectsMissingPartition() throws Exception {
    final File taskDir = temporaryFolder.newFolder("task");
    final TsFileWriterManager manager = new TsFileWriterManager(dataRegion, taskDir);
    final NonAlignedChunkData first = chunk(0L, 1);
    final NonAlignedChunkData missing = chunk(604_800_000L, 1);
    new ChunkOffsetCalculator().assign(first);
    new ChunkOffsetCalculator().assign(missing);
    final Map<TTimePartitionSlot, Long> expectedDataEnd =
        Map.of(
            first.getTimePartitionSlot(), chunkEnd(first),
            missing.getTimePartitionSlot(), chunkEnd(missing));
    try {
      manager.writePiece(List.of(first));
      final long unsealedLength = stagedFile(taskDir, 0L).length();
      assertPrepareFails(manager, expectedDataEnd, stagedFile(taskDir, 604_800_000L));
      assertEquals(unsealedLength, stagedFile(taskDir, 0L).length());

      manager.writePiece(List.of(missing));
      manager.prepare(false, Collections.emptyMap(), expectedDataEnd);
      assertPoints(stagedFile(taskDir, 0L), 0L, 1);
      assertPoints(stagedFile(taskDir, 604_800_000L), 604_800_000L, 1);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testPrepareRejectsPartitionWithoutRecordedChunks() throws Exception {
    final File taskDir = temporaryFolder.newFolder("task");
    final TsFileWriterManager manager = new TsFileWriterManager(dataRegion, taskDir);
    final NonAlignedChunkData complete = chunk(0L, 1);
    new ChunkOffsetCalculator().assign(complete);
    final Map<TTimePartitionSlot, Long> expectedDataEnd =
        Map.of(complete.getTimePartitionSlot(), chunkEnd(complete));
    try {
      // A failed piece may have opened the file without recording any durable chunk.
      try {
        manager.writePiece(List.of(chunk(0L, 1)));
        fail("A chunk without a layout must fail after opening its staged file");
      } catch (final IOException expected) {
        assertTrue(stagedFile(taskDir, 0L).isFile());
      }
      assertPrepareFails(manager, expectedDataEnd, stagedFile(taskDir, 0L));

      manager.writePiece(List.of(complete));
      manager.prepare(false, Collections.emptyMap(), expectedDataEnd);
      assertPoints(stagedFile(taskDir, 0L), 0L, 1);
    } finally {
      manager.close();
    }
  }

  private static void assertPrepareFails(
      final TsFileWriterManager manager,
      final Map<TTimePartitionSlot, Long> expectedDataEnd,
      final File missingDataFile)
      throws Exception {
    try {
      manager.prepare(false, Collections.emptyMap(), expectedDataEnd);
      fail("PREPARE must reject a partition whose announced data has not been staged");
    } catch (final LoadFileException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(missingDataFile.getAbsolutePath()));
    }
  }

  private static NonAlignedChunkData chunk(final long partitionStart, final int value) {
    final NonAlignedChunkData chunk =
        new NonAlignedChunkData(
            DEVICE,
            new ChunkHeader(
                "s1", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0),
            new TTimePartitionSlot(partitionStart));
    chunk.writeDecodePage(new long[] {partitionStart + value}, new Object[] {value}, 1);
    chunk.endChunk();
    return chunk;
  }

  private static long chunkEnd(final NonAlignedChunkData chunk) {
    return chunk.getChunkLayout().offset() + chunk.getChunkLayout().length();
  }

  private static File stagedFile(final File taskDir, final long partitionStart) {
    return new File(taskDir, DATABASE + "-0-" + partitionStart + ".tsfile");
  }

  private static void assertPoints(final File file, final long partitionStart, final int... values)
      throws Exception {
    try (final TsFileReader reader = new TsFileReader(file)) {
      final QueryDataSet result =
          reader.query(QueryExpression.create(List.of(new Path(DEVICE, "s1", false)), null));
      for (final int value : values) {
        assertTrue(result.hasNext());
        final RowRecord row = result.next();
        assertEquals(partitionStart + value, row.getTimestamp());
        assertEquals(value, row.getField(0).getIntV());
      }
      assertFalse(result.hasNext());
    }
  }
}
