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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProgressWALIteratorTest {

  @Test
  public void testIteratorGroupsByLocalSeqAndCarriesWriterMetadata() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(
            searchableEntry(5L), singleEntryMeta(19, 5L, 1L, 100L, 5L, 1000L, 7, 3L, 105L));
        writer.write(
            searchableEntry(5L), singleEntryMeta(19, 5L, 1L, 100L, 5L, 1000L, 7, 3L, 105L));
        writer.write(
            searchableEntry(12L), singleEntryMeta(19, 12L, 1L, 101L, 12L, 2000L, 7, 4L, 112L));
      }
      try (WALWriter ignored = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a sealed successor so the first WAL becomes historical and readable.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 6L)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest request = iterator.next();
        assertEquals(12L, request.getSearchIndex());
        assertEquals(112L, request.getProgressLocalSeq());
        assertEquals(2000L, request.getPhysicalTime());
        assertEquals(7, request.getNodeId());
        assertEquals(4L, request.getWriterEpoch());
        assertEquals(1, request.getRequests().size());
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorMergesFragmentsWithSameLocalSeq() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-merge");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 9, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(9L), singleEntryMeta(19, 9L, 1L, 88L, 9L, 900L, 5, 2L, 1009L));
        writer.write(searchableEntry(9L), singleEntryMeta(19, 9L, 1L, 88L, 9L, 900L, 5, 2L, 1009L));
      }
      try (WALWriter ignored = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a sealed successor so the first WAL becomes historical and readable.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest request = iterator.next();
        assertEquals(9L, request.getSearchIndex());
        assertEquals(1009L, request.getProgressLocalSeq());
        assertEquals(900L, request.getPhysicalTime());
        assertEquals(5, request.getNodeId());
        assertEquals(2L, request.getWriterEpoch());
        assertEquals(2, request.getRequests().size());
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorKeepsDifferentWritersWithSameLocalSeqSeparated() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-writers");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 16, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(15L), singleEntryMeta(19, 15L, 1L, 1L, 15L, 1500L, 7, 1L, 1L));
        writer.write(searchableEntry(16L), singleEntryMeta(19, 16L, 1L, 2L, 16L, 1501L, 8, 1L, 1L));
      }
      try (WALWriter ignored = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a sealed successor so the first WAL becomes historical and readable.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest first = iterator.next();
        assertEquals(15L, first.getSearchIndex());
        assertEquals(1L, first.getProgressLocalSeq());
        assertEquals(7, first.getNodeId());

        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest second = iterator.next();
        assertEquals(16L, second.getSearchIndex());
        assertEquals(1L, second.getProgressLocalSeq());
        assertEquals(8, second.getNodeId());

        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorDoesNotSkipNextWalFileAfterExhaustingCurrentOne() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-sequential-files");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File secondWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File thirdWal =
        dir.resolve(WALFileUtils.getLogFileName(2, 2, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 1L, 1L, 100L, 7, 1L, 1L));
      }
      try (WALWriter writer = new WALWriter(secondWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 2L, 2L, 200L, 7, 1L, 2L));
      }
      try (WALWriter writer = new WALWriter(thirdWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(3L), singleEntryMeta(19, 3L, 1L, 3L, 3L, 300L, 7, 1L, 3L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());

        assertTrue(iterator.hasNext());
        assertEquals(2L, iterator.next().getSearchIndex());

        assertTrue(iterator.hasNext());
        assertEquals(3L, iterator.next().getSearchIndex());

        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(secondWal.toPath());
      Files.deleteIfExists(thirdWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testFollowerEntryDoesNotSynthesizeSearchIndexFromProgressLocalSeq() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-follower");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(
            searchableEntry(-1L), singleEntryMeta(19, -1L, 1L, 77L, -1L, 900L, 5, 2L, 1009L));
      }
      try (WALWriter ignored = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a readable successor for the first WAL file.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest request = iterator.next();
        assertEquals(-1L, request.getSearchIndex());
        assertEquals(1009L, request.getProgressLocalSeq());
        assertEquals(900L, request.getPhysicalTime());
        assertEquals(5, request.getNodeId());
        assertEquals(2L, request.getWriterEpoch());
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorMarksIncompleteScanWhenNearLiveWalCannotBeOpened() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-incomplete-scan");
    final File brokenLiveWal =
        dir.resolve(WALFileUtils.getLogFileName(7, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      assertTrue(brokenLiveWal.mkdir());

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(dir.toFile());
      when(walNode.getCurrentWALFileVersion()).thenReturn(7L);
      when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(new WALMetaData());

      try (ProgressWALIterator iterator = new ProgressWALIterator(walNode, Long.MIN_VALUE)) {
        assertFalse(iterator.hasNext());
        assertTrue(iterator.hasIncompleteScan());
        assertTrue(iterator.hasReadError());
        assertTrue(iterator.getIncompleteScanDetail().contains("near-live WAL file"));
      }
    } finally {
      Files.deleteIfExists(brokenLiveWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  private static ByteBuffer searchableEntry(final long bodySearchIndex) {
    final ByteBuffer buffer =
        ByteBuffer.allocate(WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES + Long.BYTES);
    buffer.put(WALEntryType.INSERT_ROW_NODE.getCode());
    buffer.putLong(1L);
    buffer.putShort(PlanNodeType.INSERT_ROW.getNodeType());
    buffer.putLong(bodySearchIndex);
    return buffer;
  }

  private static WALMetaData singleEntryMeta(
      final int size,
      final long searchIndex,
      final long memTableId,
      final long epoch,
      final long syncIndex,
      final long physicalTime,
      final int nodeId,
      final long writerEpoch,
      final long localSeq) {
    final WALMetaData metaData = new WALMetaData();
    metaData.add(size, searchIndex, memTableId, physicalTime, nodeId, writerEpoch, localSeq);
    return metaData;
  }
}
