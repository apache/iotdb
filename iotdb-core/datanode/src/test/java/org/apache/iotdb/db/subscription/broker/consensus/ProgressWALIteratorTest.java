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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        writer.write(searchableEntry(5L), singleEntryMeta(19, 5L, 1L, 1000L, 7, 105L));
        writer.write(searchableEntry(5L), singleEntryMeta(19, 5L, 1L, 1000L, 7, 105L));
        writer.write(searchableEntry(12L), singleEntryMeta(19, 12L, 1L, 2000L, 7, 112L));
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
  public void testIteratorUsesMetadataSearchIndexForStartFiltering() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-metadata-search-index");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 6, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(-1L), singleEntryMeta(19, 5L, 1L, 1000L, 7, 105L));
        writer.write(searchableEntry(-1L), singleEntryMeta(19, 6L, 1L, 2000L, 7, 106L));
      }
      try (WALWriter ignored = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a sealed successor so the first WAL becomes historical and readable.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 6L)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest request = iterator.next();
        assertEquals(6L, request.getSearchIndex());
        assertEquals(106L, request.getProgressLocalSeq());
        assertEquals(2000L, request.getPhysicalTime());
        assertEquals(7, request.getNodeId());
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
        writer.write(searchableEntry(9L), singleEntryMeta(19, 9L, 1L, 900L, 5, 1009L));
        writer.write(searchableEntry(9L), singleEntryMeta(19, 9L, 1L, 900L, 5, 1009L));
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
        writer.write(searchableEntry(15L), singleEntryMeta(19, 15L, 1L, 1500L, 7, 1L));
        writer.write(searchableEntry(16L), singleEntryMeta(19, 16L, 1L, 1501L, 8, 1L));
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
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
      }
      try (WALWriter writer = new WALWriter(secondWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 200L, 7, 2L));
      }
      try (WALWriter writer = new WALWriter(thirdWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(3L), singleEntryMeta(19, 3L, 1L, 300L, 7, 3L));
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
        writer.write(searchableEntry(-1L), singleEntryMeta(19, -1L, 1L, 900L, 5, 1009L));
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
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testLiveWalReopenReusesMetadataSnapshot() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-live-snapshot");
    final File liveWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(liveWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 1000L, 7, 1L));
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 1001L, 7, 2L));
      }

      final WALMetaData firstSnapshot = singleEntryMeta(19, 1L, 1L, 1000L, 7, 1L);
      final WALMetaData secondSnapshot = firstSnapshot.copy();
      secondSnapshot.add(19, 2L, 1L, 1001L, 7, 2L);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(dir.toFile());
      when(walNode.getCurrentWALFileVersion()).thenReturn(0L);
      when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(firstSnapshot, secondSnapshot);

      try (ProgressWALIterator iterator = new ProgressWALIterator(walNode, 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());
        assertTrue(iterator.hasNext());
        assertEquals(2L, iterator.next().getSearchIndex());
        verify(walNode, times(2)).getCurrentWALMetaDataSnapshot();
      }
    } finally {
      Files.deleteIfExists(liveWal.toPath());
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

  @Test
  public void testIteratorReusePerformance() throws Exception {
    Assume.assumeTrue(
        "Enable with -Diotdb.test.subscription.performance=true",
        Boolean.getBoolean("iotdb.test.subscription.performance"));

    final int entryCount = Integer.getInteger("iotdb.test.subscription.performance.entries", 4096);
    final int batchSize = Integer.getInteger("iotdb.test.subscription.performance.batch-size", 64);
    assertTrue("entry count must be positive", entryCount > 0);
    assertTrue("batch size must be positive", batchSize > 0);
    final Path dir = Files.createTempDirectory("progress-wal-iterator-performance");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(
                WALFileUtils.getLogFileName(
                    1, entryCount + 1L, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        for (long index = 1; index <= entryCount; index++) {
          writer.write(searchableEntry(index), singleEntryMeta(19, index, 1L, index, 7, index));
        }
      }
      try (WALWriter ignored = new WALWriter(successorWal, WALFileVersion.V3)) {
        // Seal the data WAL so both benchmark variants read the same historical file.
      }

      assertEquals(entryCount, consumeWithReusedIterator(dir.toFile(), entryCount));

      final long reopenStartNanos = System.nanoTime();
      assertEquals(entryCount, consumeWithReopenedIterator(dir.toFile(), entryCount, batchSize));
      final long reopenNanos = System.nanoTime() - reopenStartNanos;

      final long refreshStartNanos = System.nanoTime();
      assertEquals(entryCount, consumeWithRefreshedIterator(dir.toFile(), entryCount, batchSize));
      final long refreshNanos = System.nanoTime() - refreshStartNanos;

      final long reuseStartNanos = System.nanoTime();
      assertEquals(entryCount, consumeWithReusedIterator(dir.toFile(), entryCount));
      final long reuseNanos = System.nanoTime() - reuseStartNanos;

      final double reopenSpeedup = (double) reopenNanos / Math.max(1L, reuseNanos);
      final double refreshSpeedup = (double) refreshNanos / Math.max(1L, reuseNanos);
      System.out.printf(
          "Subscription WAL iterator benchmark: entries=%d, batchSize=%d, "
              + "reopen=%.3f ms, refreshEachBatch=%.3f ms, reuse=%.3f ms, "
              + "reopenSpeedup=%.2fx, refreshSpeedup=%.2fx%n",
          entryCount,
          batchSize,
          reopenNanos / 1_000_000.0,
          refreshNanos / 1_000_000.0,
          reuseNanos / 1_000_000.0,
          reopenSpeedup,
          refreshSpeedup);
      assertTrue(
          "Reusing the iterator should be faster than reopening each batch", reopenSpeedup > 1.0);
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  private static int consumeWithReopenedIterator(
      final File walDirectory, final int entryCount, final int batchSize) throws Exception {
    int consumed = 0;
    long nextSearchIndex = 1L;
    while (consumed < entryCount) {
      int batchCount = 0;
      try (ProgressWALIterator iterator = new ProgressWALIterator(walDirectory, nextSearchIndex)) {
        while (batchCount < batchSize && iterator.hasNext()) {
          nextSearchIndex = iterator.next().getSearchIndex() + 1L;
          batchCount++;
          consumed++;
        }
      }
      if (batchCount == 0) {
        break;
      }
    }
    return consumed;
  }

  private static int consumeWithReusedIterator(final File walDirectory, final int entryCount)
      throws Exception {
    int consumed = 0;
    try (ProgressWALIterator iterator = new ProgressWALIterator(walDirectory, 1L)) {
      while (consumed < entryCount && iterator.hasNext()) {
        iterator.next();
        consumed++;
      }
    }
    return consumed;
  }

  private static int consumeWithRefreshedIterator(
      final File walDirectory, final int entryCount, final int batchSize) throws Exception {
    int consumed = 0;
    try (ProgressWALIterator iterator = new ProgressWALIterator(walDirectory, 1L)) {
      while (consumed < entryCount) {
        iterator.refresh();
        int batchCount = 0;
        while (batchCount < batchSize && iterator.hasNext()) {
          iterator.next();
          batchCount++;
          consumed++;
        }
        if (batchCount == 0) {
          break;
        }
      }
    }
    return consumed;
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
      final long physicalTime,
      final int nodeId,
      final long localSeq) {
    final WALMetaData metaData = new WALMetaData();
    metaData.add(size, searchIndex, memTableId, physicalTime, nodeId, localSeq);
    return metaData;
  }
}
