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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
  public void testIteratorUsesBodySearchIndexForStartFiltering() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-metadata-search-index");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 6, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(5L), singleEntryMeta(19, 5L, 1L, 1000L, 7, 105L));
        writer.write(searchableEntry(6L), singleEntryMeta(19, 6L, 1L, 2000L, 7, 106L));
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
  public void testIteratorPreservesAbsentLocalIndexAfterManyFragments() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-mixed-fragments");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1878, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        // Physical WAL entries count fragments, not local consensus requests. The old fallback
        // assigns the replicated entry 1877 + 550 = 2427, creating a false [1878, 2427) gap.
        for (int fragment = 0; fragment < 550; fragment++) {
          writer.write(searchableEntry(1877L), singleEntryMeta(19, 1877L, 1L, 1000L, 7, 1877L));
        }
        writer.write(
            searchableEntry(SearchNode.NO_CONSENSUS_INDEX),
            singleEntryMeta(19, SearchNode.NO_CONSENSUS_INDEX, 1L, 2000L, 8, 10000L));
        writer.write(searchableEntry(1878L), singleEntryMeta(19, 1878L, 1L, 3000L, 7, 1878L));
      }
      try (WALWriter ignored = new WALWriter(successorWal, WALFileVersion.V3)) {
        // Keep both files retained and sealed; there is no deletion or concurrent write.
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest fragmented = iterator.next();
        assertEquals(1877L, fragmented.getSearchIndex());
        assertEquals(1877L, fragmented.getProgressLocalSeq());
        assertEquals(7, fragmented.getNodeId());
        assertEquals(550, fragmented.getRequests().size());

        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest replicated = iterator.next();
        assertEquals(SearchNode.NO_CONSENSUS_INDEX, replicated.getSearchIndex());
        assertEquals(10000L, replicated.getProgressLocalSeq());
        assertEquals(2000L, replicated.getPhysicalTime());
        assertEquals(8, replicated.getNodeId());
        assertEquals(1, replicated.getRequests().size());

        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest nextLocal = iterator.next();
        assertEquals(1878L, nextLocal.getSearchIndex());
        assertEquals(1878L, nextLocal.getProgressLocalSeq());
        assertEquals(7, nextLocal.getNodeId());
        assertEquals(1, nextLocal.getRequests().size());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasIncompleteScan());
        assertTrue(dataWal.isFile());
      }

      // A local-index seek must also retain replicated requests with their own writer progress.
      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1878L)) {
        assertTrue(iterator.hasNext());
        assertEquals(SearchNode.NO_CONSENSUS_INDEX, iterator.next().getSearchIndex());
        assertTrue(iterator.hasNext());
        assertEquals(1878L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasIncompleteScan());
      }
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
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
  public void testLocalLowerBoundKeepsFollowerEntryWithoutSynthesizingSearchIndex()
      throws Exception {
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
      try (WALWriter writer = new WALWriter(lastWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 1000L, 6, 1L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest request = iterator.next();
        assertEquals(-1L, request.getSearchIndex());
        assertEquals(1009L, request.getProgressLocalSeq());
        assertEquals(900L, request.getPhysicalTime());
        assertEquals(5, request.getNodeId());
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorAggregatesUnreadableRetainedWalFiles() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-unreadable-files");
    final File firstBrokenWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File secondBrokenWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File lastWal =
        dir.resolve(WALFileUtils.getLogFileName(2, 2, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final Logger logger = (Logger) LoggerFactory.getLogger(ProgressWALIterator.class);
    final Level originalLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      Files.write(firstBrokenWal.toPath(), new byte[128]);
      Files.write(secondBrokenWal.toPath(), new byte[128]);
      try (WALWriter writer = new WALWriter(lastWal, WALFileVersion.V3)) {
        // Create a readable successor so both malformed WAL files are treated as retained history.
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 200L, 7, 2L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        // A continuously readable iterator must log every skip before reaching exhaustion.
        assertEquals(2, appender.list.size());
        assertTrue(appender.list.get(0).getFormattedMessage().contains(firstBrokenWal.getName()));
        assertTrue(appender.list.get(1).getFormattedMessage().contains(secondBrokenWal.getName()));
        assertTrue(appender.list.get(0).getFormattedMessage().contains("versionId=0"));
        assertTrue(
            appender.list.get(0).getFormattedMessage().contains("error=BrokenWALFileException"));
        assertEquals(0L, iterator.getSkippedBrokenWalEntryCount());
        final String detail = iterator.getSkippedBrokenWalFileDetails(0);
        assertTrue(detail.contains(firstBrokenWal.getName()));
        assertTrue(detail.contains(secondBrokenWal.getName()));
        assertTrue(detail.contains("skippedEntries=unknown"));
        assertEquals(2L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
        assertTrue(iterator.hasSkippedBrokenWalFiles());
        assertEquals(2, iterator.getSkippedBrokenWalFileCount());
        assertTrue(iterator.hasIncompleteScan());
        assertFalse(iterator.hasReadError());
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(originalLevel);
      appender.stop();
      Files.deleteIfExists(firstBrokenWal.toPath());
      Files.deleteIfExists(secondBrokenWal.toPath());
      Files.deleteIfExists(
          firstBrokenWal.toPath().resolveSibling(firstBrokenWal.getName() + ".broken"));
      Files.deleteIfExists(
          secondBrokenWal.toPath().resolveSibling(secondBrokenWal.getName() + ".broken"));
      Files.deleteIfExists(lastWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorSkipsUnreadableEntryAndPreservesCompletedPendingRequest()
      throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-unreadable-entry");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 3, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final int originalEntrySizeLimit =
        IoTDBDescriptor.getInstance().getConfig().getWalEntrySizeLimitInByte();

    try {
      IoTDBDescriptor.getInstance().getConfig().setWalEntrySizeLimitInByte(64);
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
        writer.write(oversizedSearchableEntry(65, 2L), singleEntryMeta(65, 2L, 1L, 200L, 7, 2L));
      }
      try (WALWriter writer = new WALWriter(successorWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(3L), singleEntryMeta(19, 3L, 1L, 300L, 7, 3L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());
        assertTrue(iterator.hasNext());
        assertEquals(3L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
        assertEquals(1, iterator.getSkippedBrokenWalFileCount());
        assertEquals(1L, iterator.getSkippedBrokenWalEntryCount());
        assertTrue(iterator.getSkippedBrokenWalFileDetails(0).contains("entryRange=[1, 2)"));
        assertTrue(iterator.hasIncompleteScan());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setWalEntrySizeLimitInByte(originalEntrySizeLimit);
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testIteratorDropsPendingRequestWhenUnreadableEntryContinuesSameWriterProgress()
      throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-incomplete-request");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 3, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final int originalEntrySizeLimit =
        IoTDBDescriptor.getInstance().getConfig().getWalEntrySizeLimitInByte();

    try {
      IoTDBDescriptor.getInstance().getConfig().setWalEntrySizeLimitInByte(64);
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
        writer.write(oversizedSearchableEntry(65, 2L), singleEntryMeta(65, 2L, 1L, 100L, 7, 1L));
      }
      try (WALWriter writer = new WALWriter(successorWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(3L), singleEntryMeta(19, 3L, 1L, 300L, 7, 3L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), Long.MIN_VALUE)) {
        assertTrue(iterator.hasNext());
        assertEquals(3L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
        assertEquals(1, iterator.getSkippedBrokenWalFileCount());
        assertEquals(2L, iterator.getSkippedBrokenWalEntryCount());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setWalEntrySizeLimitInByte(originalEntrySizeLimit);
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testRetainedEofRetriesAndPreservesPendingFragments() throws Exception {
    verifyEofRetryPreservesPendingFragments(false);
  }

  @Test
  public void testNearLiveEofDoesNotBlacklistFileOnRepeatedReads() throws Exception {
    verifyEofRetryPreservesPendingFragments(true);
  }

  private void verifyEofRetryPreservesPendingFragments(final boolean nearLive) throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-eof-retry");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    try {
      writeFragmentedWal(dataWal, 38);
      final WALMetaData successorMetadata = singleEntryMeta(19, 2L, 1L, 200L, 7, 2L);
      try (WALWriter writer = new WALWriter(successorWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(2L), successorMetadata);
      }
      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(dir.toFile());
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(successorMetadata);
      try (ProgressWALIterator iterator =
          nearLive
              ? new ProgressWALIterator(walNode, 1L)
              : new ProgressWALIterator(dir.toFile(), 1L)) {
        assertFalse(iterator.hasNext());
        assertTrue(iterator.isWaitingForReadableWal());
        assertEquals(0, iterator.getSkippedBrokenWalFileCount());
        if (nearLive) {
          assertFalse(iterator.hasNext());
          assertEquals(0, iterator.getSkippedBrokenWalFileCount());
        }
        // Simulate bytes/footer becoming readable after the first EOF. The retry must resume at
        // entry offset one, retaining the first fragment exactly once.
        Files.delete(dataWal.toPath());
        writeFragmentedWal(dataWal, 19);
        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest recovered = iterator.next();
        assertEquals(1L, recovered.getSearchIndex());
        assertEquals(2, recovered.getRequests().size());
        assertTrue(iterator.hasNext());
        assertEquals(2L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());
        assertEquals(0, iterator.getSkippedBrokenWalFileCount());
      }
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testRepeatedEofInStableRetainedFileReportsTruncatedRequest() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-confirmed-truncation");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    try {
      writeFragmentedWal(dataWal, 38);
      try (WALWriter writer = new WALWriter(successorWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 200L, 7, 2L));
      }
      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertFalse(iterator.hasNext());
        assertEquals(0, iterator.getSkippedBrokenWalFileCount());
        assertTrue(iterator.hasNext());
        assertEquals(2L, iterator.next().getSearchIndex());
        assertEquals(1, iterator.getSkippedBrokenWalFileCount());
        assertEquals(2L, iterator.getSkippedBrokenWalEntryCount());
        final String detail = iterator.getSkippedBrokenWalFileDetails(0);
        assertTrue(detail.contains(dataWal.getName()));
        assertTrue(detail.contains("fileSearchIndexRange=(0, 1]"));
        assertTrue(detail.contains("entryRange=[0, 2)"));
        assertTrue(detail.contains("error=EOFException"));
      }
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testLiveSnapshotEofPreservesIncompleteRequestUntilBytesAreVisible() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-live-eof-retry");
    final File liveWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    try {
      final WALMetaData metadata = singleEntryMeta(19, 1L, 1L, 100L, 7, 1L);
      metadata.add(19, 1L, 1L, 100L, 7, 1L);
      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(dir.toFile());
      when(walNode.getCurrentWALFileVersion()).thenReturn(0L);
      when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(metadata);
      try (WALWriter writer = new WALWriter(liveWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
        try (ProgressWALIterator iterator = new ProgressWALIterator(walNode, 1L)) {
          assertFalse(iterator.hasNext());
          assertEquals(0, iterator.getSkippedBrokenWalFileCount());
          writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
          assertTrue(iterator.hasNext());
          final IndexedConsensusRequest recovered = iterator.next();
          assertEquals(1L, recovered.getSearchIndex());
          assertEquals(2, recovered.getRequests().size());
          assertEquals(0, iterator.getSkippedBrokenWalFileCount());
        }
      }
    } finally {
      Files.deleteIfExists(liveWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  private static void writeFragmentedWal(final File walFile, final int secondEntrySize)
      throws Exception {
    try (WALWriter writer = new WALWriter(walFile, WALFileVersion.V3)) {
      writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
      writer.write(searchableEntry(1L), singleEntryMeta(secondEntrySize, 1L, 1L, 100L, 7, 1L));
    }
  }

  @Test
  public void testSkippedFileDetailsRemainBoundedDuringContinuousReplay() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-bounded-errors");
    final List<File> files = new ArrayList<>();
    try {
      for (int version = 0; version < 10; version++) {
        final File file =
            dir.resolve(
                    WALFileUtils.getLogFileName(
                        version, version, WALFileStatus.CONTAINS_SEARCH_INDEX))
                .toFile();
        files.add(file);
        Files.write(file.toPath(), new byte[128]);
      }
      final File successor =
          dir.resolve(WALFileUtils.getLogFileName(10, 10, WALFileStatus.CONTAINS_SEARCH_INDEX))
              .toFile();
      files.add(successor);
      try (WALWriter writer = new WALWriter(successor, WALFileVersion.V3)) {
        writer.write(searchableEntry(11L), singleEntryMeta(19, 11L, 1L, 200L, 7, 11L));
      }
      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(10, iterator.getSkippedBrokenWalFileCount());
        final String detail = iterator.getSkippedBrokenWalFileDetails(0);
        assertEquals(8, detail.split("versionId=").length - 1);
        assertFalse(detail.contains(files.get(0).getAbsolutePath()));
        assertTrue(detail.contains(files.get(9).getAbsolutePath()));
        assertEquals(2, iterator.getSkippedBrokenWalFileDetails(8).split("versionId=").length - 1);
      }
    } finally {
      for (final File file : files) {
        Files.deleteIfExists(file.toPath());
        Files.deleteIfExists(file.toPath().resolveSibling(file.getName() + ".broken"));
      }
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
        assertTrue(iterator.getIncompleteScanDetail().contains(brokenLiveWal.getName()));
        assertEquals(0, iterator.getSkippedBrokenWalFileCount());
      }
    } finally {
      Files.deleteIfExists(brokenLiveWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testBufferedNextDoesNotScanPastStaleLocalRequest() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-buffered-next");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 2, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    try {
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
        writer.write(searchableEntry(-1L), singleEntryMeta(19, -1L, 1L, 200L, 8, 20L));
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 300L, 7, 2L));
      }
      try (WALWriter ignored = new WALWriter(successorWal, WALFileVersion.V3)) {
        // Seal the data file so all requests can be read from retained WAL.
      }
      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertFalse(iterator.hasBufferedNext());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasBufferedNext());

        iterator.advanceTo(2L, null);
        assertFalse(iterator.hasBufferedNext());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasBufferedNext());

        iterator.advanceTo(3L, null);
        assertTrue(iterator.hasBufferedNext());
        assertEquals(-1L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasBufferedNext());
        assertFalse(iterator.hasNext());
      }
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testAdvanceToKeepsUncoveredFollowerRequestInCurrentFile() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-current-file-advance");
    final File dataWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File successorWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 3, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
      }
      try (WALWriter writer = new WALWriter(successorWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(-1L), singleEntryMeta(19, -1L, 1L, 200L, 8, 20L));
        writer.write(searchableEntry(3L), singleEntryMeta(19, 3L, 1L, 300L, 7, 3L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());

        iterator.advanceTo(3L, (physicalTime, nodeId, localSeq) -> false);

        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest followerRequest = iterator.next();
        assertEquals(-1L, followerRequest.getSearchIndex());
        assertEquals(8, followerRequest.getNodeId());
        assertTrue(iterator.hasNext());
        assertEquals(3L, iterator.next().getSearchIndex());
      }
    } finally {
      Files.deleteIfExists(dataWal.toPath());
      Files.deleteIfExists(successorWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testAdvanceToSkipsOnlyFullyCoveredWalFiles() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-covered-file-advance");
    final File localWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File followerWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 2, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX))
            .toFile();
    final File targetWal =
        dir.resolve(WALFileUtils.getLogFileName(2, 2, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(localWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
        writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 200L, 7, 2L));
      }
      try (WALWriter writer = new WALWriter(followerWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(-1L), singleEntryMeta(19, -1L, 1L, 300L, 8, 30L));
      }
      try (WALWriter writer = new WALWriter(targetWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(6L), singleEntryMeta(19, 6L, 1L, 600L, 7, 6L));
        writer.write(searchableEntry(7L), singleEntryMeta(19, 7L, 1L, 700L, 7, 7L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());

        iterator.advanceTo(6L, (physicalTime, nodeId, localSeq) -> nodeId == 7);

        assertTrue(iterator.hasNext());
        final IndexedConsensusRequest uncoveredFollower = iterator.next();
        assertEquals(-1L, uncoveredFollower.getSearchIndex());
        assertEquals(8, uncoveredFollower.getNodeId());
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());

        iterator.advanceTo(6L, (physicalTime, nodeId, localSeq) -> true);

        assertTrue(iterator.hasNext());
        assertEquals(6L, iterator.next().getSearchIndex());
      }
    } finally {
      Files.deleteIfExists(localWal.toPath());
      Files.deleteIfExists(followerWal.toPath());
      Files.deleteIfExists(targetWal.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testRefreshAfterExhaustionDiscoversNextWalFile() throws Exception {
    final Path dir = Files.createTempDirectory("progress-wal-iterator-refresh-after-exhaustion");
    final File firstWal =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File secondWal =
        dir.resolve(WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (WALWriter writer = new WALWriter(firstWal, WALFileVersion.V3)) {
        writer.write(searchableEntry(1L), singleEntryMeta(19, 1L, 1L, 100L, 7, 1L));
      }

      try (ProgressWALIterator iterator = new ProgressWALIterator(dir.toFile(), 1L)) {
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next().getSearchIndex());
        assertFalse(iterator.hasNext());

        try (WALWriter writer = new WALWriter(secondWal, WALFileVersion.V3)) {
          writer.write(searchableEntry(2L), singleEntryMeta(19, 2L, 1L, 200L, 7, 2L));
        }
        iterator.refresh();

        assertTrue(iterator.hasNext());
        assertEquals(2L, iterator.next().getSearchIndex());
      }
    } finally {
      Files.deleteIfExists(firstWal.toPath());
      Files.deleteIfExists(secondWal.toPath());
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

  private static ByteBuffer oversizedSearchableEntry(final int size, final long bodySearchIndex) {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(WALEntryType.INSERT_ROW_NODE.getCode());
    buffer.putLong(1L);
    buffer.putShort(PlanNodeType.INSERT_ROW.getNodeType());
    buffer.putLong(bodySearchIndex);
    buffer.position(size);
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
