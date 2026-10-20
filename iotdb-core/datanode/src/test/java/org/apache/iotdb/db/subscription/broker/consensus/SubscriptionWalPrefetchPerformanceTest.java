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

import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Manual comparisons of the iterator operations changed by subscription prefetch optimizations.
 * Both paths use the current implementation and the same sealed, uncompressed V3 WAL. Wall time is
 * diagnostic only; assertions cover work counts, writer progress and retry payload integrity.
 */
public class SubscriptionWalPrefetchPerformanceTest {

  private static final String PREFIX = "iotdb.test.subscription.performance.";
  private static final int FRAGMENTS = 2;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Enable with -Diotdb.test.subscription.performance.prefetch-overhead=true. Models a lagging
   * iterator and a pending batch that advances past a retained file with an uncovered follower.
   */
  @Test
  public void testNoIoStatsProbeAndCoalescedAdvancePerformance() throws Exception {
    assumeEnabled("prefetch-overhead");
    final int entryCount = positiveProperty("entries", 4096);
    final int batchSize = positiveProperty("batch-size", 64);
    final int iterations = positiveProperty("iterations", 5);
    final int warmups = positiveProperty("warmups", 2);
    assertTrue(entryCount >= 2);
    final File walDirectory = createWal(entryCount, true, batchSize);
    final long probeIndex = Math.max(1L, entryCount * 3L / 4L);

    final Sample[] probing = new Sample[iterations];
    final Sample[] peeking = new Sample[iterations];
    final Sample[] perEntry = new Sample[iterations];
    final Sample[] coalesced = new Sample[iterations];
    for (int round = 0; round < warmups + iterations; round++) {
      // Alternate order so the optimized path does not always follow the baseline.
      final boolean optimizedFirst = (round & 1) == 1;
      final Sample firstProbe = measureStatsProbe(walDirectory, probeIndex, optimizedFirst);
      final Sample secondProbe = measureStatsProbe(walDirectory, probeIndex, !optimizedFirst);
      final Sample firstAdvance =
          measureAdvance(walDirectory, entryCount, batchSize, optimizedFirst);
      final Sample secondAdvance =
          measureAdvance(walDirectory, entryCount, batchSize, !optimizedFirst);
      if (round >= warmups) {
        final int index = round - warmups;
        probing[index] = optimizedFirst ? secondProbe : firstProbe;
        peeking[index] = optimizedFirst ? firstProbe : secondProbe;
        perEntry[index] = optimizedFirst ? secondAdvance : firstAdvance;
        coalesced[index] = optimizedFirst ? firstAdvance : secondAdvance;
      }
    }

    printWorkload(entryCount, warmups, iterations);
    System.out.printf(Locale.ROOT, "batchSize=%d, probeSearchIndex=%d%n", batchSize, probeIndex);
    printComparison("stats probe (work=entry bodies read)", probing, peeking);
    printComparison("cursor advance (work=footer tuples checked)", perEntry, coalesced);
  }

  /**
   * Enable with -Diotdb.test.subscription.performance.memory-retry=true. Replays the queue's old
   * reset and new cache-restoration operations after each rejected materialization attempt.
   * Includes deserialization; excludes Tablet conversion, allocation and scheduler wait time.
   */
  @Test
  public void testMemoryBlockedRetryPerformance() throws Exception {
    assumeEnabled("memory-retry");
    final int entryCount = positiveProperty("entries", 4096);
    final int retryCount = positiveProperty("retries", 8);
    final int iterations = positiveProperty("iterations", 5);
    final int warmups = positiveProperty("warmups", 2);
    assertTrue(entryCount >= 2);
    final File walDirectory = createWal(entryCount, false, 0);
    final long retryIndex = Math.max(1L, entryCount * 3L / 4L);

    final Sample[] recreated = new Sample[iterations];
    final Sample[] cached = new Sample[iterations];
    for (int round = 0; round < warmups + iterations; round++) {
      final boolean optimizedFirst = (round & 1) == 1;
      final Sample first = measureRetries(walDirectory, retryIndex, retryCount, optimizedFirst);
      final Sample second = measureRetries(walDirectory, retryIndex, retryCount, !optimizedFirst);
      if (round >= warmups) {
        final int index = round - warmups;
        recreated[index] = optimizedFirst ? second : first;
        cached[index] = optimizedFirst ? first : second;
      }
    }

    printWorkload(entryCount, warmups, iterations);
    System.out.printf(Locale.ROOT, "attempts=%d, retrySearchIndex=%d%n", retryCount, retryIndex);
    printComparison("memory retry (work=entry bodies read)", recreated, cached);
  }

  private File createWal(final int entryCount, final boolean includeFollower, final int batchSize)
      throws Exception {
    final File directory = temporaryFolder.newFolder();
    final File historical =
        new File(
            directory, WALFileUtils.getLogFileName(0L, 0L, WALFileStatus.CONTAINS_SEARCH_INDEX));
    try (WALWriter writer = new WALWriter(historical, WALFileVersion.V3)) {
      for (long index = 1L; index <= entryCount; index++) {
        writeRequest(writer, index, 7, index);
      }
      if (includeFollower) {
        writeRequest(writer, -1L, 8, 1L);
      }
    }
    final File successor =
        new File(
            directory,
            WALFileUtils.getLogFileName(1L, entryCount, WALFileStatus.CONTAINS_SEARCH_INDEX));
    try (WALWriter writer = new WALWriter(successor, WALFileVersion.V3)) {
      if (includeFollower) {
        for (long index = entryCount + 1L; index <= entryCount + batchSize + 1L; index++) {
          writeRequest(writer, index, 7, index);
        }
      }
      // Even an empty successor makes the first file historical and readable.
    }
    return directory;
  }

  private static void writeRequest(
      final WALWriter writer, final long searchIndex, final int nodeId, final long localSeq)
      throws Exception {
    for (int fragment = 0; fragment < FRAGMENTS; fragment++) {
      final RelationalInsertRowNode node =
          StatementTestUtils.genInsertRowNode(Math.toIntExact(localSeq * FRAGMENTS + fragment));
      node.setSearchIndex(searchIndex);
      node.setLastFragment(fragment == FRAGMENTS - 1);
      final WALInfoEntry entry = new WALInfoEntry(1L, node);
      final ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
      entry.serialize(new WALByteBufferForTest(buffer));
      final WALMetaData metadata = new WALMetaData();
      metadata.add(entry.serializedSize(), searchIndex, 1L, 1000L + localSeq, nodeId, localSeq);
      // Small individual segments intentionally avoid changing global WAL compression settings.
      writer.write(buffer, metadata);
    }
  }

  private static Sample measureStatsProbe(
      final File directory, final long probeIndex, final boolean optimized) throws Exception {
    final Field bodyCount = bodyCountField();
    try (ProgressWALIterator iterator = new ProgressWALIterator(directory, probeIndex)) {
      final long startNanos = System.nanoTime();
      final boolean ready = optimized ? iterator.hasBufferedNext() : iterator.hasNext();
      final long elapsedNanos = System.nanoTime() - startNanos;
      assertEquals(!optimized, ready);
      final long readBodies = bodyCount.getInt(iterator);
      // One lookahead fragment is read to establish the end of a logical request.
      assertEquals(optimized ? 0L : probeIndex * FRAGMENTS + 1L, readBodies);
      assertRequest(iterator.next(), probeIndex, 7, probeIndex);
      return new Sample(elapsedNanos, readBodies);
    }
  }

  private static Sample measureAdvance(
      final File directory, final int entryCount, final int batchSize, final boolean optimized)
      throws Exception {
    final long[] footerTuples = {0L};
    final ProgressWALIterator.WriterProgressCoverage coverage =
        (physicalTime, nodeId, localSeq) -> {
          footerTuples[0]++;
          return nodeId == 7 && localSeq <= entryCount;
        };
    try (ProgressWALIterator iterator = new ProgressWALIterator(directory, 1L)) {
      final long startNanos = System.nanoTime();
      if (optimized) {
        iterator.advanceTo(entryCount + batchSize + 1L, coverage);
      } else {
        for (int index = 1; index <= batchSize; index++) {
          iterator.advanceTo(entryCount + index + 1L, coverage);
        }
      }
      final long elapsedNanos = System.nanoTime() - startNanos;
      // The first follower tuple prevents skipping the retained file on every coverage check.
      assertEquals(
          (entryCount * (long) FRAGMENTS + 1L) * (optimized ? 1L : batchSize), footerTuples[0]);
      assertRequest(iterator.next(), -1L, 8, 1L);
      final long nextIndex = entryCount + batchSize + 1L;
      assertRequest(iterator.next(), nextIndex, 7, nextIndex);
      assertFalse(iterator.hasNext());
      return new Sample(elapsedNanos, footerTuples[0]);
    }
  }

  private static Sample measureRetries(
      final File directory, final long retryIndex, final int attempts, final boolean optimized)
      throws Exception {
    final Field bodyCount = bodyCountField();
    ProgressWALIterator iterator = null;
    IndexedConsensusRequest previousRequest = null;
    long readBodies = 0L;
    int previousBodyCount = 0;
    try {
      final long startNanos = System.nanoTime();
      for (int attempt = 0; attempt < attempts; attempt++) {
        if (iterator == null) {
          iterator = new ProgressWALIterator(directory, retryIndex);
        }
        final IndexedConsensusRequest request = iterator.next();
        assertRequest(request, retryIndex, 7, retryIndex);
        final int currentBodyCount = bodyCount.getInt(iterator);
        readBodies += optimized ? currentBodyCount - previousBodyCount : currentBodyCount;
        previousBodyCount = currentBodyCount;
        if (optimized && previousRequest != null) {
          assertSame(previousRequest, request);
        }
        previousRequest = request;
        if (attempt + 1 < attempts) {
          if (optimized) {
            iterator.retry(request);
          } else {
            iterator.close();
            iterator = null;
          }
        }
      }
      final long elapsedNanos = System.nanoTime() - startNanos;
      assertEquals((retryIndex * FRAGMENTS + 1L) * (optimized ? 1L : attempts), readBodies);
      // The last attempt succeeds: the pending lookahead must still lead to the next request.
      assertRequest(iterator.next(), retryIndex + 1L, 7, retryIndex + 1L);
      return new Sample(elapsedNanos, readBodies);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  private static void assertRequest(
      final IndexedConsensusRequest request,
      final long searchIndex,
      final int nodeId,
      final long localSeq) {
    assertEquals(searchIndex, request.getSearchIndex());
    assertEquals(nodeId, request.getNodeId());
    assertEquals(localSeq, request.getProgressLocalSeq());
    assertEquals(1000L + localSeq, request.getPhysicalTime());
    assertEquals(FRAGMENTS, request.getRequests().size());
    final InsertNode insert = ConsensusLogToTabletConverter.deserializeToInsertNode(request);
    assertNotNull(insert);
    assertEquals(searchIndex, insert.getSearchIndex());
    assertEquals(nodeId, insert.getNodeId());
    assertEquals(localSeq, insert.getSyncIndex());
    assertEquals(1000L + localSeq, insert.getPhysicalTime());
    assertTrue(insert instanceof InsertRowsNode);
    final InsertRowsNode rows = (InsertRowsNode) insert;
    assertEquals(FRAGMENTS, rows.getInsertRowNodeList().size());
    for (int fragment = 0; fragment < FRAGMENTS; fragment++) {
      final InsertRowNode row = rows.getInsertRowNodeList().get(fragment);
      final long expectedValue = localSeq * FRAGMENTS + fragment;
      assertEquals(expectedValue, row.getTime());
      assertEquals((double) expectedValue, (Double) row.getValues()[2], 0.0);
    }
    for (final IConsensusRequest fragment : request.getRequests()) {
      assertEquals(0, fragment.serializeToByteBuffer().position());
    }
  }

  private static Field bodyCountField() throws Exception {
    // All counted reads remain in one historical file; inspect existing state without a
    // production instrumentation hook or overhead in WALReader.next().
    final Field field =
        ProgressWALIterator.class.getDeclaredField("consumedEntryCountInCurrentFile");
    field.setAccessible(true);
    return field;
  }

  private static void assumeEnabled(final String benchmark) {
    Assume.assumeTrue(
        "Enable with -D" + PREFIX + benchmark + "=true", Boolean.getBoolean(PREFIX + benchmark));
  }

  private static int positiveProperty(final String name, final int defaultValue) {
    final int value = Integer.getInteger(PREFIX + name, defaultValue);
    assertTrue(name + " must be positive", value > 0);
    return value;
  }

  private static void printWorkload(final int entryCount, final int warmups, final int iterations) {
    System.out.printf(
        Locale.ROOT,
        "Subscription WAL benchmark: requests=%d, fragments=%d, warmups=%d, samples=%d, "
            + "os=%s, java=%s%n",
        entryCount,
        FRAGMENTS,
        warmups,
        iterations,
        System.getProperty("os.name"),
        System.getProperty("java.version"));
  }

  private static void printComparison(
      final String name, final Sample[] baseline, final Sample[] optimized) {
    final double baselineNanos = medianNanos(baseline);
    final double optimizedNanos = medianNanos(optimized);
    System.out.printf(
        Locale.ROOT,
        "%s: baselineMedian=%.6f ms, optimizedMedian=%.6f ms, speedup=%.2fx, "
            + "baselineWork=%d, optimizedWork=%d%n",
        name,
        baselineNanos / 1_000_000.0,
        optimizedNanos / 1_000_000.0,
        baselineNanos / Math.max(1.0, optimizedNanos),
        baseline[0].work,
        optimized[0].work);
  }

  private static double medianNanos(final Sample[] samples) {
    final long[] values = new long[samples.length];
    for (int index = 0; index < samples.length; index++) {
      values[index] = samples[index].elapsedNanos;
    }
    Arrays.sort(values);
    final int middle = values.length / 2;
    return values.length % 2 == 0
        ? values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0
        : values[middle];
  }

  private static final class Sample {
    private final long elapsedNanos;
    private final long work;

    private Sample(final long elapsedNanos, final long work) {
      this.elapsedNanos = elapsedNanos;
      this.work = work;
    }
  }
}
