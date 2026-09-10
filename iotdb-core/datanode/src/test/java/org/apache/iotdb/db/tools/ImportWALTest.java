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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALSignalEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ILogWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.tools.ImportWAL.ReplayResult;
import org.apache.iotdb.db.tools.ImportWAL.WALReplayer.ReplayDecision;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ImportWALTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Covers recursive directory discovery with WAL versions 2 and 10 in separate node folders. The
   * result must ignore non-WAL files and preserve parent-folder then numeric-version order.
   */
  @Test
  public void testCollectWALFilesRecursivelyAndSortByVersion() throws IOException {
    final Path source = temporaryFolder.newFolder("wal-root").toPath();
    final Path nodeA = Files.createDirectory(source.resolve("node-a"));
    final Path nodeB = Files.createDirectory(source.resolve("node-b"));
    final Path a10 = createWALFile(nodeA, 10);
    final Path a2 = createWALFile(nodeA, 2);
    final Path b1 = createWALFile(nodeB, 1);
    Files.createFile(nodeA.resolve("ignore.txt"));

    final List<Path> files = ImportWAL.collectWALFiles(source);

    assertEquals(
        Arrays.asList(
            a2.toAbsolutePath().normalize(),
            a10.toAbsolutePath().normalize(),
            b1.toAbsolutePath().normalize()),
        files);
  }

  /** Covers CLI discovery of source-file and parallel replay options without opening a Session. */
  @Test
  public void testHelpDescribesDeleteSourceOption() {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();

    final int exitCode =
        ImportWAL.run(new String[] {"--help"}, new PrintStream(output), new PrintStream(output));

    assertEquals(0, exitCode);
    assertTrue(output.toString().contains("--on_success"));
    assertTrue(output.toString().contains("--thread_num"));
  }

  /** Covers valid thread counts and rejects zero, negative, and non-numeric values. */
  @Test
  public void testParseThreadNumOption() {
    assertEquals(1, ImportWAL.parseThreadNum("1"));
    assertEquals(4, ImportWAL.parseThreadNum("4"));
    assertThrows(IllegalArgumentException.class, () -> ImportWAL.parseThreadNum("0"));
    assertThrows(IllegalArgumentException.class, () -> ImportWAL.parseThreadNum("-1"));
    assertThrows(IllegalArgumentException.class, () -> ImportWAL.parseThreadNum("invalid"));
  }

  /**
   * Covers directory-level parallel replay with two WAL files per directory. Different directories
   * must overlap, while versions within each directory must retain ascending replay order.
   */
  @Test
  public void testReplayWALDirectoriesInParallelAndPreserveDirectoryOrder() throws Exception {
    final Path source = temporaryFolder.newFolder("parallel-wal-root").toPath();
    final Path nodeA = Files.createDirectory(source.resolve("node-a"));
    final Path nodeB = Files.createDirectory(source.resolve("node-b"));
    final Path a1 = createWALFile(nodeA, 1);
    final Path a2 = createWALFile(nodeA, 2);
    final Path b1 = createWALFile(nodeB, 1);
    final Path b2 = createWALFile(nodeB, 2);
    writeWAL(a1.toFile(), new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.a", 1)));
    writeWAL(a2.toFile(), new WALInfoEntry(2, WALTestUtils.getInsertRowNode("root.sg.a", 2)));
    writeWAL(b1.toFile(), new WALInfoEntry(3, WALTestUtils.getInsertRowNode("root.sg.b", 1)));
    writeWAL(b2.toFile(), new WALInfoEntry(4, WALTestUtils.getInsertRowNode("root.sg.b", 2)));
    final List<Path> walFiles = ImportWAL.collectWALFiles(source);
    final CyclicBarrier replayBarrier = new CyclicBarrier(2);
    final AtomicInteger activeReplays = new AtomicInteger();
    final AtomicInteger maxActiveReplays = new AtomicInteger();
    final AtomicInteger createdWorkers = new AtomicInteger();
    final Map<String, List<Long>> replayedTimestamps = new ConcurrentHashMap<>();

    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALDirectories(
            walFiles,
            2,
            () -> {
              createdWorkers.incrementAndGet();
              final Session session = mock(Session.class);
              doAnswer(
                      invocation -> {
                        final Tablet tablet = invocation.getArgument(0);
                        final int active = activeReplays.incrementAndGet();
                        maxActiveReplays.accumulateAndGet(active, Math::max);
                        try {
                          replayBarrier.await(5, TimeUnit.SECONDS);
                          replayedTimestamps
                              .computeIfAbsent(
                                  tablet.getDeviceId(), ignored -> new CopyOnWriteArrayList<>())
                              .add(tablet.getTimestamp(0));
                        } finally {
                          activeReplays.decrementAndGet();
                        }
                        return null;
                      })
                  .when(session)
                  .insertTablet(any(Tablet.class));
              return new ImportWAL.WALReplayer(session, null, null);
            },
            null,
            false);

    assertEquals(2, createdWorkers.get());
    assertTrue(maxActiveReplays.get() >= 2);
    assertEquals(Arrays.asList(1L, 2L), replayedTimestamps.get("root.sg.a"));
    assertEquals(Arrays.asList(1L, 2L), replayedTimestamps.get("root.sg.b"));
    assertEquals(4, statistics.getReplayedOperationCount());
    assertEquals(4, statistics.getCompletedFileCount());
  }

  /** Covers the default retention value, deletion value, normalization, and invalid input. */
  @Test
  public void testParseOnSuccessOption() {
    assertFalse(ImportWAL.shouldDeleteSource("none"));
    assertTrue(ImportWAL.shouldDeleteSource(" DELETE "));
    assertThrows(IllegalArgumentException.class, () -> ImportWAL.shouldDeleteSource("unsupported"));
  }

  /**
   * Covers a real WAL file containing one tree insert and one internal signal. The insert must be
   * sent once as a Tablet, while the signal is counted as skipped and no corruption is reported.
   */
  @Test
  public void testReplayWALFileReplaysInsertAndSkipsInternalEntry() throws Exception {
    final File walFile = createWALFile(0);
    final InsertRowNode rowNode = WALTestUtils.getInsertRowNode("root.sg.d1", 100);
    writeWAL(walFile, new WALInfoEntry(1, rowNode), new WALSignalEntry(WALEntryType.CLOSE_SIGNAL));
    final Session treeSession = mock(Session.class);

    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALFiles(
            Collections.singletonList(walFile.toPath()),
            new ImportWAL.WALReplayer(treeSession, null, null));

    assertEquals(1, statistics.getReplayedOperationCount());
    assertEquals(1, statistics.getSkippedEntryCount());
    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(treeSession).insertTablet(tabletCaptor.capture());
    assertEquals("root.sg.d1", tabletCaptor.getValue().getDeviceId());
    assertEquals(100, tabletCaptor.getValue().getTimestamp(0));
    assertTrue(walFile.exists());
  }

  /** Covers opt-in deletion after every source WAL file completes successfully. */
  @Test
  public void testReplayDeletesSourceFilesAfterAllFilesSucceed() throws Exception {
    final File firstWALFile = createWALFile(0);
    final File secondWALFile = createWALFile(1);
    writeWAL(
        firstWALFile, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.delete.d1", 1)));
    writeWAL(
        secondWALFile, new WALInfoEntry(2, WALTestUtils.getInsertRowNode("root.sg.delete.d2", 2)));

    ImportWAL.replayWALFiles(
        Arrays.asList(firstWALFile.toPath(), secondWALFile.toPath()),
        new ImportWAL.WALReplayer(mock(Session.class), null, null),
        null,
        true);

    assertFalse(firstWALFile.exists());
    assertFalse(secondWALFile.exists());
  }

  /** Serial replay must retain tree/table deletes skipped with SKIP and delete complete WALs. */
  @Test
  public void testReplayRetainsSourceFilesWithSkippedDataEntries() throws Exception {
    assertReplayRetainsSkippedSourceFiles(ReplayDecision.SKIP, false);
  }

  /** SKIP_ALL must preserve the same source files as SKIP during serial replay. */
  @Test
  public void testReplayRetainsSourceFilesWithSkipAllDataEntries() throws Exception {
    assertReplayRetainsSkippedSourceFiles(ReplayDecision.SKIP_ALL, false);
  }

  /** Parallel workers must retain skipped data without preventing deletion of complete WALs. */
  @Test
  public void testParallelReplayRetainsSourceFilesWithSkippedDataEntries() throws Exception {
    assertReplayRetainsSkippedSourceFiles(ReplayDecision.SKIP, true);
  }

  /** SKIP_ALL must preserve data skipped by either worker during directory-level replay. */
  @Test
  public void testParallelReplayRetainsSourceFilesWithSkipAllDataEntries() throws Exception {
    assertReplayRetainsSkippedSourceFiles(ReplayDecision.SKIP_ALL, true);
  }

  private void assertReplayRetainsSkippedSourceFiles(
      final ReplayDecision decision, final boolean parallel) throws Exception {
    final Path source = temporaryFolder.newFolder("skip-wal-root").toPath();
    final Path nodeA = Files.createDirectory(source.resolve("node-a"));
    final Path nodeB = Files.createDirectory(source.resolve("node-b"));
    final Path treeDeleteWAL = createWALFile(nodeA, 0);
    final Path completeWAL = createWALFile(nodeA, 1);
    final Path tableDeleteWAL = createWALFile(nodeB, 0);
    final DeleteDataNode treeDelete =
        new DeleteDataNode(
            new PlanNodeId(""), List.of(new MeasurementPath("root.sg.d1.s1")), 10, 20);
    final RelationalDeleteDataNode tableDelete =
        new RelationalDeleteDataNode(
            new PlanNodeId(""),
            new TableDeletionEntry(new DeletionPredicate("table1"), new TimeRange(10, 20)),
            "db");
    final WALInfoEntry insert = new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1));
    // A later successful insert must not hide an earlier skip in the same file, and skips must
    // not prevent deletion of a fully replayed file in the same directory.
    writeWAL(treeDeleteWAL.toFile(), new WALInfoEntry(1, treeDelete), insert);
    writeWAL(tableDeleteWAL.toFile(), insert, new WALInfoEntry(1, tableDelete));
    // Signals, separators, and empty snapshots carry no pending data and remain deletable.
    writeWAL(
        completeWAL.toFile(),
        insert,
        new WALSignalEntry(WALEntryType.CLOSE_SIGNAL),
        new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL),
        new WALInfoEntry(1, new ContinuousSameSearchIndexSeparatorNode()),
        new WALInfoEntry(1, new PrimitiveMemTable()));
    final byte[] treeDeleteBytes = Files.readAllBytes(treeDeleteWAL);
    final byte[] tableDeleteBytes = Files.readAllBytes(tableDeleteWAL);
    final Session session = mock(Session.class);
    final List<Path> walFiles = ImportWAL.collectWALFiles(source);
    final ImportWAL.ReplayStatistics statistics =
        parallel
            ? ImportWAL.replayWALDirectories(
                walFiles,
                2,
                () ->
                    new ImportWAL.WALReplayer(session, null, null, (entry, treeModel) -> decision),
                null,
                true)
            : ImportWAL.replayWALFiles(
                walFiles,
                new ImportWAL.WALReplayer(session, null, null, (entry, treeModel) -> decision),
                null,
                true);

    assertTrue(Files.exists(treeDeleteWAL));
    assertTrue(Files.exists(tableDeleteWAL));
    assertArrayEquals(treeDeleteBytes, Files.readAllBytes(treeDeleteWAL));
    assertArrayEquals(tableDeleteBytes, Files.readAllBytes(tableDeleteWAL));
    assertFalse(Files.exists(completeWAL));
    assertEquals(3, statistics.getReplayedOperationCount());
    assertEquals(6, statistics.getSkippedEntryCount());
    assertEquals(3, statistics.getCompletedFileCount());
    verify(session, times(3)).insertTablet(any(Tablet.class));
    verify(session, never()).deleteData(any(), anyLong(), anyLong());
  }

  /** Covers all-or-nothing replay gating: a later failure must retain every source WAL file. */
  @Test
  public void testReplayRetainsAllSourceFilesWhenAnyFileFails() throws Exception {
    final File validWALFile = createWALFile(0);
    final File corruptedWALFile = createWALFile(1);
    writeWAL(
        validWALFile, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.retain.d1", 1)));
    Files.write(corruptedWALFile.toPath(), new byte[] {WALEntryType.INSERT_ROW_NODE.getCode()});

    assertThrows(
        IOException.class,
        () ->
            ImportWAL.replayWALFiles(
                Arrays.asList(validWALFile.toPath(), corruptedWALFile.toPath()),
                new ImportWAL.WALReplayer(mock(Session.class), null, null),
                null,
                true));

    assertTrue(validWALFile.exists());
    assertTrue(corruptedWALFile.exists());
  }

  /**
   * Covers all-or-nothing deletion during directory-level parallel replay. A corrupted directory
   * must retain WAL files from both the failed directory and another concurrently replayed one.
   */
  @Test
  public void testParallelReplayRetainsAllSourceFilesWhenAnyDirectoryFails() throws Exception {
    final Path source = temporaryFolder.newFolder("parallel-retain-wal-root").toPath();
    final Path validDirectory = Files.createDirectory(source.resolve("valid"));
    final Path corruptedDirectory = Files.createDirectory(source.resolve("corrupted"));
    final Path validWALFile = createWALFile(validDirectory, 0);
    final Path corruptedWALFile = createWALFile(corruptedDirectory, 0);
    writeWAL(
        validWALFile.toFile(),
        new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.parallel.retain", 1)));
    Files.write(corruptedWALFile, new byte[] {WALEntryType.INSERT_ROW_NODE.getCode()});

    assertThrows(
        IOException.class,
        () ->
            ImportWAL.replayWALDirectories(
                ImportWAL.collectWALFiles(source),
                2,
                () -> new ImportWAL.WALReplayer(mock(Session.class), null, null),
                null,
                true));

    assertTrue(Files.exists(validWALFile));
    assertTrue(Files.exists(corruptedWALFile));
  }

  @Test
  public void testReplayReportsProgressAndFileStatistics() throws Exception {
    final File walFile = createWALFile(0);
    writeWAL(
        walFile,
        new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.progress", 100)),
        new WALSignalEntry(WALEntryType.CLOSE_SIGNAL));
    final ByteArrayOutputStream output = new ByteArrayOutputStream();

    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALFiles(
            Collections.singletonList(walFile.toPath()),
            new ImportWAL.WALReplayer(mock(Session.class), null, null),
            new PrintStream(output));

    assertEquals(1, statistics.getCompletedFileCount());
    assertEquals(Files.size(walFile.toPath()), statistics.getTotalBytes());
    assertTrue(statistics.getElapsedSeconds() >= 0);
    assertTrue(output.toString().contains("1/1"));
  }

  /**
   * Covers an active WAL containing one complete entry but no end marker or metadata. Replay must
   * treat EOF at the entry boundary as clean and import the entry without waiting for writer close.
   */
  @Test
  public void testReplayActiveWALFileAtEntryBoundary() throws Exception {
    final File walFile = createWALFile(0);
    final InsertRowNode rowNode = WALTestUtils.getInsertRowNode("root.sg.active", 102);
    final Session treeSession = mock(Session.class);

    try (WALWriter writer = new WALWriter(walFile)) {
      writer.write(serializeWAL(new WALInfoEntry(1, rowNode)));
      writer.force();

      final ImportWAL.ReplayStatistics statistics =
          ImportWAL.replayWALFiles(
              Collections.singletonList(walFile.toPath()),
              new ImportWAL.WALReplayer(treeSession, null, null));

      assertEquals(1, statistics.getReplayedOperationCount());
      assertEquals(0, statistics.getSkippedEntryCount());
      verify(treeSession).insertTablet(any(Tablet.class));
    }
  }

  /**
   * Covers an aligned tree row. The replay must use the aligned Session API and must not fall back
   * to the non-aligned Tablet API.
   */
  @Test
  public void testReplayAlignedTreeInsertUsesAlignedSessionAPI() throws Exception {
    final InsertRowNode rowNode = WALTestUtils.getInsertRowNode("root.sg.aligned", 101);
    rowNode.setAligned(true);
    final Session treeSession = mock(Session.class);

    new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, rowNode));

    verify(treeSession).insertAlignedTablet(any(Tablet.class));
    verify(treeSession, never()).insertTablet(any(Tablet.class));
  }

  /**
   * Covers a table-model WAL tablet with an explicit target database. Replay must use the table
   * Session and retain the source table name in the converted Tablet.
   */
  @Test
  public void testReplayTableInsertUsesRelationalSessionAPI() throws Exception {
    final RelationalInsertTabletNode node = WALFileTest.getRelationalInsertTabletNode("table1");
    final Session treeSession = mock(Session.class);
    final Session tableSession = mock(Session.class);

    new ImportWAL.WALReplayer(treeSession, tableSession, "db").replay(new WALInfoEntry(1, node));

    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(tableSession).insertRelationalTablet(tabletCaptor.capture());
    assertEquals("table1", tabletCaptor.getValue().getTableName());
    verify(treeSession, never()).insertTablet(any(Tablet.class));
  }

  /**
   * Covers a table-model WAL insert without a target database. Replay must fail before issuing any
   * write because WAL insert entries do not carry their source database name.
   */
  @Test
  public void testReplayTableInsertRequiresDatabase() throws Exception {
    final RelationalInsertTabletNode node = WALFileTest.getRelationalInsertTabletNode("table1");
    final Session treeSession = mock(Session.class);

    assertThrows(
        StatementExecutionException.class,
        () -> new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, node)));

    verify(treeSession, never()).insertTablet(any(Tablet.class));
  }

  /**
   * Covers a tree deletion with multiple paths and a bounded time range. Replay must pass the
   * original paths and inclusive time bounds to Session.deleteData.
   */
  @Test
  public void testReplayTreeDeletePreservesPathsAndTimeRange() throws Exception {
    final DeleteDataNode deleteNode =
        new DeleteDataNode(
            new PlanNodeId(""),
            Arrays.asList(
                new MeasurementPath("root.sg.d1.s1"), new MeasurementPath("root.sg.d2.*")),
            10,
            20);
    final Session treeSession = mock(Session.class);

    new ImportWAL.WALReplayer(
            treeSession,
            null,
            null,
            (entry, treeDelete) -> ImportWAL.WALReplayer.ReplayDecision.EXECUTE)
        .replay(new WALInfoEntry(1, deleteNode));

    verify(treeSession)
        .deleteData(eq(Arrays.asList("root.sg.d1.s1", "root.sg.d2.*")), eq(10L), eq(20L));
  }

  /** A skipped tree delete must be distinguished from a control entry that needs no replay. */
  @Test
  public void testReplayTreeDeleteSkipsAfterConfirmation() throws Exception {
    final DeleteDataNode deleteNode =
        new DeleteDataNode(
            new PlanNodeId(""), List.of(new MeasurementPath("root.sg.d1.s1")), 10, 20);
    final Session treeSession = mock(Session.class);

    final ReplayResult result =
        new ImportWAL.WALReplayer(
                treeSession,
                null,
                null,
                (entry, treeDelete) -> ImportWAL.WALReplayer.ReplayDecision.SKIP)
            .replay(new WALInfoEntry(1, deleteNode));

    assertEquals(ReplayResult.SKIPPED, result);
    verify(treeSession, never()).deleteData(any(), anyLong(), anyLong());
  }

  /** Execute-all replays deletes; skip-all reports skipped data for each affected worker. */
  @Test
  public void testReplayTreeDeleteExecuteAllAndSkipAllDecisions() throws Exception {
    final DeleteDataNode deleteNode =
        new DeleteDataNode(
            new PlanNodeId(""), List.of(new MeasurementPath("root.sg.d1.s1")), 10, 20);
    final Session treeSession = mock(Session.class);
    final AtomicInteger executeAllPromptCount = new AtomicInteger();
    final ImportWAL.WALReplayer.ReplayDecisionPrompt executeAllPrompt =
        (entry, treeDelete) -> {
          executeAllPromptCount.incrementAndGet();
          return ImportWAL.WALReplayer.ReplayDecision.EXECUTE_ALL;
        };
    final ImportWAL.WALReplayer firstExecuteAllReplayer =
        new ImportWAL.WALReplayer(treeSession, null, null, executeAllPrompt);
    final ImportWAL.WALReplayer secondExecuteAllReplayer =
        new ImportWAL.WALReplayer(treeSession, null, null, executeAllPrompt);

    assertEquals(
        ReplayResult.REPLAYED, firstExecuteAllReplayer.replay(new WALInfoEntry(1, deleteNode)));
    assertEquals(
        ReplayResult.REPLAYED, secondExecuteAllReplayer.replay(new WALInfoEntry(2, deleteNode)));
    assertEquals(2, executeAllPromptCount.get());
    verify(treeSession, times(2)).deleteData(any(), eq(10L), eq(20L));

    final Session skippedTreeSession = mock(Session.class);
    final AtomicInteger skipAllPromptCount = new AtomicInteger();
    final ImportWAL.WALReplayer.ReplayDecisionPrompt skipAllPrompt =
        (entry, treeDelete) -> {
          skipAllPromptCount.incrementAndGet();
          return ImportWAL.WALReplayer.ReplayDecision.SKIP_ALL;
        };
    final ImportWAL.WALReplayer firstSkipAllReplayer =
        new ImportWAL.WALReplayer(skippedTreeSession, null, null, skipAllPrompt);
    final ImportWAL.WALReplayer secondSkipAllReplayer =
        new ImportWAL.WALReplayer(skippedTreeSession, null, null, skipAllPrompt);

    assertEquals(
        ReplayResult.SKIPPED, firstSkipAllReplayer.replay(new WALInfoEntry(1, deleteNode)));
    assertEquals(
        ReplayResult.SKIPPED, secondSkipAllReplayer.replay(new WALInfoEntry(2, deleteNode)));
    assertEquals(2, skipAllPromptCount.get());
    verify(skippedTreeSession, never()).deleteData(any(), anyLong(), anyLong());
  }

  @Test
  public void testReplayTreeDeleteTerminatesAfterConfirmation() throws Exception {
    final DeleteDataNode deleteNode =
        new DeleteDataNode(
            new PlanNodeId(""), List.of(new MeasurementPath("root.sg.d1.s1")), 10, 20);

    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    null,
                    null,
                    (entry, treeDelete) -> ImportWAL.WALReplayer.ReplayDecision.TERMINATE)
                .replay(new WALInfoEntry(1, deleteNode)));
  }

  /** Covers an unsupported entry when the interactive user explicitly chooses to skip it. */
  @Test
  public void testReplayUnsupportedEntrySkipsAfterConfirmation() throws Exception {
    final WALEntry entry = mockUnsupportedEntry();

    final ReplayResult result =
        new ImportWAL.WALReplayer(
                mock(Session.class),
                null,
                null,
                (ignored, treeDelete) -> ImportWAL.WALReplayer.ReplayDecision.SKIP)
            .replay(entry);

    assertEquals(ReplayResult.SKIPPED, result);
  }

  /** Table deletes and ObjectNode entries skipped with SKIP_ALL must both retain their WALs. */
  @Test
  public void testReplayUnsupportedEntriesSkipAllAfterConfirmation() throws Exception {
    final AtomicInteger promptCount = new AtomicInteger();
    final ImportWAL.WALReplayer.ReplayDecisionPrompt skipAllPrompt =
        (entry, treeDelete) -> {
          promptCount.incrementAndGet();
          return ImportWAL.WALReplayer.ReplayDecision.SKIP_ALL;
        };
    final ImportWAL.WALReplayer relationalDeleteReplayer =
        new ImportWAL.WALReplayer(mock(Session.class), null, null, skipAllPrompt);

    assertEquals(ReplayResult.SKIPPED, relationalDeleteReplayer.replay(mockUnsupportedEntry()));
    assertEquals(ReplayResult.SKIPPED, relationalDeleteReplayer.replay(mockUnsupportedEntry()));

    final WALEntry objectEntry = mock(WALEntry.class);
    when(objectEntry.getType()).thenReturn(WALEntryType.OBJECT_FILE_NODE);
    when(objectEntry.getValue()).thenReturn(mock(ObjectNode.class));
    final ImportWAL.WALReplayer objectNodeReplayer =
        new ImportWAL.WALReplayer(mock(Session.class), null, null, skipAllPrompt);
    assertEquals(ReplayResult.SKIPPED, objectNodeReplayer.replay(objectEntry));
    assertEquals(3, promptCount.get());
  }

  /** Covers an unsupported entry when the interactive user declines the skip prompt. */
  @Test
  public void testReplayUnsupportedEntryFailsAfterDecliningSkip() {
    final WALEntry entry = mockUnsupportedEntry();

    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    null,
                    null,
                    (ignored, treeDelete) -> ImportWAL.WALReplayer.ReplayDecision.TERMINATE)
                .replay(entry));
  }

  /** Covers non-interactive execution, which must retain the original fail-fast behavior. */
  @Test
  public void testReplayUnsupportedEntryFailsWithoutInteractiveInput() {
    final WALEntry entry = mockUnsupportedEntry();

    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    null,
                    null,
                    new ImportWAL.WALReplayer.ReplayDecisionController((java.io.Console) null))
                .replay(entry));
  }

  @Test
  public void testReplayDecisionParsing() {
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.EXECUTE,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("e", true));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.SKIP,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("s", true));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.EXECUTE_ALL,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("a", true));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.SKIP_ALL,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("l", true));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.TERMINATE,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("a", false));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.SKIP_ALL,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("l", false));
    assertEquals(
        ImportWAL.WALReplayer.ReplayDecision.TERMINATE,
        ImportWAL.WALReplayer.ReplayDecisionController.parseDecision("q", true));
  }

  /** Covers a non-aligned snapshot whose measurements have independent time axes. */
  @Test
  public void testReplayNonAlignedMemTableSnapshotAsTablets() throws Exception {
    final PrimitiveMemTable memTable = new PrimitiveMemTable("root.sg", "0");
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT64));
    final StringArrayDeviceID deviceId = new StringArrayDeviceID("root.sg.d1");
    memTable.write(deviceId, schemas, 3, new Object[] {30, 300L});
    memTable.write(deviceId, schemas, 1, new Object[] {10, null});
    final Session treeSession = mock(Session.class);

    new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, memTable));

    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(treeSession, times(2)).insertTablet(tabletCaptor.capture());
    final Tablet s1Tablet =
        tabletCaptor.getAllValues().stream()
            .filter(tablet -> "s1".equals(tablet.getSchemas().get(0).getMeasurementName()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    assertEquals(2, s1Tablet.getRowSize());
    assertEquals(1, s1Tablet.getTimestamp(0));
    assertEquals(3, s1Tablet.getTimestamp(1));
    assertArrayEquals(new int[] {10, 30}, (int[]) s1Tablet.getValues()[0]);
  }

  /** Covers an aligned snapshot with nulls and verifies the aligned Session API is used. */
  @Test
  public void testReplayAlignedMemTableSnapshotPreservesNulls() throws Exception {
    final PrimitiveMemTable memTable = new PrimitiveMemTable("root.sg", "0");
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT64));
    final StringArrayDeviceID deviceId = new StringArrayDeviceID("root.sg.d1");
    memTable.writeAlignedRow(deviceId, schemas, 2, new Object[] {20, null});
    memTable.writeAlignedRow(deviceId, schemas, 1, new Object[] {10, 100L});
    final Session treeSession = mock(Session.class);

    new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, memTable));

    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(treeSession).insertAlignedTablet(tabletCaptor.capture());
    verify(treeSession, never()).insertTablet(any(Tablet.class));
    final Tablet tablet = tabletCaptor.getValue();
    assertEquals(2, tablet.getRowSize());
    assertEquals(1, tablet.getTimestamp(0));
    assertEquals(2, tablet.getTimestamp(1));
    assertArrayEquals(new int[] {10, 20}, (int[]) tablet.getValues()[0]);
    assertTrue(tablet.getBitMaps()[1].isMarked(1));
  }

  /** Covers snapshot serialization and deserialization through a real WAL file. */
  @Test
  public void testReplaySerializedMemTableSnapshot() throws Exception {
    final PrimitiveMemTable memTable = new PrimitiveMemTable("root.sg", "0");
    memTable.write(
        new StringArrayDeviceID("root.sg.d1"),
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)),
        7,
        new Object[] {70});
    final File walFile = createWALFile(0);
    writeWAL(walFile, new WALInfoEntry(1, memTable));
    final Session treeSession = mock(Session.class);

    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALFiles(
            Collections.singletonList(walFile.toPath()),
            new ImportWAL.WALReplayer(treeSession, null, null));

    assertEquals(1, statistics.getReplayedOperationCount());
    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(treeSession).insertTablet(tabletCaptor.capture());
    assertEquals(7, tabletCaptor.getValue().getTimestamp(0));
    assertEquals(70, ((int[]) tabletCaptor.getValue().getValues()[0])[0]);
  }

  /** Covers a snapshot larger than the replay batch limit. */
  @Test
  public void testReplayMemTableSnapshotSplitsLargeChunk() throws Exception {
    final PrimitiveMemTable memTable = new PrimitiveMemTable("root.sg", "0");
    final List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32));
    final StringArrayDeviceID deviceId = new StringArrayDeviceID("root.sg.d1");
    for (int i = 0; i < 1025; i++) {
      memTable.write(deviceId, schemas, i, new Object[] {i});
    }
    final Session treeSession = mock(Session.class);

    new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, memTable));

    final ArgumentCaptor<Tablet> tabletCaptor = ArgumentCaptor.forClass(Tablet.class);
    verify(treeSession, times(2)).insertTablet(tabletCaptor.capture());
    assertEquals(1024, tabletCaptor.getAllValues().get(0).getRowSize());
    assertEquals(1, tabletCaptor.getAllValues().get(1).getRowSize());
  }

  /** A signal snapshot carries no user data and must not prevent deletion of its source WAL. */
  @Test
  public void testReplaySignalMemTableSnapshotIsSkipped() throws Exception {
    final IMemTable signalMemTable = mock(IMemTable.class);
    when(signalMemTable.isSignalMemTable()).thenReturn(true);
    final Session treeSession = mock(Session.class);

    final ReplayResult result =
        new ImportWAL.WALReplayer(treeSession, null, null)
            .replay(new WALInfoEntry(1, signalMemTable));

    assertEquals(ReplayResult.IGNORED, result);
    verify(treeSession, never()).insertTablet(any(Tablet.class));
    verify(treeSession, never()).insertAlignedTablet(any(Tablet.class));
  }

  /** Covers a table-model snapshot without a target database. */
  @Test
  public void testReplayTableMemTableSnapshotRequiresDatabase() throws Exception {
    final PrimitiveMemTable memTable = new PrimitiveMemTable("db", "0");
    memTable.writeAlignedRow(
        new StringArrayDeviceID("table1", "device1"),
        Collections.singletonList(new MeasurementSchema("temperature", TSDataType.FLOAT)),
        1,
        new Object[] {1.0F});
    final Session treeSession = mock(Session.class);

    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(treeSession, null, null)
                .replay(new WALInfoEntry(1, memTable)));

    verify(treeSession, never()).insertAlignedTablet(any(Tablet.class));
  }

  /** Covers table-model identifier quoting, including an embedded double quote. */
  @Test
  public void testQuoteTableIdentifierForDescribe() {
    assertEquals("\"table\"", ImportWAL.WALReplayer.quoteIdentifier("table"));
    assertEquals("\"table\"\"name\"", ImportWAL.WALReplayer.quoteIdentifier("table\"name"));
  }

  /**
   * Covers a truncated WAL that cannot yield a complete entry. The file-level replay must fail so
   * callers cannot mistake a partial replay for success.
   */
  @Test
  public void testReplayWALFileFailsOnCorruption() throws Exception {
    final File walFile = createWALFile(0);
    Files.write(walFile.toPath(), new byte[] {WALEntryType.INSERT_ROW_NODE.getCode()});

    final IOException exception =
        assertThrows(
            IOException.class,
            () ->
                ImportWAL.replayWALFiles(
                    Collections.singletonList(walFile.toPath()),
                    new ImportWAL.WALReplayer(mock(Session.class), null, null)));

    assertTrue(exception.getMessage().contains(walFile.getName()));
  }

  @Test
  public void testPasswordIsRequiredWhenInteractiveInputIsUnavailable() {
    final CommandLine commandLine = mock(CommandLine.class);
    when(commandLine.hasOption("password")).thenReturn(false);

    assertThrows(IllegalArgumentException.class, () -> ImportWAL.getPassword(commandLine, null));
  }

  @Test
  public void testExplicitPasswordTakesPrecedence() {
    final CommandLine commandLine = mock(CommandLine.class);
    when(commandLine.hasOption("password")).thenReturn(true);
    when(commandLine.getOptionValue("password")).thenReturn("secret");

    assertEquals("secret", ImportWAL.getPassword(commandLine, null));
  }

  private Path createWALFile(final Path parent, final long version) throws IOException {
    return Files.createFile(
        parent.resolve(
            WALFileUtils.getLogFileName(version, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)));
  }

  private File createWALFile(final long version) throws IOException {
    return temporaryFolder.newFile(
        WALFileUtils.getLogFileName(version, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
  }

  private static void writeWAL(final File walFile, final WALEntry... entries) throws IOException {
    try (ILogWriter writer = new WALWriter(walFile)) {
      writer.write(serializeWAL(entries));
    }
  }

  private static ByteBuffer serializeWAL(final WALEntry... entries) {
    int serializedSize = 0;
    for (final WALEntry entry : entries) {
      serializedSize += entry.serializedSize();
    }
    final WALByteBufferForTest buffer =
        new WALByteBufferForTest(ByteBuffer.allocate(serializedSize));
    for (final WALEntry entry : entries) {
      entry.serialize(buffer);
    }
    return buffer.getBuffer();
  }

  private static WALEntry mockUnsupportedEntry() {
    final WALEntry entry = mock(WALEntry.class);
    when(entry.getType()).thenReturn(WALEntryType.RELATIONAL_DELETE_DATA_NODE);
    when(entry.getValue()).thenReturn(mock(RelationalDeleteDataNode.class));
    return entry;
  }
}
