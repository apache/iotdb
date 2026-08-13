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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
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
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    new ImportWAL.WALReplayer(treeSession, null, null).replay(new WALInfoEntry(1, deleteNode));

    verify(treeSession)
        .deleteData(eq(Arrays.asList("root.sg.d1.s1", "root.sg.d2.*")), eq(10L), eq(20L));
  }

  /** Covers an unsupported entry when the interactive user explicitly chooses to skip it. */
  @Test
  public void testReplayUnsupportedEntrySkipsAfterConfirmation() throws Exception {
    final WALEntry entry = mockUnsupportedEntry();

    final boolean replayed =
        new ImportWAL.WALReplayer(mock(Session.class), null, null, ignored -> true).replay(entry);

    assertFalse(replayed);
  }

  /** Covers an unsupported entry when the interactive user declines the skip prompt. */
  @Test
  public void testReplayUnsupportedEntryFailsAfterDecliningSkip() {
    final WALEntry entry = mockUnsupportedEntry();

    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(mock(Session.class), null, null, ignored -> false)
                .replay(entry));
  }

  /** Covers non-interactive execution, which must retain the original fail-fast behavior. */
  @Test
  public void testReplayUnsupportedEntryFailsWithoutInteractiveInput() {
    final WALEntry entry = mockUnsupportedEntry();

    assertThrows(
        StatementExecutionException.class,
        () -> new ImportWAL.WALReplayer(mock(Session.class), null, null, null).replay(entry));
  }

  /** Covers accepted confirmations and the safe default for all other prompt answers. */
  @Test
  public void testUnsupportedEntrySkipConfirmationParsing() {
    assertTrue(ImportWAL.WALReplayer.isSkipConfirmation("y"));
    assertTrue(ImportWAL.WALReplayer.isSkipConfirmation(" YES "));
    assertFalse(ImportWAL.WALReplayer.isSkipConfirmation("n"));
    assertFalse(ImportWAL.WALReplayer.isSkipConfirmation(""));
    assertFalse(ImportWAL.WALReplayer.isSkipConfirmation(null));
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

  /** Covers a signal snapshot, which carries no user data and must be skipped. */
  @Test
  public void testReplaySignalMemTableSnapshotIsSkipped() throws Exception {
    final IMemTable signalMemTable = mock(IMemTable.class);
    when(signalMemTable.isSignalMemTable()).thenReturn(true);
    final Session treeSession = mock(Session.class);

    final boolean replayed =
        new ImportWAL.WALReplayer(treeSession, null, null)
            .replay(new WALInfoEntry(1, signalMemTable));

    assertFalse(replayed);
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
