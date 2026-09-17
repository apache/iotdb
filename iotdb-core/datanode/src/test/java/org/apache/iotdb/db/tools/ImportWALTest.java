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
import org.apache.iotdb.db.i18n.ImportWALMessages;
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
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;
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
import org.apache.iotdb.db.tools.ImportWAL.WALReplayer.ReplayDecisionController;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
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
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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

  /** IoTConsensus directories end in a region ID; hyphens inside database names are preserved. */
  @Test
  public void testInferDatabaseFromWALDirectory() {
    assertEquals("factory", ImportWAL.inferDatabaseFromWALDirectory(Paths.get("wal/factory-3")));
    assertEquals(
        "factory-east-2",
        ImportWAL.inferDatabaseFromWALDirectory(Paths.get("wal/factory-east-2-17")));
    assertEquals("数据库", ImportWAL.inferDatabaseFromWALDirectory(Paths.get("wal/数据库-0")));
    assertEquals("root", ImportWAL.inferDatabaseFromWALDirectory(Paths.get("wal/root-1")));
    for (final String name :
        Arrays.asList(
            "0",
            "17",
            "root.sg-3",
            "backup",
            "factory-x",
            "-3",
            "factory-3-copy",
            "a.b-2",
            "bad name-1")) {
      assertNull(name, ImportWAL.inferDatabaseFromWALDirectory(Paths.get("wal", name)));
    }
    assertNull(ImportWAL.inferDatabaseFromWALDirectory(null));
  }

  /**
   * A single file uses its parent directory; one directory is confirmed only once for many files.
   */
  @Test
  public void testConfirmInferredDatabasePerDirectory() throws Exception {
    final Path directory = temporaryFolder.newFolder("factory-east-3").toPath();
    final Path first = createWALFile(directory, 0);
    createWALFile(directory, 1);
    final AtomicInteger confirmations = new AtomicInteger();
    final Map<Path, String> databases =
        ImportWAL.resolveDirectoryDatabases(
            ImportWAL.collectWALFiles(directory),
            null,
            false,
            (source, database) -> {
              assertEquals(directory, source);
              assertEquals("factory-east", database);
              confirmations.incrementAndGet();
              return " YeS ";
            });
    assertEquals(1, confirmations.get());
    assertEquals("factory-east", databases.get(directory));
    assertEquals(
        databases,
        ImportWAL.resolveDirectoryDatabases(
            ImportWAL.collectWALFiles(first), null, false, (source, database) -> "y"));
  }

  /** Explicit -db overrides every directory and never asks for inference approval. */
  @Test
  public void testExplicitDatabaseOverridesDirectories() {
    final List<Path> files =
        Arrays.asList(
            Paths.get("factory-3/one.wal"), Paths.get("other-4/two.wal"), Paths.get("0/three.wal"));
    final Map<Path, String> databases =
        ImportWAL.resolveDirectoryDatabases(
            files,
            "target",
            false,
            (source, database) -> {
              throw new AssertionError("Explicit database must not prompt");
            });
    assertEquals(3, databases.size());
    assertTrue(databases.values().stream().allMatch("target"::equals));
    assertEquals(databases, ImportWAL.resolveDirectoryDatabases(files, "target", false, null));
  }

  /** Rejecting a candidate, EOF, empty/invalid input, or no console must not authorize writes. */
  @Test
  public void testInferredDatabaseRequiresPositiveConfirmation() {
    final List<Path> files = Collections.singletonList(Paths.get("factory-3/one.wal"));
    for (final String answer : Arrays.asList("n", "no", "", "invalid", "execute", null)) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ImportWAL.resolveDirectoryDatabases(
                  files, null, false, (source, database) -> answer));
    }
    final IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> ImportWAL.resolveDirectoryDatabases(files, null, false, null));
    assertTrue(failure.getMessage().contains("factory-3"));
    assertTrue(failure.getMessage().contains("-db/--database"));
    assertTrue(failure.getMessage().contains("--skip_db_confirmation"));
  }

  /** The flag authorizes inferred targets without a console, but never invents missing names. */
  @Test
  public void testSkipDatabaseConfirmationOption() throws Exception {
    final CommandLine commandLine =
        new DefaultParser()
            .parse(ImportWAL.createOptions(), new String[] {"-f", "wal", "--skip_db_confirmation"});
    assertTrue(commandLine.hasOption("skip_db_confirmation"));
    assertFalse(ImportWAL.createOptions().getOption("skip_db_confirmation").hasArg());
    final List<Path> files =
        Arrays.asList(
            Paths.get("factory-east-3/one.wal"),
            Paths.get("factory-west-4/two.wal"),
            Paths.get("0/three.wal"));
    final Map<Path, String> databases =
        ImportWAL.resolveDirectoryDatabases(
            files, null, commandLine.hasOption("skip_db_confirmation"), null);
    assertEquals("factory-east", databases.get(files.get(0).getParent()));
    assertEquals("factory-west", databases.get(files.get(1).getParent()));
    assertTrue(databases.containsKey(files.get(2).getParent()));
    assertNull(databases.get(files.get(2).getParent()));
    assertEquals(
        databases,
        ImportWAL.resolveDirectoryDatabases(
            files,
            null,
            true,
            (directory, database) -> {
              throw new AssertionError("Skipped confirmation must not prompt");
            }));
    assertTrue(
        ImportWAL.resolveDirectoryDatabases(files, "target", true, null).values().stream()
            .allMatch("target"::equals));
  }

  /** Accept-all applies to subsequent directories, without reusing the first database name. */
  @Test
  public void testAcceptAllInferredDatabases() {
    final List<Path> files =
        Arrays.asList(
            Paths.get("first-1/one.wal"),
            Paths.get("second-2/two.wal"),
            Paths.get("third-3/three.wal"),
            Paths.get("unknown/four.wal"));
    for (final String answer : Arrays.asList("a", " A ", "all", " AlL ")) {
      final AtomicInteger prompts = new AtomicInteger();
      final Map<Path, String> databases =
          ImportWAL.resolveDirectoryDatabases(
              files,
              null,
              false,
              (directory, database) -> prompts.incrementAndGet() == 1 ? "y" : answer);
      assertEquals(2, prompts.get());
      assertEquals("first", databases.get(files.get(0).getParent()));
      assertEquals("second", databases.get(files.get(1).getParent()));
      assertEquals("third", databases.get(files.get(2).getParent()));
      assertNull(databases.get(files.get(3).getParent()));
    }
    // A later import must ask again even if an earlier import accepted all inferred targets.
    assertThrows(
        IllegalArgumentException.class,
        () -> ImportWAL.resolveDirectoryDatabases(files, null, false, null));
  }

  /** Tree and shared WAL node directories do not imply a table database or require confirmation. */
  @Test
  public void testUnrecognizedDirectoryDoesNotInferFromAncestor() {
    final List<Path> files =
        Arrays.asList(
            Paths.get("factory-3/0/one.wal"),
            Paths.get("root.sg-4/two.wal"),
            Paths.get("backup/three.wal"));
    final Map<Path, String> databases =
        ImportWAL.resolveDirectoryDatabases(files, null, false, null);
    assertEquals(3, databases.size());
    assertTrue(databases.values().stream().allMatch(database -> database == null));
  }

  /**
   * One thread may visit several databases; each directory needs its own Session and schema cache.
   */
  @Test
  public void testInferredDatabasesRouteDirectoriesSequentially() throws Exception {
    assertInferredDatabasesRouteDirectories(1);
  }

  /** Concurrent directory replay must use the confirmed database for each directory. */
  @Test
  public void testInferredDatabasesRouteDirectoriesInParallel() throws Exception {
    assertInferredDatabasesRouteDirectories(2);
  }

  private void assertInferredDatabasesRouteDirectories(final int threads) throws Exception {
    final Path source = temporaryFolder.newFolder("database-routing").toPath();
    final Map<String, Session> tables = new LinkedHashMap<>();
    final List<Path> directories = new ArrayList<>();
    for (final String database : Arrays.asList("factory-east", "factory-west")) {
      final Path directory = Files.createDirectory(source.resolve(database + "-3"));
      directories.add(directory);
      final Session session = mock(Session.class);
      mockTableSchema(session, "table1", database + "_time", "tag1");
      tables.put(database, session);
      writeWAL(
          createWALFile(directory, 0).toFile(),
          new WALInfoEntry(1, WALFileTest.getRelationalInsertTabletNode("table1")),
          tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20)));
      writeWAL(
          createWALFile(directory, 1).toFile(),
          tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 30, 40)));
    }
    final Path treeDirectory = Files.createDirectory(source.resolve("root.sg-4"));
    writeWAL(
        createWALFile(treeDirectory, 0).toFile(),
        new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1)));
    final List<Path> files = ImportWAL.collectWALFiles(source);
    final AtomicInteger confirmations = new AtomicInteger();
    final Map<Path, String> databases =
        ImportWAL.resolveDirectoryDatabases(
            files,
            null,
            false,
            (directory, database) -> {
              assertTrue(directories.contains(directory));
              assertTrue(tables.containsKey(database));
              confirmations.incrementAndGet();
              return "yes";
            });
    final Session tree = mock(Session.class);
    final AtomicInteger closed = new AtomicInteger();
    final ReplayDecisionController controller = policyController("--on_delete", "execute");
    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALDirectories(
            files,
            threads,
            directory -> {
              assertEquals(2, confirmations.get());
              final String database = databases.get(directory);
              return new ImportWAL.WALReplayer(tree, tables.get(database), database, controller) {
                @Override
                public void close() {
                  closed.incrementAndGet();
                }
              };
            },
            null,
            false);
    assertEquals(7, statistics.getReplayedOperationCount());
    assertEquals(3, closed.get());
    for (final Map.Entry<String, Session> table : tables.entrySet()) {
      verify(table.getValue()).insertRelationalTablet(any(Tablet.class));
      verify(table.getValue()).executeQueryStatement("DESCRIBE \"table1\"");
      final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
      verify(table.getValue(), times(2)).executeNonQueryStatement(sql.capture());
      assertTrue(
          sql.getAllValues().stream()
              .allMatch(statement -> statement.contains(table.getKey() + "_time")));
    }
    verify(tree).insertTablet(any(Tablet.class));
    verify(tree, never()).insertRelationalTablet(any(Tablet.class));
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
    assertTrue(output.toString().contains("--on_delete"));
    assertTrue(output.toString().contains("--on_object"));
    assertTrue(output.toString().contains("--on_unsupported"));
    assertTrue(output.toString().contains("--on_corrupted"));
    assertTrue(output.toString().contains("--skip_db_confirmation"));
  }

  @Test
  public void testReplayPolicyDefaultsRequireInteraction() throws Exception {
    final WALEntry deletion = tableDeleteEntry();
    for (final ReplayDecisionController controller :
        Arrays.asList(
            policyController(),
            policyController(
                "--on_delete",
                "ask",
                "--on_object",
                "ask",
                "--on_unsupported",
                "ask",
                "--on_corrupted",
                "ask"))) {
      assertEquals(ReplayDecision.TERMINATE, controller.decide(deletion, true));
      assertEquals(ReplayDecision.TERMINATE, controller.decide(mockUnsupportedEntry(), false));
      assertEquals(ReplayDecision.TERMINATE, controller.decide(deletion, false));
      assertFalse(controller.skipCorruptedFile("broken.wal"));
    }
  }

  /** Invalid policies must fail before reading WAL files or connecting to the target. */
  @Test
  public void testInvalidReplayPoliciesFailEarly() {
    for (final String option :
        Arrays.asList("on_delete", "on_object", "on_unsupported", "on_corrupted")) {
      final List<String> invalidValues = new ArrayList<>(Arrays.asList("", "invalid"));
      if (!"on_delete".equals(option)) {
        invalidValues.add("execute");
      }
      for (final String value : invalidValues) {
        final ByteArrayOutputStream error = new ByteArrayOutputStream();
        assertEquals(
            1,
            ImportWAL.run(
                new String[] {"-f", "unused.wal", "--" + option, value},
                new PrintStream(error),
                new PrintStream(error)));
        assertTrue(error.toString().contains("--" + option));
        assertTrue(error.toString().contains("ask"));
      }
      final ByteArrayOutputStream error = new ByteArrayOutputStream();
      assertEquals(
          1,
          ImportWAL.run(
              new String[] {"-f", "unused.wal", "--" + option},
              new PrintStream(error),
              new PrintStream(error)));
    }
  }

  /** One explicit delete policy applies to both data models and all workers. */
  @Test
  public void testConfiguredDeletePoliciesAcrossWorkers() throws Exception {
    final WALEntry treeDelete =
        new WALInfoEntry(
            1,
            new DeleteDataNode(
                new PlanNodeId(""),
                Collections.singletonList(new MeasurementPath("root.sg.d1.s1")),
                10,
                20));
    final WALEntry tableDelete =
        tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20));
    for (final String policy : Arrays.asList(" ExEcUtE ", "skip", "terminate")) {
      final Session tree = mock(Session.class);
      final Session table = mock(Session.class);
      final ReplayDecisionController controller = policyController("--on_delete", policy);
      if (policy.trim().equalsIgnoreCase("execute")) {
        mockTableSchema(table, "table1", "time", "tag1");
      }
      for (int worker = 0; worker < 2; worker++) {
        final ImportWAL.WALReplayer replayer =
            new ImportWAL.WALReplayer(tree, table, "target_db", controller);
        for (final WALEntry entry : Arrays.asList(treeDelete, tableDelete)) {
          if ("terminate".equals(policy)) {
            assertThrows(StatementExecutionException.class, () -> replayer.replay(entry));
          } else {
            assertEquals(
                "skip".equals(policy) ? ReplayResult.SKIPPED : ReplayResult.REPLAYED,
                replayer.replay(entry));
          }
        }
      }
      if (policy.trim().equalsIgnoreCase("execute")) {
        verify(tree, times(2)).deleteData(Collections.singletonList("root.sg.d1.s1"), 10, 20);
        verify(table, times(2)).executeNonQueryStatement(any());
      } else {
        verifyZeroInteractions(tree, table);
      }
    }
  }

  /** ObjectNode and conversion failures have independent policies, without interactive input. */
  @Test
  public void testConfiguredUnsupportedPoliciesAreIndependent() throws Exception {
    final WALEntry unconvertibleDelete =
        tableDeleteEntry(
            tableDeletion("table1", new TagPredicate.SegmentExactMatch("a", 2), 10, 20));
    for (final String objectPolicy : Arrays.asList("skip", "terminate")) {
      for (final String unsupportedPolicy : Arrays.asList("skip", "terminate")) {
        final Session tree = mock(Session.class);
        final Session table = mock(Session.class);
        mockTableSchema(table, "table1", "time", "tag1");
        final ImportWAL.WALReplayer replayer =
            new ImportWAL.WALReplayer(
                tree,
                table,
                "target_db",
                policyController(
                    "--on_delete", "execute",
                    "--on_object", objectPolicy,
                    "--on_unsupported", unsupportedPolicy));
        for (int entry = 0; entry < 2; entry++) {
          if ("skip".equals(objectPolicy)) {
            assertEquals(ReplayResult.SKIPPED, replayer.replay(mockUnsupportedEntry()));
          } else {
            assertThrows(
                StatementExecutionException.class, () -> replayer.replay(mockUnsupportedEntry()));
          }
          if ("skip".equals(unsupportedPolicy)) {
            assertEquals(ReplayResult.SKIPPED, replayer.replay(unconvertibleDelete));
          } else {
            assertThrows(
                StatementExecutionException.class, () -> replayer.replay(unconvertibleDelete));
          }
        }
        verify(table, never()).executeNonQueryStatement(any());
        verifyZeroInteractions(tree);
      }
    }
  }

  /** Skipping through CLI policy must still protect the WAL from --on_success delete. */
  @Test
  public void testConfiguredSkipRetainsSourceWAL() throws Exception {
    final File walFile = createWALFile(0);
    writeWAL(walFile, tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20)));
    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALFiles(
            Collections.singletonList(walFile.toPath()),
            new ImportWAL.WALReplayer(
                mock(Session.class), null, null, policyController("--on_delete", "skip")),
            null,
            true);
    assertEquals(1, statistics.getSkippedEntryCount());
    assertEquals(0, statistics.getReplayedOperationCount());
    assertTrue(walFile.exists());
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

  /** Unsupported ObjectNode entries skipped with SKIP_ALL must retain their source WALs. */
  @Test
  public void testReplayUnsupportedEntriesSkipAllAfterConfirmation() throws Exception {
    final AtomicInteger promptCount = new AtomicInteger();
    final ImportWAL.WALReplayer.ReplayDecisionPrompt skipAllPrompt =
        (entry, treeDelete) -> {
          promptCount.incrementAndGet();
          return ImportWAL.WALReplayer.ReplayDecision.SKIP_ALL;
        };
    final ImportWAL.WALReplayer replayer =
        new ImportWAL.WALReplayer(mock(Session.class), null, null, skipAllPrompt);

    assertEquals(ReplayResult.SKIPPED, replayer.replay(mockUnsupportedEntry()));
    assertEquals(ReplayResult.SKIPPED, replayer.replay(mockUnsupportedEntry()));
    assertEquals(2, promptCount.get());
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
                    new ImportWAL.WALReplayer.ReplayDecisionController((Console) null))
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

  /**
   * A real table-delete WAL must execute one SQL per table and become deletable only on success.
   */
  @Test
  public void testReplayTableDeleteAsSql() throws Exception {
    final File walFile = createWALFile(0);
    writeWAL(
        walFile,
        tableDeleteEntry(
            tableDeletion("table1", new TagPredicate.SegmentExactMatch("a", 1), 10, 20),
            tableDeletion("table1", new TagPredicate.SegmentExactMatch(null, 2), 30, 40)));
    final Session tree = mock(Session.class);
    final Session table = mock(Session.class);
    final SessionDataSet schema = mockTableSchema(table, "table1", "ts", "tag1", "tag2");
    final ImportWAL.ReplayStatistics statistics =
        ImportWAL.replayWALFiles(
            Collections.singletonList(walFile.toPath()),
            new ImportWAL.WALReplayer(
                tree,
                table,
                "target_db",
                (entry, executable) -> {
                  assertTrue(executable);
                  return ReplayDecision.EXECUTE;
                }),
            null,
            true);

    verify(table)
        .executeNonQueryStatement(
            "DELETE FROM \"table1\" WHERE (((\"tag1\" = 'a') AND (\"ts\" >= 10) AND (\"ts\" <= 20)) OR ((\"tag2\" IS NULL) AND (\"ts\" >= 30) AND (\"ts\" <= 40)))");
    verify(schema).close();
    verifyZeroInteractions(tree);
    assertEquals(1, statistics.getReplayedOperationCount());
    assertEquals(0, statistics.getSkippedEntryCount());
    assertFalse(walFile.exists());
  }

  /**
   * A node spanning tables emits one DELETE per table and reuses DESCRIBE metadata on later nodes.
   */
  @Test
  public void testReplayTableDeleteGroupsTablesAndCachesSchema() throws Exception {
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    mockTableSchema(table, "table2", "time", "tag1");
    final ImportWAL.WALReplayer replayer =
        new ImportWAL.WALReplayer(
            mock(Session.class), table, "target_db", (entry, executable) -> ReplayDecision.EXECUTE);
    final WALEntry entry =
        tableDeleteEntry(
            tableDeletion("table1", new TagPredicate.NOP(), 10, 20),
            tableDeletion("table2", new TagPredicate.NOP(), 10, 20));
    assertEquals(ReplayResult.REPLAYED, replayer.replay(entry));
    assertEquals(ReplayResult.REPLAYED, replayer.replay(entry));
    verify(table, times(2))
        .executeNonQueryStatement(
            "DELETE FROM \"table1\" WHERE ((\"time\" >= 10) AND (\"time\" <= 20))");
    verify(table, times(2))
        .executeNonQueryStatement(
            "DELETE FROM \"table2\" WHERE ((\"time\" >= 10) AND (\"time\" <= 20))");
    verify(table).executeQueryStatement("DESCRIBE \"table1\"");
    verify(table).executeQueryStatement("DESCRIBE \"table2\"");
  }

  /** SKIP/SKIP_ALL and quit must avoid all table RPCs, even if no target database was supplied. */
  @Test
  public void testReplayTableDeleteDecisionsBeforeSchemaLookup() throws Exception {
    final WALEntry entry =
        tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20));
    final Session table = mock(Session.class);
    for (final ReplayDecision decision :
        Arrays.asList(ReplayDecision.SKIP, ReplayDecision.SKIP_ALL)) {
      assertEquals(
          ReplayResult.SKIPPED,
          new ImportWAL.WALReplayer(
                  mock(Session.class), table, "target_db", (ignored, executable) -> decision)
              .replay(entry));
      assertEquals(
          ReplayResult.SKIPPED,
          new ImportWAL.WALReplayer(
                  mock(Session.class), null, null, (ignored, executable) -> decision)
              .replay(entry));
    }
    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    table,
                    "target_db",
                    (ignored, executable) -> ReplayDecision.TERMINATE)
                .replay(entry));
    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    table,
                    "target_db",
                    new ReplayDecisionController((Console) null))
                .replay(entry));
    assertThrows(
        StatementExecutionException.class,
        () ->
            new ImportWAL.WALReplayer(
                    mock(Session.class),
                    null,
                    null,
                    (ignored, executable) -> ReplayDecision.EXECUTE)
                .replay(entry));
    verifyZeroInteractions(table);
  }

  /**
   * Empty device sets represent no pending deletion and must never send an unconditional DELETE.
   */
  @Test
  public void testReplayEmptyTableDelete() throws Exception {
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    final ImportWAL.WALReplayer replayer =
        new ImportWAL.WALReplayer(
            mock(Session.class), table, "target_db", (entry, executable) -> ReplayDecision.EXECUTE);
    assertEquals(
        ReplayResult.IGNORED,
        replayer.replay(
            tableDeleteEntry(
                tableDeletion(
                    "table1", new TagPredicate.DeviceIn(Collections.emptySet()), 10, 20))));
    assertEquals(ReplayResult.IGNORED, replayer.replay(tableDeleteEntry()));
    verify(table, never()).executeNonQueryStatement(any());
  }

  /**
   * Quitting the conversion fallback must retain the entire WAL without executing earlier tables.
   */
  @Test
  public void testTableDeletePrevalidationRetainsSource() throws Exception {
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    mockTableSchema(table, "table2", "time", "tag1");
    final TableDeletionEntry columnDelete =
        new TableDeletionEntry(
            new DeletionPredicate(
                "table2", new TagPredicate.NOP(), Collections.singletonList("s1")),
            new TimeRange(10, 20));
    final List<TableDeletionEntry> invalidEntries =
        Arrays.asList(
            columnDelete,
            tableDeletion("table2", new TagPredicate.SegmentExactMatch("a", 2), 10, 20));
    for (int i = 0; i < invalidEntries.size(); i++) {
      final File walFile = createWALFile(i);
      writeWAL(
          walFile,
          tableDeleteEntry(
              tableDeletion("table1", new TagPredicate.NOP(), 10, 20), invalidEntries.get(i)));
      final byte[] original = Files.readAllBytes(walFile.toPath());
      final AtomicInteger prompts = new AtomicInteger();
      final ImportWAL.WALReplayer replayer =
          new ImportWAL.WALReplayer(
              mock(Session.class),
              table,
              "target_db",
              new ReplayDecisionController(
                  (prompt, argument) -> {
                    prompts.incrementAndGet();
                    return argument instanceof WALEntryType ? "a" : "q";
                  }));
      assertThrows(
          IOException.class,
          () ->
              ImportWAL.replayWALFiles(
                  Collections.singletonList(walFile.toPath()), replayer, null, true));
      assertArrayEquals(original, Files.readAllBytes(walFile.toPath()));
      assertEquals(2, prompts.get());
    }
    verify(table, never()).executeNonQueryStatement(any());
  }

  /** Skipping an unconvertible node retains its WAL and lets serial replay finish later entries. */
  @Test
  public void testSkipUnconvertibleTableDeleteRetainsSource() throws Exception {
    assertSkipUnconvertibleTableDeletes("s", false);
  }

  /** Skip-all for conversion failures must not skip subsequent convertible table deletes. */
  @Test
  public void testSkipAllUnconvertibleTableDeletesRetainsSources() throws Exception {
    assertSkipUnconvertibleTableDeletes("l", false);
  }

  /** Parallel workers must preserve skipped nodes while deleting only completely replayed WALs. */
  @Test
  public void testParallelSkipUnconvertibleTableDeleteRetainsSource() throws Exception {
    assertSkipUnconvertibleTableDeletes("s", true);
  }

  /** Parallel workers share one unsupported skip-all choice independently of table execute-all. */
  @Test
  public void testParallelSkipAllUnconvertibleTableDeletesRetainsSources() throws Exception {
    assertSkipUnconvertibleTableDeletes("l", true);
  }

  private void assertSkipUnconvertibleTableDeletes(final String answer, final boolean parallel)
      throws Exception {
    final Path root = temporaryFolder.newFolder("conversion-wals").toPath();
    final Path nodeA = Files.createDirectory(root.resolve("node-a"));
    final Path nodeB = Files.createDirectory(root.resolve("node-b"));
    final Path columnWAL = createWALFile(nodeA, 0);
    final Path incompatibleWAL = createWALFile(nodeB, 0);
    final Path completeWAL = createWALFile(nodeA, 1);
    final TableDeletionEntry valid = tableDeletion("table1", new TagPredicate.NOP(), 10, 20);
    final TableDeletionEntry columnDelete =
        new TableDeletionEntry(
            new DeletionPredicate(
                "table2", new TagPredicate.NOP(), Collections.singletonList("s1")),
            new TimeRange(10, 20));
    final WALEntry insert = new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1));
    // Even the convertible part of a mixed node must not execute when the user skips the node.
    writeWAL(columnWAL.toFile(), tableDeleteEntry(valid, columnDelete), insert);
    writeWAL(
        incompatibleWAL.toFile(),
        tableDeleteEntry(
            valid, tableDeletion("table2", new TagPredicate.SegmentExactMatch("a", 2), 10, 20)),
        insert);
    writeWAL(completeWAL.toFile(), tableDeleteEntry(valid));
    final byte[] columnBytes = Files.readAllBytes(columnWAL);
    final byte[] incompatibleBytes = Files.readAllBytes(incompatibleWAL);
    final Session tree = mock(Session.class);
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    mockTableSchema(table, "table2", "time", "tag1");
    final AtomicInteger executePrompts = new AtomicInteger();
    final List<String> skipReasons = new CopyOnWriteArrayList<>();
    final ReplayDecisionController controller =
        new ReplayDecisionController(
            (prompt, argument) -> {
              if (argument instanceof WALEntryType) {
                executePrompts.incrementAndGet();
                return "a";
              }
              assertEquals(
                  ImportWALMessages
                      .MESSAGE_UNSUPPORTED_WAL_OPERATION_ARG_CHOOSE_S_SKIP_L_SKIP_ALL_Q_QUIT_0A734E52,
                  prompt);
              skipReasons.add((String) argument);
              return answer;
            });
    final List<Path> files = ImportWAL.collectWALFiles(root);
    final ImportWAL.ReplayStatistics statistics =
        parallel
            ? ImportWAL.replayWALDirectories(
                files,
                2,
                () -> new ImportWAL.WALReplayer(tree, table, "target_db", controller),
                null,
                true)
            : ImportWAL.replayWALFiles(
                files, new ImportWAL.WALReplayer(tree, table, "target_db", controller), null, true);
    assertArrayEquals(columnBytes, Files.readAllBytes(columnWAL));
    assertArrayEquals(incompatibleBytes, Files.readAllBytes(incompatibleWAL));
    assertFalse(Files.exists(completeWAL));
    assertEquals(3, statistics.getReplayedOperationCount());
    assertEquals(2, statistics.getSkippedEntryCount());
    assertEquals(3, statistics.getCompletedFileCount());
    assertEquals(1, executePrompts.get());
    assertEquals("l".equals(answer) ? 1 : 2, skipReasons.size());
    final List<String> expectedReasons =
        Arrays.asList(
            String.format(
                ImportWALMessages
                    .EXCEPTION_CANNOT_REPLAY_COLUMN_SPECIFIC_DELETION_FOR_TABLE_ARG_AS_DELETE_FROM_4A7ACC93,
                "table2"),
            String.format(
                ImportWALMessages
                    .EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_TAG_SEGMENT_INDEX_ARG_IS_INCOMPATIBLE_WITH_TARGET_TABLE_ARG_D5E3CCEE,
                2,
                "table2"));
    assertTrue(expectedReasons.containsAll(skipReasons));
    verify(table).executeNonQueryStatement(any());
    verify(tree, times(2)).insertTablet(any(Tablet.class));
  }

  /** Missing interaction and attempts to execute an unconvertible delete must still terminate. */
  @Test
  public void testUnconvertibleTableDeleteRequiresSkipChoice() throws Exception {
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    final WALEntry entry =
        tableDeleteEntry(
            tableDeletion("table1", new TagPredicate.SegmentExactMatch("a", 2), 10, 20));
    for (final String answer : Arrays.asList(null, "e", "a")) {
      final AtomicInteger prompts = new AtomicInteger();
      final ReplayDecisionController controller =
          new ReplayDecisionController(
              (prompt, argument) -> {
                prompts.incrementAndGet();
                return argument instanceof WALEntryType ? "a" : answer;
              });
      assertThrows(
          StatementExecutionException.class,
          () ->
              new ImportWAL.WALReplayer(mock(Session.class), table, "target_db", controller)
                  .replay(entry));
      assertEquals(2, prompts.get());
    }
    verify(table, never()).executeNonQueryStatement(any());
  }

  /** DESCRIBE RPC errors must abort import rather than being offered as skippable conversions. */
  @Test
  public void testTableDeleteSchemaFailureDoesNotOfferSkip() throws Exception {
    for (final Exception failure :
        Arrays.asList(
            new StatementExecutionException("describe failed"),
            new IoTDBConnectionException("disconnected"))) {
      final Session table = mock(Session.class);
      when(table.executeQueryStatement(any())).thenThrow(failure);
      final AtomicInteger prompts = new AtomicInteger();
      final ImportWAL.WALReplayer replayer =
          new ImportWAL.WALReplayer(
              mock(Session.class),
              table,
              "target_db",
              (entry, executable) -> {
                assertTrue(executable);
                prompts.incrementAndGet();
                return ReplayDecision.EXECUTE;
              });
      assertEquals(
          failure,
          assertThrows(
              Exception.class,
              () ->
                  replayer.replay(
                      tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20)))));
      assertEquals(1, prompts.get());
      verify(table, never()).executeNonQueryStatement(any());
    }
  }

  /** DELETE RPC failures cannot be skipped and must retain even previously completed WAL files. */
  @Test
  public void testTableDeleteExecutionFailureRetainsAllSources() throws Exception {
    final File insertFile = createWALFile(0);
    final File deleteFile = createWALFile(1);
    writeWAL(insertFile, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1)));
    writeWAL(deleteFile, tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20)));
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    doThrow(new StatementExecutionException("delete failed"))
        .when(table)
        .executeNonQueryStatement(any());
    final ImportWAL.WALReplayer replayer =
        new ImportWAL.WALReplayer(
            mock(Session.class),
            table,
            "target_db",
            (entry, executable) -> {
              assertTrue(executable);
              return ReplayDecision.EXECUTE;
            });
    assertThrows(
        IOException.class,
        () ->
            ImportWAL.replayWALFiles(
                Arrays.asList(insertFile.toPath(), deleteFile.toPath()), replayer, null, true));
    assertTrue(insertFile.exists());
    assertTrue(deleteFile.exists());
  }

  /**
   * Shared workers remember table execute-all independently of tree skip-all and unsupported skips.
   */
  @Test
  public void testDeleteDecisionControllerSeparatesModelsAcrossWorkers() throws Exception {
    final AtomicInteger prompts = new AtomicInteger();
    final ReplayDecisionController controller =
        new ReplayDecisionController(
            (prompt, type) -> {
              prompts.incrementAndGet();
              return type == WALEntryType.RELATIONAL_DELETE_DATA_NODE ? "a" : "l";
            });
    final Session tree = mock(Session.class);
    final Session table = mock(Session.class);
    mockTableSchema(table, "table1", "time", "tag1");
    final ImportWAL.WALReplayer first =
        new ImportWAL.WALReplayer(tree, table, "target_db", controller);
    final ImportWAL.WALReplayer second =
        new ImportWAL.WALReplayer(tree, table, "target_db", controller);
    final WALEntry treeDelete =
        new WALInfoEntry(
            1,
            new DeleteDataNode(
                new PlanNodeId(""),
                Collections.singletonList(new MeasurementPath("root.sg.d1.s1")),
                10,
                20));
    final WALEntry tableDelete =
        tableDeleteEntry(tableDeletion("table1", new TagPredicate.NOP(), 10, 20));
    assertEquals(ReplayResult.SKIPPED, first.replay(treeDelete));
    assertEquals(ReplayResult.SKIPPED, first.replay(mockUnsupportedEntry()));
    assertEquals(ReplayResult.REPLAYED, first.replay(tableDelete));
    assertEquals(ReplayResult.SKIPPED, second.replay(treeDelete));
    assertEquals(ReplayResult.SKIPPED, second.replay(mockUnsupportedEntry()));
    assertEquals(ReplayResult.REPLAYED, second.replay(tableDelete));
    assertEquals(3, prompts.get());
    verify(table, times(2)).executeNonQueryStatement(any());
    verifyZeroInteractions(tree);
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
  public void testConfiguredSkipCorruptedFiles() throws Exception {
    assertReplaySkipsCorruptedFiles(policyController("--on_corrupted", " SkIp "), false);
  }

  @Test
  public void testParallelConfiguredSkipCorruptedFiles() throws Exception {
    assertReplaySkipsCorruptedFiles(policyController("--on_corrupted", "skip"), true);
  }

  @Test
  public void testInteractivelySkipEachCorruptedFile() throws Exception {
    final List<String> reasons = new ArrayList<>();
    assertReplaySkipsCorruptedFiles(
        new ReplayDecisionController(
            (prompt, reason) -> {
              assertEquals(
                  ImportWALMessages
                      .MESSAGE_WAL_CORRUPTION_DETECTED_ARG_ALREADY_REPLAYED_OPERATIONS_ARE_NOT_ROLLED_BACK_CHOOSE_S_SKIP_FILE_L_SKIP_ALL_CORRUPTED_FILES_Q_QUIT_BFED14E4,
                  prompt);
              reasons.add(reason.toString());
              return "s";
            }),
        false);
    assertEquals(2, reasons.size());
    assertTrue(reasons.get(0).contains("node-a"));
    assertTrue(reasons.get(1).contains("node-b"));
  }

  @Test
  public void testParallelInteractivelySkipAllCorruptedFiles() throws Exception {
    final AtomicInteger prompts = new AtomicInteger();
    final ReplayDecisionController controller =
        new ReplayDecisionController(
            (prompt, reason) -> {
              prompts.incrementAndGet();
              return "l";
            });
    assertReplaySkipsCorruptedFiles(controller, true);
    assertEquals(1, prompts.get());
    controller.decide(mockUnsupportedEntry(), false);
    assertEquals(2, prompts.get());
  }

  /**
   * Complete entries before corruption stay counted, and later files in each directory continue.
   */
  private void assertReplaySkipsCorruptedFiles(
      final ReplayDecisionController controller, final boolean parallel) throws Exception {
    final Path source = temporaryFolder.newFolder("corrupted-wal-root").toPath();
    final List<Path> corrupted = new ArrayList<>();
    final List<Path> complete = new ArrayList<>();
    for (final String node : Arrays.asList("node-a", "node-b")) {
      final Path directory = Files.createDirectory(source.resolve(node));
      final Path brokenWAL = createWALFile(directory, 0);
      // Keep one valid entry before a truncated entry in a properly framed WAL segment.
      try (ILogWriter writer = new WALWriter(brokenWAL.toFile())) {
        writer.write(
            serializeWAL(new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1))));
        final ByteBuffer partialEntry = ByteBuffer.allocate(1);
        partialEntry.put(WALEntryType.INSERT_ROW_NODE.getCode());
        writer.write(partialEntry);
      }
      corrupted.add(brokenWAL);
      final Path validWAL = createWALFile(directory, 1);
      writeWAL(
          validWAL.toFile(), new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 2)));
      complete.add(validWAL);
    }
    final Session session = mock(Session.class);
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final List<Path> files = ImportWAL.collectWALFiles(source);
    final ImportWAL.ReplayStatistics statistics =
        parallel
            ? ImportWAL.replayWALDirectories(
                files,
                2,
                () -> new ImportWAL.WALReplayer(session, null, null, controller),
                new PrintStream(output),
                true)
            : ImportWAL.replayWALFiles(
                files,
                new ImportWAL.WALReplayer(session, null, null, controller),
                new PrintStream(output),
                true);
    assertEquals(4, statistics.getReplayedOperationCount());
    assertEquals(0, statistics.getSkippedEntryCount());
    assertEquals(2, statistics.getSkippedCorruptedFileCount());
    assertEquals(4, statistics.getCompletedFileCount());
    assertEquals(100.0, statistics.getProgressPercent(), 0.0);
    for (final Path file : corrupted) {
      assertTrue(Files.exists(file));
      assertTrue(output.toString().contains(file.toString()));
    }
    for (final Path file : complete) {
      assertFalse(Files.exists(file));
    }
    verify(session, times(4)).insertTablet(any(Tablet.class));
  }

  @Test
  public void testCorruptionTerminationRetainsAllSourceFiles() throws Exception {
    final File valid = createWALFile(0);
    final File corrupted = createWALFile(1);
    final File later = createWALFile(2);
    writeWAL(valid, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1)));
    Files.write(corrupted.toPath(), new byte[] {WALEntryType.INSERT_ROW_NODE.getCode()});
    writeWAL(later, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 2)));
    final List<ReplayDecisionController> controllers =
        new ArrayList<>(
            Arrays.asList(policyController("--on_corrupted", "terminate"), policyController()));
    for (final String answer : Arrays.asList("q", "invalid", "e", null)) {
      controllers.add(new ReplayDecisionController((prompt, reason) -> answer));
    }
    for (final ReplayDecisionController controller : controllers) {
      final Session session = mock(Session.class);
      final IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  ImportWAL.replayWALFiles(
                      Arrays.asList(valid.toPath(), corrupted.toPath(), later.toPath()),
                      new ImportWAL.WALReplayer(session, null, null, controller),
                      null,
                      true));
      assertTrue(failure.getMessage().contains(corrupted.getName()));
      assertTrue(valid.exists());
      assertTrue(corrupted.exists());
      assertTrue(later.exists());
      verify(session).insertTablet(any(Tablet.class));
    }
  }

  @Test
  public void testUnsupportedSkipAllDoesNotSkipCorruption() {
    final AtomicInteger prompts = new AtomicInteger();
    final ReplayDecisionController controller =
        new ReplayDecisionController(
            (prompt, reason) -> prompts.incrementAndGet() == 1 ? "l" : "q");
    assertEquals(ReplayDecision.SKIP_ALL, controller.decide(mockUnsupportedEntry(), false));
    assertFalse(controller.skipCorruptedFile("broken.wal"));
    assertEquals(2, prompts.get());
  }

  /** A failed RPC may already have applied data; corruption policy must not hide it. */
  @Test
  public void testSkipCorruptionDoesNotSkipReplayFailures() throws Exception {
    final File walFile = createWALFile(0);
    writeWAL(walFile, new WALInfoEntry(1, WALTestUtils.getInsertRowNode("root.sg.d1", 1)));
    for (final Exception cause :
        Arrays.asList(
            new IoTDBConnectionException("disconnected"),
            new StatementExecutionException("insert failed"))) {
      final Session session = mock(Session.class);
      doThrow(cause).when(session).insertTablet(any(Tablet.class));
      final IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  ImportWAL.replayWALFiles(
                      Collections.singletonList(walFile.toPath()),
                      new ImportWAL.WALReplayer(
                          session, null, null, policyController("--on_corrupted", "skip")),
                      null,
                      true));
      assertEquals(cause, failure.getCause());
      assertTrue(walFile.exists());
    }
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

  private static ReplayDecisionController policyController(final String... options)
      throws Exception {
    final List<String> arguments = new ArrayList<>(Arrays.asList("-f", "unused.wal"));
    arguments.addAll(Arrays.asList(options));
    return new ReplayDecisionController(
        new DefaultParser().parse(ImportWAL.createOptions(), arguments.toArray(new String[0])),
        null);
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
    when(entry.getType()).thenReturn(WALEntryType.OBJECT_FILE_NODE);
    when(entry.getValue()).thenReturn(mock(ObjectNode.class));
    return entry;
  }

  private static WALEntry tableDeleteEntry(final TableDeletionEntry... entries) {
    return new WALInfoEntry(
        1, new RelationalDeleteDataNode(new PlanNodeId(""), Arrays.asList(entries), "source_db"));
  }

  private static TableDeletionEntry tableDeletion(
      final String table, final TagPredicate predicate, final long start, final long end) {
    return new TableDeletionEntry(
        new DeletionPredicate(table, predicate), new TimeRange(start, end));
  }

  private static SessionDataSet mockTableSchema(
      final Session session, final String table, final String timeColumn, final String... tags)
      throws Exception {
    final SessionDataSet dataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("DESCRIBE " + ImportWAL.WALReplayer.quoteIdentifier(table)))
        .thenReturn(dataSet);
    when(dataSet.iterator())
        .thenAnswer(
            ignored -> {
              final SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
              final AtomicInteger row = new AtomicInteger(-1);
              when(iterator.next()).thenAnswer(call -> row.incrementAndGet() <= tags.length);
              when(iterator.getString(1))
                  .thenAnswer(call -> row.get() == 0 ? timeColumn : tags[row.get() - 1]);
              when(iterator.getString(2))
                  .thenAnswer(call -> row.get() == 0 ? "TIMESTAMP" : "STRING");
              when(iterator.getString(3)).thenAnswer(call -> row.get() == 0 ? "TIME" : "TAG");
              return iterator;
            });
    return dataSet;
  }
}
