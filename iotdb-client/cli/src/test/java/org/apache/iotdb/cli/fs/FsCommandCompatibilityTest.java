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

package org.apache.iotdb.cli.fs;

import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.jline.utils.NonBlocking;
import org.jline.utils.NonBlockingReader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FsCommandCompatibilityTest {

  @Mock private FilesystemSchemaProvider provider;
  @Mock private FilesystemMutationProvider mutations;
  private ByteArrayOutputStream out;
  private ByteArrayOutputStream err;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.initMocks(this);
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    when(provider.describe(any(FsPath.class)))
        .thenAnswer(
            invocation -> {
              FsPath path = invocation.getArgument(0);
              return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
            });
  }

  @Test
  public void wcCountsExactUtf8InputWithoutAddingNewline() {
    FilesystemShell shell = shell("\u4e2da", false);

    assertEquals(0, shell.runNonInteractive("wc -c"));

    assertEquals("4" + System.lineSeparator(), out.toString());
    verifyZeroInteractions(mutations);
  }

  @Test
  public void tailBytesPreservesInputWithoutFinalNewline() {
    FilesystemShell shell = shell("abcdef", false);

    assertEquals(0, shell.runNonInteractive("tail -c 3 -"));

    assertEquals("def", out.toString());
  }

  @Test
  public void tailBytesFromStartUsesOneBasedOffset() {
    FilesystemShell shell = shell("abcdef", false);

    assertEquals(0, shell.runNonInteractive("tail -c +3 -"));

    assertEquals("cdef", out.toString());
  }

  @Test
  public void tailLinesFromStartIncludesRequestedLine() {
    FilesystemShell shell = shell("one\ntwo\nthree", false);

    assertEquals(0, shell.runNonInteractive("tail -n +2 -"));

    assertEquals("two\nthree", out.toString());
  }

  @Test
  public void tailLastLinePreservesTrailingNewline() {
    FilesystemShell shell = shell("one\ntwo\nthree\n", false);

    assertEquals(0, shell.runNonInteractive("tail -n 1 -"));

    assertEquals("three\n", out.toString());
  }

  @Test
  public void tailZeroLinesIsEmpty() {
    FilesystemShell shell = shell("one\ntwo\n", false);

    assertEquals(0, shell.runNonInteractive("tail -n 0 -"));

    assertEquals("", out.toString());
  }

  @Test
  public void teeEchoesExactInputAndOverwritesEveryTarget() throws SQLException {
    FilesystemShell shell = shell("time,value\n1,2", true);

    assertEquals(0, shell.runNonInteractive("tee /db/a.csv /db/b.csv"));

    assertEquals("time,value\n1,2", out.toString());
    verify(mutations)
        .write(FsPath.absolute("/db/a.csv"), Arrays.asList("time,value", "1,2"), false);
    verify(mutations)
        .write(FsPath.absolute("/db/b.csv"), Arrays.asList("time,value", "1,2"), false);
  }

  @Test
  public void teeAppendEchoesAndRequestsAppend() throws SQLException {
    FilesystemShell shell = shell("time,value\n1,2\n", true);

    assertEquals(0, shell.runNonInteractive("tee -a /db/a.csv"));

    assertEquals("time,value\n1,2\n", out.toString());
    verify(mutations).write(FsPath.absolute("/db/a.csv"), Arrays.asList("time,value", "1,2"), true);
  }

  @Test
  public void teeWithoutTargetsCopiesStandardInput() {
    FilesystemShell shell = shell("payload", false);

    assertEquals(0, shell.runNonInteractive("tee"));

    assertEquals("payload", out.toString());
    verifyZeroInteractions(mutations);
  }

  @Test
  public void teeReadonlyDoesNotConsumeOrWriteTargets() {
    FilesystemShell shell = shell("payload", false);

    assertEquals(FilesystemShell.RUNTIME_ERROR, shell.runNonInteractive("tee /db/a.csv"));

    assertEquals("", out.toString());
    verifyZeroInteractions(mutations);
  }

  @Test
  public void interactiveTeeUsesTerminalReaderWithoutClosingIt() throws Exception {
    ByteArrayInputStream rawInput = new ByteArrayInputStream(new byte[] {1, 2, 3});
    CliContext context =
        new CliContext(rawInput, new PrintStream(out), new PrintStream(err), ExitType.EXCEPTION);
    LineReader lineReader = mock(LineReader.class);
    Terminal terminal = mock(Terminal.class);
    NonBlockingReader terminalReader =
        spy(NonBlocking.nonBlocking("tee-test", new StringReader("time,value\n1,42")));
    when(lineReader.getTerminal()).thenReturn(terminal);
    when(terminal.reader()).thenReturn(terminalReader);
    context.setLineReader(lineReader);

    try {
      assertTrue(new FilesystemShell(context, provider, mutations, true).execute("tee /db/a.csv"));

      assertEquals("time,value\n1,42", out.toString());
      assertEquals(3, rawInput.available());
      verify(mutations)
          .write(FsPath.absolute("/db/a.csv"), Arrays.asList("time,value", "1,42"), false);
      verify(terminalReader, never()).close();
    } finally {
      terminalReader.close();
    }
  }

  @Test
  public void cdDashReturnsToPreviousDirectoryAndPrintsIt() throws SQLException {
    directory("/a");
    directory("/b");
    FilesystemShell shell = shell("", false);

    assertEquals(0, shell.runNonInteractive("cd /a"));
    assertEquals(0, shell.runNonInteractive("cd /b"));
    assertEquals(0, shell.runNonInteractive("cd -"));
    assertEquals(0, shell.runNonInteractive("pwd"));

    assertEquals("/a\n/a\n", out.toString());
  }

  @Test
  public void cdWithoutArgumentsReturnsToVirtualRoot() throws SQLException {
    directory("/a");
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    FilesystemShell shell = shell("", false);

    assertEquals(0, shell.runNonInteractive("cd /a"));
    assertEquals(0, shell.runNonInteractive("cd"));
    assertEquals(0, shell.runNonInteractive("pwd"));

    assertEquals("/" + System.lineSeparator(), out.toString());
  }

  @Test
  public void exitReturnsRequestedStatus() {
    assertEquals(7, shell("", false).runNonInteractive("exit 7"));
    verifyZeroInteractions(mutations);
  }

  @Test
  public void copyNoClobberSkipsExistingTarget() throws SQLException {
    file("/db/a.csv");
    file("/db/b.csv");

    assertEquals(0, shell("", true).runNonInteractive("cp -n /db/a.csv /db/b.csv"));

    verifyZeroInteractions(mutations);
  }

  @Test
  public void copyDefaultsToReplacingExistingTarget() throws SQLException {
    file("/db/a.csv");
    file("/db/b.csv");

    assertEquals(0, shell("", true).runNonInteractive("cp /db/a.csv /db/b.csv"));

    verify(mutations).copy(FsPath.absolute("/db/a.csv"), FsPath.absolute("/db/b.csv"), true);
  }

  @Test
  public void copyMultipleSourcesUsesDestinationDirectory() throws SQLException {
    file("/db/a.csv");
    file("/db/b.csv");
    directory("/dest");

    assertEquals(0, shell("", true).runNonInteractive("cp /db/a.csv /db/b.csv /dest"));

    verify(mutations).copy(FsPath.absolute("/db/a.csv"), FsPath.absolute("/dest/a.csv"), false);
    verify(mutations).copy(FsPath.absolute("/db/b.csv"), FsPath.absolute("/dest/b.csv"), false);
  }

  @Test
  public void copyInteractiveRefusalKeepsTarget() throws SQLException {
    file("/db/a.csv");
    file("/db/b.csv");

    assertEquals(0, shell("n\n", true).runNonInteractive("cp -i /db/a.csv /db/b.csv"));

    verifyZeroInteractions(mutations);
    assertTrue(err.size() > 0);
  }

  @Test
  public void copyInteractiveReadsAnAnswerForEachTarget() throws SQLException {
    file("/db/a.csv");
    file("/db/b.csv");
    directory("/dest");
    file("/dest/a.csv");
    file("/dest/b.csv");

    assertEquals(0, shell("y\ny\n", true).runNonInteractive("cp -i /db/a.csv /db/b.csv /dest"));

    verify(mutations).copy(FsPath.absolute("/db/a.csv"), FsPath.absolute("/dest/a.csv"), true);
    verify(mutations).copy(FsPath.absolute("/db/b.csv"), FsPath.absolute("/dest/b.csv"), true);
  }

  @Test
  public void removeForceSkipsMissingPaths() {
    assertEquals(0, shell("", true).runNonInteractive("rm -f /missing.csv"));
    verifyZeroInteractions(mutations);
  }

  @Test
  public void removeForceStillRemovesExistingPaths() throws SQLException {
    file("/db/a.csv");

    assertEquals(0, shell("", true).runNonInteractive("rm -f /db/a.csv /missing.csv"));

    verify(mutations).remove(FsPath.absolute("/db/a.csv"));
    verify(mutations, never()).remove(FsPath.absolute("/missing.csv"));
  }

  @Test
  public void mkdirParentsAcceptsExistingDatabase() throws SQLException {
    directory("/db");

    assertEquals(0, shell("", true).runNonInteractive("mkdir -p /db"));

    verifyZeroInteractions(mutations);
  }

  @Test
  public void rmdirPassesEveryDirectoryToGuardedProvider() throws SQLException {
    directory("/a");
    directory("/b");

    assertEquals(0, shell("", true).runNonInteractive("rmdir /a /b"));

    verify(mutations).rmdir(FsPath.absolute("/a"));
    verify(mutations).rmdir(FsPath.absolute("/b"));
  }

  @Test
  public void lessAndMorePrintAllStandardInputInBatchMode() {
    StringBuilder input = new StringBuilder();
    for (int i = 0; i < 40; i++) input.append(i).append('\n');

    assertEquals(0, shell(input.toString(), false).runNonInteractive("less -"));
    assertEquals(input.toString(), out.toString());
    out.reset();
    assertEquals(0, shell(input.toString(), false).runNonInteractive("more -"));
    assertEquals(input.toString(), out.toString());
  }

  @Test
  public void structuredListingUsesModelAndObjectWithoutDuplicatingSidecar() throws SQLException {
    directory("/db");
    when(provider.list(FsPath.absolute("/db")))
        .thenReturn(
            Arrays.asList(
                new FsNode(
                    "sensors.csv",
                    FsPath.absolute("/db/sensors.csv"),
                    FsNodeType.TABLE_DATA_FILE,
                    Collections.singletonMap("table", "sensors")),
                new FsNode(
                    "sensors.meta",
                    FsPath.absolute("/db/sensors.meta"),
                    FsNodeType.TABLE_META_FILE,
                    Collections.singletonMap("table", "sensors"))));

    assertEquals(0, shell("", false).runNonInteractive("ls -f csv /db"));

    assertEquals("model,object\ntable,sensors\n", out.toString());
    verify(provider, never()).read(any(FsPath.class), org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  public void longListingDoesNotInventPermissionsOwnersOrSize() throws SQLException {
    file("/db/sensors.csv");

    assertEquals(0, shell("", false).runNonInteractive("ls -l /db/sensors.csv"));

    assertEquals("---------- - - - - - sensors.csv\n", out.toString());
    assertFalse(out.toString().contains("iotdb"));
    verify(provider, never()).read(any(FsPath.class), org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  public void treePrintsRootBranchesAndDisplayedCounts() throws SQLException {
    directory("/db");
    when(provider.list(FsPath.absolute("/db")))
        .thenReturn(
            Arrays.asList(
                new FsNode("a.csv", FsPath.absolute("/db/a.csv"), FsNodeType.TABLE_DATA_FILE),
                new FsNode("b.csv", FsPath.absolute("/db/b.csv"), FsNodeType.TABLE_DATA_FILE)));

    assertEquals(0, shell("", false).runNonInteractive("tree /db"));

    assertEquals(
        "/db\n|-- a.csv\n`-- b.csv\n\n" + String.format(CliMessages.FS_TREE_SUMMARY, 0, 2) + "\n",
        out.toString());
  }

  @Test
  public void statSizeMatchesCatCsvAndExcludesAttributes() throws SQLException {
    file("/db/a.csv");
    when(provider.columns(FsPath.absolute("/db/a.csv")))
        .thenReturn(
            Arrays.asList(
                new FsColumn("time", "TIME", "TIMESTAMP"),
                new FsColumn("owner", "ATTRIBUTE", "STRING"),
                new FsColumn("value", "FIELD", "INT32")));
    when(provider.read(FsPath.absolute("/db/a.csv"), -1))
        .thenReturn(SqlRow.list(SqlRow.of("time", "1", "owner", "hidden", "value", "2")));
    FilesystemShell shell = shell("", false);

    assertEquals(0, shell.runNonInteractive("cat -f csv /db/a.csv"));
    int size = out.size();
    assertFalse(out.toString().contains("owner"));
    out.reset();
    assertEquals(0, shell.runNonInteractive("stat /db/a.csv"));

    assertTrue(out.toString().contains(String.format(CliMessages.FS_STAT_SIZE, size)));
  }

  private FilesystemShell shell(String input, boolean writeEnabled) {
    return new FilesystemShell(
        new CliContext(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
            new PrintStream(out),
            new PrintStream(err),
            ExitType.EXCEPTION),
        provider,
        mutations,
        writeEnabled);
  }

  private void file(String value) throws SQLException {
    FsPath path = FsPath.absolute(value);
    when(provider.describe(path))
        .thenReturn(new FsNode(path.getFileName(), path, FsNodeType.TABLE_DATA_FILE));
  }

  private void directory(String value) throws SQLException {
    FsPath path = FsPath.absolute(value);
    when(provider.describe(path))
        .thenReturn(new FsNode(path.getFileName(), path, FsNodeType.TABLE_DATABASE));
  }
}
