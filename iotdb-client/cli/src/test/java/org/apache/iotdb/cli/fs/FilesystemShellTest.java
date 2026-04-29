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

import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FilesystemShellTest {

  @Mock private FilesystemSchemaProvider provider;
  @Mock private FilesystemMutationProvider mutationProvider;

  private ByteArrayOutputStream out;
  private FilesystemShell shell;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    out = new ByteArrayOutputStream();
    CliContext ctx =
        new CliContext(
            new ByteArrayInputStream(new byte[0]),
            new PrintStream(out),
            System.err,
            ExitType.EXCEPTION);
    shell = new FilesystemShell(ctx, provider);
  }

  @Test
  public void executePwdPrintsCurrentPath() throws SQLException {
    assertTrue(shell.execute("pwd"));

    assertTrue(out.toString().contains("/"));
  }

  @Test
  public void executeLsPrintsChildNodes() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("root", FsPath.absolute("/root"), FsNodeType.TREE_ROOT),
                new FsNode("test", FsPath.absolute("/test"), FsNodeType.TREE_ROOT)));

    assertTrue(shell.execute("ls /"));

    assertTrue(out.toString().contains("root,test"));
    assertFalse(out.toString().contains("TREE_ROOT"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLlPrintsLongListing() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE),
                new FsNode("value", FsPath.absolute("/value"), FsNodeType.TABLE_COLUMN)));

    assertTrue(shell.execute("ll /"));

    assertTrue(out.toString().contains("dr-xr-xr-x"));
    assertTrue(out.toString().contains("-r--r--r--"));
    assertTrue(out.toString().contains("testtest"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLsLongOptionPrintsLongListing() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE)));

    assertTrue(shell.execute("ls -l /"));

    assertTrue(out.toString().contains("dr-xr-xr-x"));
    assertTrue(out.toString().contains("testtest"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeCdUpdatesCurrentPath() throws SQLException {
    when(provider.describe(FsPath.absolute("/root")))
        .thenReturn(new FsNode("root", FsPath.absolute("/root"), FsNodeType.TREE_ROOT));

    assertTrue(shell.execute("cd /root"));
    assertTrue(shell.execute("pwd"));

    assertTrue(out.toString().contains("/root"));
  }

  @Test
  public void executeExitStopsShell() throws SQLException {
    assertFalse(shell.execute("exit"));
  }

  @Test
  public void executeWriteCommandRejectsReadOnlyMode() throws SQLException {
    assertTrue(shell.execute("mkdir /db1"));

    assertTrue(out.toString().contains("mkdir: /db1: Read-only file system"));
    verifyZeroInteractions(mutationProvider);
  }

  @Test
  public void executeWriteCommandsWhenEnabled() throws SQLException {
    shell = new FilesystemShell(shellContext(), provider, mutationProvider, true);

    assertTrue(shell.execute("mkdir /db1"));
    assertTrue(shell.execute("rm /db1/table1"));
    assertTrue(shell.execute("mv /db1/table1 /db1/table2"));

    verify(mutationProvider).mkdir(FsPath.absolute("/db1"));
    verify(mutationProvider).remove(FsPath.absolute("/db1/table1"));
    verify(mutationProvider).move(FsPath.absolute("/db1/table1"), FsPath.absolute("/db1/table2"));
  }

  @Test
  public void executeTreePrintsChildrenUntilDepth() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(new FsNode("root", FsPath.absolute("/root"), FsNodeType.TREE_ROOT)));
    when(provider.list(FsPath.absolute("/root")))
        .thenReturn(
            Arrays.asList(new FsNode("sg", FsPath.absolute("/root/sg"), FsNodeType.TREE_DATABASE)));

    assertTrue(shell.execute("tree -L 2 /"));

    assertTrue(out.toString().contains("root"));
    assertTrue(out.toString().contains("sg"));
    assertFalse(out.toString().contains("TREE_ROOT"));
    assertFalse(out.toString().contains("TREE_DATABASE"));
    verify(provider).list(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/root"));
  }

  @Test
  public void executeCatReadsTablePath() throws SQLException {
    when(provider.read(FsPath.absolute("/db1/table1"), 20))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    assertTrue(shell.execute("cat /db1/table1"));

    assertTrue(out.toString().contains("1\ta\t42"));
    assertFalse(out.toString().contains("{"));
    verify(provider).read(FsPath.absolute("/db1/table1"), 20);
  }

  @Test
  public void executeCatReadsMultiplePathsSequentially() throws SQLException {
    when(provider.read(FsPath.absolute("/db1/table1/tag1"), 20))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "tag1", "a")));
    when(provider.read(FsPath.absolute("/db1/table1/s1"), 20))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "s1", "42")));

    assertTrue(shell.execute("cat /db1/table1/tag1 /db1/table1/s1"));

    assertTrue(out.toString().contains("1\ta"));
    assertTrue(out.toString().contains("1\t42"));
    verify(provider).read(FsPath.absolute("/db1/table1/tag1"), 20);
    verify(provider).read(FsPath.absolute("/db1/table1/s1"), 20);
  }

  @Test
  public void executeHeadReadsPathWithLimit() throws SQLException {
    when(provider.read(FsPath.absolute("/db1/table1"), 5))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    assertTrue(shell.execute("head -n 5 /db1/table1"));

    assertTrue(out.toString().contains("1\ta\t42"));
    verify(provider).read(FsPath.absolute("/db1/table1"), 5);
  }

  @Test
  public void executeTailReadsPathWithLimit() throws SQLException {
    when(provider.tail(FsPath.absolute("/db1/table1"), 3))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "2", "tag1", "b", "s1", "43")));

    assertTrue(shell.execute("tail -n 3 /db1/table1"));

    assertTrue(out.toString().contains("2\tb\t43"));
    verify(provider).tail(FsPath.absolute("/db1/table1"), 3);
  }

  @Test
  public void executeWcLineCountPrintsCountAndPath() throws SQLException {
    when(provider.count(FsPath.absolute("/db1/table1"))).thenReturn(2L);

    assertTrue(shell.execute("wc -l /db1/table1"));

    assertTrue(out.toString().contains("2 /db1/table1"));
    verify(provider).count(FsPath.absolute("/db1/table1"));
  }

  @Test
  public void executeGrepPrintsOnlyMatchingRows() throws SQLException {
    when(provider.read(FsPath.absolute("/db1/table1"), 20))
        .thenReturn(
            Arrays.asList(
                SqlRow.of("Time", "1", "tag1", "spricoder", "s1", "42"),
                SqlRow.of("Time", "2", "tag1", "other", "s1", "43")));

    assertTrue(shell.execute("grep spricoder /db1/table1"));

    assertTrue(out.toString().contains("1\tspricoder\t42"));
    assertFalse(out.toString().contains("2\tother\t43"));
    verify(provider).read(FsPath.absolute("/db1/table1"), 20);
  }

  @Test
  public void executeFindPrintsMatchingDescendants() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.describe(FsPath.absolute("/db1")))
        .thenReturn(new FsNode("db1", FsPath.absolute("/db1"), FsNodeType.TABLE_DATABASE));
    when(provider.describe(FsPath.absolute("/db1/table1")))
        .thenReturn(new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.TABLE_TABLE));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(new FsNode("db1", FsPath.absolute("/db1"), FsNodeType.TABLE_DATABASE)));
    when(provider.list(FsPath.absolute("/db1")))
        .thenReturn(
            Arrays.asList(
                new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.TABLE_TABLE)));
    when(provider.list(FsPath.absolute("/db1/table1"))).thenReturn(new ArrayList<>());

    assertTrue(shell.execute("find / -name table1"));

    assertTrue(out.toString().contains("/db1/table1"));
    verify(provider).list(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/db1"));
  }

  @Test
  public void executeLessAndMoreReadPath() throws SQLException {
    when(provider.read(FsPath.absolute("/db1/table1"), 20))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    assertTrue(shell.execute("less /db1/table1"));
    assertTrue(shell.execute("more /db1/table1"));

    assertTrue(out.toString().contains("1\ta\t42"));
    verify(provider, times(2)).read(FsPath.absolute("/db1/table1"), 20);
  }

  @Test
  public void executeFilePrintsNodeType() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1")))
        .thenReturn(new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.TABLE_TABLE));

    assertTrue(shell.execute("file /db1/table1"));

    assertTrue(out.toString().contains("/db1/table1: TABLE_TABLE"));
    verify(provider).describe(FsPath.absolute("/db1/table1"));
  }

  @Test
  public void executeDuPrintsLogicalSizeAndPath() throws SQLException {
    when(provider.count(FsPath.absolute("/db1/table1"))).thenReturn(2L);

    assertTrue(shell.execute("du /db1/table1"));

    assertTrue(out.toString().contains("2\t/db1/table1"));
    verify(provider).count(FsPath.absolute("/db1/table1"));
  }

  @Test
  public void executePasteReadsMultiplePaths() throws SQLException {
    when(provider.read(
            Arrays.asList(FsPath.absolute("/db1/table1/tag1"), FsPath.absolute("/db1/table1/s1")),
            20))
        .thenReturn(Arrays.asList(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    assertTrue(shell.execute("paste /db1/table1/tag1 /db1/table1/s1"));

    assertTrue(out.toString().contains("1\ta\t42"));
    assertFalse(out.toString().contains("{"));
    verify(provider)
        .read(
            Arrays.asList(FsPath.absolute("/db1/table1/tag1"), FsPath.absolute("/db1/table1/s1")),
            20);
  }

  @Test
  public void completerCompletesChildrenFromCurrentDirectory() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE),
                new FsNode("value", FsPath.absolute("/value"), FsNodeType.TABLE_COLUMN)));

    List<String> values = complete(shell.createCompleter(), "cd t");

    assertTrue(values.contains("testtest/"));
    assertFalse(values.contains("value"));
    verify(provider).list(FsPath.absolute("/"));
  }

  private static List<String> complete(Completer completer, String line) {
    ParsedLine parsedLine = new DefaultParser().parse(line, line.length());
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, parsedLine, candidates);
    return candidates.stream().map(Candidate::value).collect(Collectors.toList());
  }

  private CliContext shellContext() {
    return new CliContext(
        new ByteArrayInputStream(new byte[0]),
        new PrintStream(out),
        System.err,
        ExitType.EXCEPTION);
  }
}
