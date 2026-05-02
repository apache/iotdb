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
import org.jline.reader.LineReader;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FilesystemShellTest {

  @Mock private FilesystemSchemaProvider provider;
  @Mock private FilesystemMutationProvider mutationProvider;
  @Mock private LineReader lineReader;

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
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("root", FsPath.absolute("/root"), FsNodeType.TREE_ROOT),
                new FsNode("test", FsPath.absolute("/test"), FsNodeType.TREE_ROOT)));

    assertTrue(shell.execute("ls /"));

    assertEquals("root" + System.lineSeparator() + "test" + System.lineSeparator(), out.toString());
    assertFalse(out.toString().contains(","));
    assertFalse(out.toString().contains("TREE_ROOT"));
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLlPrintsLongListing() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE),
                new FsNode(
                    "value.csv", FsPath.absolute("/value.csv"), FsNodeType.TABLE_DATA_FILE)));

    assertTrue(shell.execute("ll /"));

    assertTrue(out.toString().contains("dr-xr-xr-x"));
    assertTrue(out.toString().contains("-r--r--r--"));
    assertTrue(out.toString().contains("testtest"));
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLsLongOptionPrintsLongListing() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE)));

    assertTrue(shell.execute("ls -l /"));

    assertTrue(out.toString().contains("dr-xr-xr-x"));
    assertTrue(out.toString().contains("testtest"));
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLsAllPrintsDotEntries() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE)));

    assertTrue(shell.execute("ls -a /"));

    assertEquals(
        "."
            + System.lineSeparator()
            + ".."
            + System.lineSeparator()
            + "testtest"
            + System.lineSeparator(),
        out.toString());
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLlAllPrintsDotEntriesInLongListing() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE)));

    assertTrue(shell.execute("ll -a /"));

    assertTrue(out.toString().contains("dr-xr-xr-x  1 iotdb iotdb 0 ."));
    assertTrue(out.toString().contains("dr-xr-xr-x  1 iotdb iotdb 0 .."));
    assertTrue(out.toString().contains("dr-xr-xr-x  1 iotdb iotdb 0 testtest"));
    assertFalse(out.toString().contains("-a"));
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void executeLsRegularFilePrintsFileName() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1.csv")))
        .thenReturn(
            new FsNode(
                "table1.csv", FsPath.absolute("/db1/table1.csv"), FsNodeType.TABLE_DATA_FILE));

    assertTrue(shell.execute("ls /db1/table1.csv"));

    assertEquals("table1.csv" + System.lineSeparator(), out.toString());
    verify(provider).describe(FsPath.absolute("/db1/table1.csv"));
    verify(provider, times(0)).list(FsPath.absolute("/db1/table1.csv"));
  }

  @Test
  public void executeLsUnknownPathPrintsNoSuchFile() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1")))
        .thenReturn(new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.UNKNOWN));

    assertTrue(shell.execute("ls /db1/table1"));

    assertEquals(
        "ls: /db1/table1: No such file or directory" + System.lineSeparator(), out.toString());
    verify(provider).describe(FsPath.absolute("/db1/table1"));
    verify(provider, times(0)).list(FsPath.absolute("/db1/table1"));
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
    assertTrue(shell.execute("rm /db1/table1.csv"));
    assertTrue(shell.execute("mv /db1/table1.csv /db1/table2.csv"));

    verify(mutationProvider).mkdir(FsPath.absolute("/db1"));
    verify(mutationProvider).remove(FsPath.absolute("/db1/table1.csv"));
    verify(mutationProvider)
        .move(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db1/table2.csv"));
  }

  @Test
  public void executeStandardWriteCommandsWhenEnabled() throws SQLException {
    shell = new FilesystemShell(shellContext(), provider, mutationProvider, true);

    assertTrue(shell.execute("rmdir /db1"));
    assertTrue(shell.execute("rm -r /db2"));
    assertTrue(shell.execute("cp /db1/table1.schema /db1/table2.schema"));

    verify(mutationProvider).rmdir(FsPath.absolute("/db1"));
    verify(mutationProvider).removeRecursive(FsPath.absolute("/db2"));
    verify(mutationProvider)
        .copy(FsPath.absolute("/db1/table1.schema"), FsPath.absolute("/db1/table2.schema"));
  }

  @Test
  public void executeRecursiveRemoveRejectsReadOnlyMode() throws SQLException {
    assertTrue(shell.execute("rm -r /db1"));

    assertTrue(out.toString().contains("rm: /db1: Read-only file system"));
    verifyZeroInteractions(mutationProvider);
  }

  @Test
  public void executeTeeRejectsReadOnlyMode() throws SQLException {
    assertTrue(shell.execute("tee -a /db1/table1.csv"));

    assertTrue(out.toString().contains("tee: /db1/table1.csv: Read-only file system"));
    verifyZeroInteractions(mutationProvider);
  }

  @Test
  public void executeTeeReadsInteractiveBufferUntilWriteQuit() throws SQLException {
    CliContext ctx = shellContext();
    ctx.setLineReader(lineReader);
    shell = new FilesystemShell(ctx, provider, mutationProvider, true);
    when(lineReader.readLine("tee> ", null)).thenReturn("time,key,value", "1,spricoder,2.0", ":wq");

    assertTrue(shell.execute("tee -a /db1/table1.csv"));

    verify(mutationProvider)
        .append(
            FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,spricoder,2.0"));
  }

  @Test
  public void executeTeeQuitWarnsBeforeDiscardingInteractiveBuffer() throws SQLException {
    CliContext ctx = shellContext();
    ctx.setLineReader(lineReader);
    shell = new FilesystemShell(ctx, provider, mutationProvider, true);
    when(lineReader.readLine("tee> ", null)).thenReturn("time,key,value", ":q", ":q!");

    assertTrue(shell.execute("tee -a /db1/table1.csv"));

    assertTrue(out.toString().contains("tee: use :wq to write or :q! to quit without writing"));
    verifyZeroInteractions(mutationProvider);
  }

  @Test
  public void executeTeeNonInteractiveReadsStandardInputUntilEof() throws SQLException {
    CliContext ctx =
        new CliContext(
            new ByteArrayInputStream("time,key,value\n1,spricoder,2.0\n".getBytes()),
            new PrintStream(out),
            System.err,
            ExitType.EXCEPTION);
    shell = new FilesystemShell(ctx, provider, mutationProvider, true);

    assertTrue(shell.executeNonInteractive("tee -a /db1/table1.csv"));

    verify(mutationProvider)
        .append(
            FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,spricoder,2.0"));
  }

  @Test
  public void executeTreePrintsChildrenUntilDepth() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
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
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/root"));
  }

  @Test
  public void executeLsRecursivePrintsChildren() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(new FsNode("db1", FsPath.absolute("/db1"), FsNodeType.TABLE_DATABASE)));
    when(provider.list(FsPath.absolute("/db1")))
        .thenReturn(
            Arrays.asList(
                new FsNode(
                    "table1.csv", FsPath.absolute("/db1/table1.csv"), FsNodeType.TABLE_DATA_FILE)));

    assertTrue(shell.execute("ls -R /"));

    assertTrue(out.toString().contains("db1"));
    assertTrue(out.toString().contains("table1.csv"));
    verify(provider).describe(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/db1"));
  }

  @Test
  public void executeTreeUnknownPathPrintsNoSuchFile() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1")))
        .thenReturn(new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.UNKNOWN));

    assertTrue(shell.execute("tree /db1/table1"));

    assertEquals(
        "tree: /db1/table1: No such file or directory" + System.lineSeparator(), out.toString());
    verify(provider).describe(FsPath.absolute("/db1/table1"));
    verify(provider, times(0)).list(FsPath.absolute("/db1/table1"));
  }

  @Test
  public void executeCatReadsSchemaFileLines() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.schema"), 20))
        .thenReturn(Arrays.asList("ColumnName,DataType", "key,STRING"));

    assertTrue(shell.execute("cat /db1/table1.schema"));

    assertTrue(out.toString().contains("ColumnName,DataType"));
    assertTrue(out.toString().contains("key,STRING"));
    verify(provider).readLines(FsPath.absolute("/db1/table1.schema"), 20);
  }

  @Test
  public void executeCatPrintsCsvFileLines() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("Time,tag1,s1", "1,a,42"));

    assertTrue(shell.execute("cat /db1/table1.csv"));

    assertTrue(out.toString().contains("Time,tag1,s1"));
    assertTrue(out.toString().contains("1,a,42"));
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
  }

  @Test
  public void executeCatReadsMultipleTextFilesSequentially() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("time,key", "1,a"));
    when(provider.readLines(FsPath.absolute("/db1/table1.meta"), 20))
        .thenReturn(Arrays.asList("TableName,Status", "table1,USING"));

    assertTrue(shell.execute("cat /db1/table1.csv /db1/table1.meta"));

    assertEquals(
        "time,key"
            + System.lineSeparator()
            + "1,a"
            + System.lineSeparator()
            + "TableName,Status"
            + System.lineSeparator()
            + "table1,USING"
            + System.lineSeparator(),
        out.toString());
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
    verify(provider).readLines(FsPath.absolute("/db1/table1.meta"), 20);
  }

  @Test
  public void executeHeadReadsSchemaFileLinesWithLimit() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.schema"), 5))
        .thenReturn(Arrays.asList("ColumnName,DataType", "key,STRING"));

    assertTrue(shell.execute("head -n 5 /db1/table1.schema"));

    assertTrue(out.toString().contains("ColumnName,DataType"));
    verify(provider).readLines(FsPath.absolute("/db1/table1.schema"), 5);
  }

  @Test
  public void executeHeadReadsCsvFileLinesWithLimit() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 5))
        .thenReturn(Arrays.asList("Time,tag1,s1", "1,a,42"));

    assertTrue(shell.execute("head -n 5 /db1/table1.csv"));

    assertTrue(out.toString().contains("Time,tag1,s1"));
    assertTrue(out.toString().contains("1,a,42"));
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 5);
  }

  @Test
  public void executeTailReadsSchemaFileLinesWithLimit() throws SQLException {
    when(provider.tailLines(FsPath.absolute("/db1/table1.schema"), 3))
        .thenReturn(Arrays.asList("key,STRING", "value,DOUBLE"));

    assertTrue(shell.execute("tail -n 3 /db1/table1.schema"));

    assertTrue(out.toString().contains("value,DOUBLE"));
    verify(provider).tailLines(FsPath.absolute("/db1/table1.schema"), 3);
  }

  @Test
  public void executeTailReadsCsvFileLinesWithLimit() throws SQLException {
    when(provider.tailLines(FsPath.absolute("/db1/table1.csv"), 3))
        .thenReturn(Arrays.asList("Time,tag1,s1", "2,b,43"));

    assertTrue(shell.execute("tail -n 3 /db1/table1.csv"));

    assertTrue(out.toString().contains("Time,tag1,s1"));
    assertTrue(out.toString().contains("2,b,43"));
    verify(provider).tailLines(FsPath.absolute("/db1/table1.csv"), 3);
  }

  @Test
  public void executeWcLineCountPrintsCountAndPath() throws SQLException {
    when(provider.count(FsPath.absolute("/db1/table1.csv"))).thenReturn(2L);

    assertTrue(shell.execute("wc -l /db1/table1.csv"));

    assertTrue(out.toString().contains("2 /db1/table1.csv"));
    verify(provider).count(FsPath.absolute("/db1/table1.csv"));
  }

  @Test
  public void executeGrepFiltersCsvFileLines() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("Time,tag1,s1", "1,spricoder,42", "2,other,43"));

    assertTrue(shell.execute("grep spricoder /db1/table1.csv"));

    assertTrue(out.toString().contains("1,spricoder,42"));
    assertFalse(out.toString().contains("2,other,43"));
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
  }

  @Test
  public void executeGrepPrintsOnlyMatchingRows() throws SQLException {
    when(provider.read(FsPath.absolute("/root/sg/d1/s1"), 20))
        .thenReturn(
            Arrays.asList(
                SqlRow.of("Time", "1", "tag1", "spricoder", "s1", "42"),
                SqlRow.of("Time", "2", "tag1", "other", "s1", "43")));

    assertTrue(shell.execute("grep spricoder /root/sg/d1/s1"));

    assertTrue(out.toString().contains("1\tspricoder\t42"));
    assertFalse(out.toString().contains("2\tother\t43"));
    verify(provider).read(FsPath.absolute("/root/sg/d1/s1"), 20);
  }

  @Test
  public void executeFindPrintsMatchingDescendants() throws SQLException {
    when(provider.describe(FsPath.absolute("/")))
        .thenReturn(new FsNode("/", FsPath.absolute("/"), FsNodeType.VIRTUAL_ROOT));
    when(provider.describe(FsPath.absolute("/db1")))
        .thenReturn(new FsNode("db1", FsPath.absolute("/db1"), FsNodeType.TABLE_DATABASE));
    when(provider.describe(FsPath.absolute("/db1/table1.csv")))
        .thenReturn(
            new FsNode(
                "table1.csv", FsPath.absolute("/db1/table1.csv"), FsNodeType.TABLE_DATA_FILE));
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(new FsNode("db1", FsPath.absolute("/db1"), FsNodeType.TABLE_DATABASE)));
    when(provider.list(FsPath.absolute("/db1")))
        .thenReturn(
            Arrays.asList(
                new FsNode(
                    "table1.csv", FsPath.absolute("/db1/table1.csv"), FsNodeType.TABLE_DATA_FILE)));

    assertTrue(shell.execute("find / -name table1.csv"));

    assertTrue(out.toString().contains("/db1/table1.csv"));
    verify(provider).list(FsPath.absolute("/"));
    verify(provider).list(FsPath.absolute("/db1"));
  }

  @Test
  public void executeLessAndMoreReadMetaFileLines() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.meta"), 20))
        .thenReturn(Arrays.asList("TableName,Status", "table1,USING"));

    assertTrue(shell.execute("less /db1/table1.meta"));
    assertTrue(shell.execute("more /db1/table1.meta"));

    assertTrue(out.toString().contains("table1,USING"));
    verify(provider, times(2)).readLines(FsPath.absolute("/db1/table1.meta"), 20);
  }

  @Test
  public void executeLessAndMoreReadCsvFileLines() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("Time,tag1,s1", "1,a,42"));

    assertTrue(shell.execute("less /db1/table1.csv"));
    assertTrue(shell.execute("more /db1/table1.csv"));

    assertTrue(out.toString().contains("Time,tag1,s1"));
    assertTrue(out.toString().contains("1,a,42"));
    verify(provider, times(2)).readLines(FsPath.absolute("/db1/table1.csv"), 20);
  }

  @Test
  public void executeStatPrintsUnixStyleMetadata() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1.csv")))
        .thenReturn(
            new FsNode(
                "table1.csv",
                FsPath.absolute("/db1/table1.csv"),
                FsNodeType.TABLE_DATA_FILE,
                java.util.Collections.singletonMap("table", "table1")));

    assertTrue(shell.execute("stat /db1/table1.csv"));

    assertTrue(out.toString().contains("File: /db1/table1.csv"));
    assertTrue(out.toString().contains("Type: regular file"));
    assertTrue(out.toString().contains("table: table1"));
    assertFalse(out.toString().contains("TABLE_DATA_FILE"));
    verify(provider).describe(FsPath.absolute("/db1/table1.csv"));
  }

  @Test
  public void executeFilePrintsUnixFileType() throws SQLException {
    when(provider.describe(FsPath.absolute("/db1/table1")))
        .thenReturn(new FsNode("table1", FsPath.absolute("/db1/table1"), FsNodeType.UNKNOWN));

    assertTrue(shell.execute("file /db1/table1"));

    assertTrue(out.toString().contains("/db1/table1: unknown"));
    verify(provider).describe(FsPath.absolute("/db1/table1"));
  }

  @Test
  public void executeDuPrintsLogicalSizeAndPath() throws SQLException {
    when(provider.count(FsPath.absolute("/db1/table1.csv"))).thenReturn(2L);

    assertTrue(shell.execute("du /db1/table1.csv"));

    assertTrue(out.toString().contains("2\t/db1/table1.csv"));
    verify(provider).count(FsPath.absolute("/db1/table1.csv"));
  }

  @Test
  public void executePasteReadsMultipleTextFilesLineByLine() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("time,key", "1,a", "2,b"));
    when(provider.readLines(FsPath.absolute("/db1/table2.csv"), 20))
        .thenReturn(Arrays.asList("time,value", "1,42"));

    assertTrue(shell.execute("paste /db1/table1.csv /db1/table2.csv"));

    assertEquals(
        "time,key\ttime,value"
            + System.lineSeparator()
            + "1,a\t1,42"
            + System.lineSeparator()
            + "2,b\t"
            + System.lineSeparator(),
        out.toString());
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
    verify(provider).readLines(FsPath.absolute("/db1/table2.csv"), 20);
  }

  @Test
  public void executeJoinMatchesCsvRowsByFirstField() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("key,value", "a,10", "b,20", "c,30"));
    when(provider.readLines(FsPath.absolute("/db1/table2.csv"), 20))
        .thenReturn(Arrays.asList("key,status", "a,ok", "b,bad", "d,missing"));

    assertTrue(shell.execute("join -t, /db1/table1.csv /db1/table2.csv"));

    assertEquals(
        "key,value,status"
            + System.lineSeparator()
            + "a,10,ok"
            + System.lineSeparator()
            + "b,20,bad"
            + System.lineSeparator(),
        out.toString());
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
    verify(provider).readLines(FsPath.absolute("/db1/table2.csv"), 20);
  }

  @Test
  public void executeJoinSupportsDifferentKeyFields() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("time,key,value", "1,a,10", "2,b,20"));
    when(provider.readLines(FsPath.absolute("/db1/table2.csv"), 20))
        .thenReturn(Arrays.asList("key,status", "a,ok", "b,bad"));

    assertTrue(shell.execute("join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv"));

    assertEquals(
        "key,time,value,status"
            + System.lineSeparator()
            + "a,1,10,ok"
            + System.lineSeparator()
            + "b,2,20,bad"
            + System.lineSeparator(),
        out.toString());
  }

  @Test
  public void executeJoinPrintsCartesianMatchesForDuplicateKeys() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("a,10", "a,11"));
    when(provider.readLines(FsPath.absolute("/db1/table2.csv"), 20))
        .thenReturn(Arrays.asList("a,ok", "a,good"));

    assertTrue(shell.execute("join -t, /db1/table1.csv /db1/table2.csv"));

    assertEquals(
        "a,10,ok"
            + System.lineSeparator()
            + "a,10,good"
            + System.lineSeparator()
            + "a,11,ok"
            + System.lineSeparator()
            + "a,11,good"
            + System.lineSeparator(),
        out.toString());
  }

  @Test
  public void executeJoinSkipsRowsWithoutKeyOrMatch() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("a,10", "missing-key", "b,20"));
    when(provider.readLines(FsPath.absolute("/db1/table2.csv"), 20))
        .thenReturn(Arrays.asList("a,ok", "c,unused"));

    assertTrue(shell.execute("join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv"));

    assertEquals("", out.toString());
  }

  @Test
  public void executeJoinUsesReadableRowsForNonTextPath() throws SQLException {
    when(provider.read(FsPath.absolute("/root/sg/d1/s1"), 20))
        .thenReturn(
            Arrays.asList(SqlRow.of("Time", "1", "s1", "10"), SqlRow.of("Time", "2", "s1", "20")));
    when(provider.read(FsPath.absolute("/root/sg/d1/s2"), 20))
        .thenReturn(
            Arrays.asList(
                SqlRow.of("Time", "1", "s2", "ok"), SqlRow.of("Time", "3", "s2", "skip")));

    assertTrue(shell.execute("join /root/sg/d1/s1 /root/sg/d1/s2"));

    assertEquals("1 10 ok" + System.lineSeparator(), out.toString());
    verify(provider).read(FsPath.absolute("/root/sg/d1/s1"), 20);
    verify(provider).read(FsPath.absolute("/root/sg/d1/s2"), 20);
  }

  @Test
  public void executeCutSelectsCsvFieldsByNumber() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("time,key,value", "1,spricoder,2.0", "2,other,3.0"));

    assertTrue(shell.execute("cut -d, -f2,3 /db1/table1.csv"));

    assertEquals(
        "key,value"
            + System.lineSeparator()
            + "spricoder,2.0"
            + System.lineSeparator()
            + "other,3.0"
            + System.lineSeparator(),
        out.toString());
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
  }

  @Test
  public void executeCutSelectsCsvFieldRange() throws SQLException {
    when(provider.readLines(FsPath.absolute("/db1/table1.csv"), 20))
        .thenReturn(Arrays.asList("time,key,value", "1,spricoder,2.0"));

    assertTrue(shell.execute("cut -d, -f1-2 /db1/table1.csv"));

    assertEquals(
        "time,key" + System.lineSeparator() + "1,spricoder" + System.lineSeparator(),
        out.toString());
    verify(provider).readLines(FsPath.absolute("/db1/table1.csv"), 20);
  }

  @Test
  public void completerCompletesChildrenFromCurrentDirectory() throws SQLException {
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("testtest", FsPath.absolute("/testtest"), FsNodeType.TABLE_DATABASE),
                new FsNode(
                    "value.csv", FsPath.absolute("/value.csv"), FsNodeType.TABLE_DATA_FILE)));

    List<String> values = complete(shell.createCompleter(), "cd t");

    assertTrue(values.contains("testtest/"));
    assertFalse(values.contains("value"));
    verify(provider).list(FsPath.absolute("/"));
  }

  @Test
  public void completerCompletesJoinCommand() {
    List<String> values = complete(shell.createCompleter(), "j");

    assertTrue(values.contains("join"));
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
