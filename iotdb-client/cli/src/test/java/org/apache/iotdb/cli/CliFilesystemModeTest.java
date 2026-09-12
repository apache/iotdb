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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.fs.FilesystemShell;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.jline.reader.LineReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CliFilesystemModeTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock private IoTDBConnection connection;
  @Mock private Statement statement;
  @Mock private FilesystemShell shell;
  @Mock private LineReader lineReader;

  private CliContext ctx;
  private ByteArrayOutputStream out;
  private ByteArrayOutputStream err;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    ctx = new CliContext(System.in, new PrintStream(out), new PrintStream(err), ExitType.EXCEPTION);
  }

  @Test
  public void createFilesystemShellUsesTableProviderForTableDialect() throws Exception {
    when(connection.getSqlDialect()).thenReturn("table");
    mockSingleColumnQuery("SHOW DATABASES", "Database", "db1");
    mockSingleColumnQuery("SHOW TABLES FROM db1", "TableName", "table1");

    FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
    shell.execute("ls /db1");

    verify(statement).executeQuery("SHOW DATABASES");
    verify(statement).executeQuery("SHOW TABLES FROM db1");
  }

  @Test
  public void createFilesystemShellUsesWriteModeForTableDialect() throws Exception {
    when(connection.getSqlDialect()).thenReturn("table");
    when(connection.createStatement()).thenReturn(statement);
    AbstractCli.setFsWriteMode(AbstractCli.FS_WRITE_MODE_ENABLED);
    try {
      FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
      shell.execute("mkdir /db1");
    } finally {
      AbstractCli.setFsWriteMode(AbstractCli.FS_WRITE_MODE_DISABLED);
    }

    verify(statement).execute("CREATE DATABASE db1");
  }

  @Test
  public void createFilesystemShellRejectsWriteWhenWriteModeDisabled() throws Exception {
    when(connection.getSqlDialect()).thenReturn("table");
    AbstractCli.setFsWriteMode(AbstractCli.FS_WRITE_MODE_DISABLED);

    FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
    shell.execute("mkdir /db1");

    org.junit.Assert.assertTrue(err.toString().contains("Read-only file system"));
  }

  @Test
  public void createFilesystemShellUsesTreeProviderForTreeDialect() throws Exception {
    when(connection.getSqlDialect()).thenReturn("tree");
    mockSingleColumnQuery("SHOW DATABASES", "Database", "root.sg");
    mockSingleColumnQuery("SHOW CHILD PATHS root.sg", "ChildPaths", "root.sg.d1");

    FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
    shell.execute("ls /root/sg");

    verify(statement).executeQuery("SHOW DATABASES");
    verify(statement).executeQuery("SHOW CHILD PATHS root.sg");
  }

  @Test
  public void filesystemReaderPrintsCommandErrorAndContinues() throws Exception {
    ctx.setLineReader(lineReader);
    when(lineReader.readLine("IoTDB:fs> ", null)).thenReturn("cat time");
    when(shell.execute("cat time")).thenThrow(new SQLException("550: Table does not exist"));

    boolean shouldStop = Cli.filesystemReaderReadLine(ctx, shell);

    assertFalse(shouldStop);
    verify(shell).execute("cat time");
    org.junit.Assert.assertTrue(err.toString().contains("cat: 550: Table does not exist"));
  }

  @Test
  public void filesystemHelpRunsBeforeUsernamePasswordAndConnection() throws Exception {
    assertOfflineExit("head --help", FilesystemShell.SUCCESS);
    assertTrue(out.toString().contains("head"));
    assertEquals("", err.toString());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemPwdRunsBeforeUsernamePasswordAndConnection() throws Exception {
    assertOfflineExit("pwd", FilesystemShell.SUCCESS);
    assertEquals("/" + System.lineSeparator(), out.toString());
    assertEquals("", err.toString());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemWriteCreatesLocalFileBeforeUsernamePasswordAndConnection()
      throws Exception {
    Path output = temporaryFolder.getRoot().toPath().resolve("output with spaces.tsfile");
    ByteArrayInputStream input =
        new ByteArrayInputStream("time,value\n1,42\n".getBytes(StandardCharsets.UTF_8));
    ctx = new CliContext(input, new PrintStream(out), new PrintStream(err), ExitType.EXCEPTION);

    assertOfflineExit(
        "write --table sensors --field value INT64 --stdin -o '" + output + "'",
        FilesystemShell.SUCCESS,
        "--fs_write_mode",
        "enabled");

    assertTrue(Files.exists(output));
    byte[] fileBytes = Files.readAllBytes(output);
    assertTrue(fileBytes.length > 12);
    assertEquals("TsFile", new String(fileBytes, 0, 6, StandardCharsets.US_ASCII));
    assertEquals(
        "TsFile", new String(fileBytes, fileBytes.length - 6, 6, StandardCharsets.US_ASCII));
    assertEquals("", out.toString());
    assertEquals("", err.toString());
    assertEquals(0, input.available());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemWriteReadsLocalCsvBeforeUsernamePasswordAndConnection() throws Exception {
    Path input = temporaryFolder.getRoot().toPath().resolve("input with spaces.csv");
    Path output = temporaryFolder.getRoot().toPath().resolve("sensors.tsfile");
    Files.write(input, "value,time\n42,1\n".getBytes(StandardCharsets.UTF_8));

    assertOfflineExit(
        "write --table sensors --field value INT64 -i '" + input + "' -o '" + output + "'",
        FilesystemShell.SUCCESS,
        "--fs_write_mode",
        "enabled");

    assertTrue(Files.size(output) > 12);
    assertEquals("", out.toString());
    assertEquals("", err.toString());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemReadOnlyWriteRejectsBeforeConsumingInputOrConnecting() throws Exception {
    Path output = temporaryFolder.getRoot().toPath().resolve("sensors.tsfile");
    byte[] csv = "time,value\n1,42\n".getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream input = new ByteArrayInputStream(csv);
    ctx = new CliContext(input, new PrintStream(out), new PrintStream(err), ExitType.EXCEPTION);

    assertOfflineExit(
        "write --table sensors --field value INT64 --stdin -o '" + output + "'",
        FilesystemShell.RUNTIME_ERROR);

    assertFalse(Files.exists(output));
    assertEquals(csv.length, input.available());
    assertEquals("", out.toString());
    assertTrue(err.toString().contains("Read-only file system"));
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemWriteMissingInputFailsWithoutConnecting() throws Exception {
    Path input = temporaryFolder.getRoot().toPath().resolve("missing.csv");
    Path output = temporaryFolder.getRoot().toPath().resolve("sensors.tsfile");

    assertOfflineExit(
        "write --table sensors --field value INT64 -i '" + input + "' -o '" + output + "'",
        FilesystemShell.INPUT_ERROR,
        "--fs_write_mode",
        "enabled");

    assertFalse(Files.exists(output));
    assertEquals("", out.toString());
    assertTrue(err.size() > 0);
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemExitRunsBeforeUsernamePasswordAndConnection() throws Exception {
    assertOfflineExit("exit", FilesystemShell.SUCCESS);
    assertEquals("", out.toString());
    assertEquals("", err.toString());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemSqlPassthroughIsRejectedBeforeUsernamePasswordAndConnection()
      throws Exception {
    assertOfflineExit("sql SELECT * FROM root.sg.d1", FilesystemShell.USAGE_ERROR);
    assertEquals("", out.toString());
    assertTrue(err.toString().contains("Unsupported filesystem command"));
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemCommandHelpRunsBeforeUsernamePasswordAndConnection() throws Exception {
    assertOfflineExit("help ls", FilesystemShell.SUCCESS);
    assertTrue(out.toString().contains("Usage: ls"));
    assertTrue(out.toString().contains("Examples:"));
    assertEquals("", err.toString());
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemUsageErrorRunsBeforeUsernamePasswordAndConnection() throws Exception {
    assertOfflineExit("unknown", FilesystemShell.USAGE_ERROR);
    assertEquals("", out.toString());
    assertTrue(err.toString().contains("unknown"));
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemMalformedArgumentsRunBeforeUsernamePasswordAndConnection()
      throws Exception {
    assertOfflineExit("pwd /extra", FilesystemShell.USAGE_ERROR);
    assertEquals("", out.toString());
    assertTrue(err.toString().contains("pwd"));
    assertNull(ctx.getLineReader());
  }

  @Test
  public void filesystemHelpDoesNotIgnoreInvalidStartupOptions() throws Exception {
    try {
      Cli.runCli(
          ctx, new String[] {"--access_mode", "filesystem", "--invalid", "-e", "head --help"});
      fail("Expected CLI exit");
    } catch (RuntimeException e) {
      assertEquals("Exiting with code 1", e.getMessage());
    } finally {
      AbstractCli.hasExecuteSQL = false;
      AbstractCli.setAccessMode(AbstractCli.ACCESS_MODE_SQL);
    }
    assertNull(ctx.getLineReader());
    assertFalse(out.toString().contains("head -n 5 /db1/table1.csv"));
  }

  private void assertOfflineExit(String command, int status, String... options) throws Exception {
    List<String> arguments = new ArrayList<>(Arrays.asList("--access_mode", "filesystem"));
    arguments.addAll(Arrays.asList(options));
    arguments.add("-e");
    arguments.add(command);
    try {
      Cli.runCli(ctx, arguments.toArray(new String[0]));
      fail("Expected CLI exit");
    } catch (RuntimeException e) {
      assertEquals("Exiting with code " + status, e.getMessage());
    } finally {
      AbstractCli.hasExecuteSQL = false;
      AbstractCli.setAccessMode(AbstractCli.ACCESS_MODE_SQL);
      AbstractCli.setFsWriteMode(AbstractCli.FS_WRITE_MODE_DISABLED);
    }
  }

  private void mockSingleColumnQuery(String sql, String column, String value) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(sql)).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnLabel(1)).thenReturn(column);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn(value);
  }
}
