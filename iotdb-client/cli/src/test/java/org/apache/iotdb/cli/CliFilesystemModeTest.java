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
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CliFilesystemModeTest {

  @Mock private IoTDBConnection connection;
  @Mock private Statement statement;
  @Mock private ResultSet resultSet;
  @Mock private ResultSetMetaData metaData;
  @Mock private FilesystemShell shell;
  @Mock private LineReader lineReader;

  private CliContext ctx;
  private ByteArrayOutputStream out;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    out = new ByteArrayOutputStream();
    ctx = new CliContext(System.in, new PrintStream(out), System.err, ExitType.EXCEPTION);
  }

  @Test
  public void createFilesystemShellUsesTableProviderForTableDialect() throws Exception {
    when(connection.getSqlDialect()).thenReturn("table");
    mockSingleColumnQuery("SHOW TABLES FROM db1", "TableName", "table1");

    FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
    shell.execute("ls /db1");

    verify(statement).executeQuery("SHOW TABLES FROM db1");
  }

  @Test
  public void createFilesystemShellUsesTreeProviderForTreeDialect() throws Exception {
    when(connection.getSqlDialect()).thenReturn("tree");
    mockSingleColumnQuery("SHOW CHILD PATHS root.sg", "ChildPaths", "root.sg.d1");

    FilesystemShell shell = Cli.createFilesystemShell(ctx, connection);
    shell.execute("ls /root/sg");

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
    org.junit.Assert.assertTrue(out.toString().contains("cat: 550: Table does not exist"));
  }

  private void mockSingleColumnQuery(String sql, String column, String value) throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(sql)).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnLabel(1)).thenReturn(column);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn(value);
  }
}
