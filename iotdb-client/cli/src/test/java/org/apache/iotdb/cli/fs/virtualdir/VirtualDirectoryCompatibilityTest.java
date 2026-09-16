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

package org.apache.iotdb.cli.fs.virtualdir;

import org.apache.iotdb.cli.fs.FilesystemShell;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.TableFilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.TreeFilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VirtualDirectoryCompatibilityTest {

  @Test
  public void tableCommandsKeepUpstreamResultsForCanonicalAndVirtualPaths() throws Exception {
    SqlExecutor executor = mock(SqlExecutor.class);
    when(executor.query("SHOW DATABASES")).thenReturn(SqlRow.list(SqlRow.of("Database", "db")));
    when(executor.query("SHOW TABLES FROM db"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "sensors")));
    when(executor.query("SHOW TABLES DETAILS FROM db"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "sensors", "TTL(ms)", "INF")));
    when(executor.query("DESC db.sensors DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "time", "DataType", "TIMESTAMP", "Category", "TIME"),
                SqlRow.of("ColumnName", "site", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "value", "DataType", "INT64", "Category", "FIELD")));
    when(executor.query("SELECT * FROM db.sensors ORDER BY time ASC, site ASC NULLS LAST"))
        .thenReturn(SqlRow.list(SqlRow.of("time", "1", "site", "north", "value", "42")));
    TableFilesystemSchemaProvider upstream = new TableFilesystemSchemaProvider(executor);
    VirtualDirectorySchemaProvider wrapped =
        new VirtualDirectorySchemaProvider(
            upstream, VirtualDirectoryResolvers.forTable(executor, upstream));

    for (String command :
        Arrays.asList(
            "cat -f csv",
            "schema -f csv",
            "meta -f csv",
            "stats -f csv",
            "count -f csv",
            "head --start 0 -m value -f ndjson --tag-filter site eq north")) {
      String expected = run(upstream, command + " /db/sensors.csv");
      assertFalse(expected.isEmpty());
      assertEquals(expected, run(wrapped, command + " /db/sensors.csv"));
      assertEquals(expected, run(wrapped, command + " /.virtual/by-database/db/sensors.csv"));
      assertEquals(expected, run(wrapped, command + " /.virtual/by-table/sensors/db/sensors.csv"));
    }
    String listing = run(wrapped, "ls /.virtual/by-table/sensors/db");
    assertTrue(listing.contains("sensors.csv"));
    assertTrue(listing.contains("sensors.meta"));
    assertFalse(listing.contains("sensors.schema"));
  }

  @Test
  public void treeCommandsKeepUpstreamResultsAcrossEveryVirtualResolver() throws Exception {
    SqlExecutor executor = mock(SqlExecutor.class);
    when(executor.query("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "root.sg")));
    when(executor.query("SHOW TIMESERIES root.sg.d1.s1"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "Timeseries",
                    "root.sg.d1.s1",
                    "DataType",
                    "INT64",
                    "Encoding",
                    "RLE",
                    "Compression",
                    "LZ4")));
    when(executor.query("SELECT s1 FROM root.sg.d1 ORDER BY time ASC"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "root.sg.d1.s1", "42")));
    when(executor.query("SELECT * FROM root.sg.d1 ORDER BY time ASC"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "root.sg.d1.s1", "42")));
    TreeFilesystemSchemaProvider upstream = new TreeFilesystemSchemaProvider(executor);
    VirtualDirectorySchemaProvider wrapped =
        new VirtualDirectorySchemaProvider(
            upstream, VirtualDirectoryResolvers.forTree(executor, upstream));

    for (String command :
        Arrays.asList(
            "cat -f csv",
            "schema -f csv",
            "meta -f csv",
            "stats -f csv",
            "count -f csv",
            "head --start 0 -m s1 -f ndjson")) {
      String expected = run(upstream, command + " /root/sg/d1/s1");
      assertFalse(expected.isEmpty());
      assertEquals(expected, run(wrapped, command + " /root/sg/d1/s1"));
      for (String path :
          Arrays.asList(
              "by-database/root.sg/d1/s1",
              "by-measurement/s1/root.sg.d1.s1",
              "by-tag/unit/c/root.sg.d1.s1",
              "by-attribute/owner/ops/root.sg.d1.s1")) {
        assertEquals(expected, run(wrapped, command + " /.virtual/" + path));
      }
    }
  }

  @Test
  public void sqlEscapeStillUsesUpstreamProvider() throws Exception {
    SqlExecutor executor = mock(SqlExecutor.class);
    when(executor.executeQueryOrUpdate("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "db")));
    TableFilesystemSchemaProvider upstream = new TableFilesystemSchemaProvider(executor);
    VirtualDirectorySchemaProvider wrapped =
        new VirtualDirectorySchemaProvider(upstream, Collections.emptyList());
    assertEquals(run(upstream, "sql SHOW DATABASES"), run(wrapped, "sql SHOW DATABASES"));
    assertEquals("table", wrapped.model());
  }

  private static String run(FilesystemSchemaProvider provider, String command) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream errors = new ByteArrayOutputStream();
    FilesystemShell shell =
        new FilesystemShell(
            new CliContext(
                new ByteArrayInputStream(new byte[0]),
                new PrintStream(output),
                new PrintStream(errors),
                ExitType.EXCEPTION),
            provider);
    int status = shell.runNonInteractive(command);
    assertEquals(command + ": " + errors.toString(), FilesystemShell.SUCCESS, status);
    return output.toString();
  }
}
