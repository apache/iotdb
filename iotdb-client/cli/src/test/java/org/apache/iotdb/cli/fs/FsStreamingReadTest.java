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

import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor.RowVisitor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FsStreamingReadTest {
  @Test
  public void filteredOffsetPageStopsAfterLastRequestedRow() throws Exception {
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    when(provider.model()).thenReturn("tree");
    when(provider.columns(any()))
        .thenReturn(
            Arrays.asList(
                new FsColumn("time", "TIME", "TIMESTAMP"), new FsColumn("x", "FIELD", "INT32")));
    int[] visits = {0};
    doAnswer(
            invocation -> {
              RowVisitor visitor = invocation.getArgument(3);
              for (int i = 0; i < 1000000; i++) {
                visits[0]++;
                if (!visitor.visit(
                    SqlRow.of("time", Integer.toString(i), "x", Integer.toString(i)))) break;
              }
              return null;
            })
        .when(provider)
        .scan(any(), any(), eq(false), any());
    ReadOptions options =
        new ReadOptions(
            "ndjson",
            "",
            "",
            Collections.singletonList("x"),
            2,
            3,
            10L,
            20L,
            Collections.emptyList(),
            "all");
    List<SqlRow> rows =
        new FsRowReader(provider).read(FsPath.absolute("/root/d"), options, false).getRows();
    assertEquals(15, visits[0]);
    assertEquals("13", rows.get(0).get("time"));
    assertEquals("14", rows.get(1).get("time"));
    verify(provider, never()).read(any(FsPath.class), anyInt());
  }

  @Test
  public void descendingPageReversesOnlySelectedRows() throws Exception {
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    when(provider.model()).thenReturn("tree");
    when(provider.columns(any()))
        .thenReturn(
            Arrays.asList(
                new FsColumn("time", "TIME", "TIMESTAMP"), new FsColumn("x", "FIELD", "INT32")));
    doAnswer(
            invocation -> {
              RowVisitor visitor = invocation.getArgument(3);
              for (int i = 100; i >= 0; i--)
                if (!visitor.visit(
                    SqlRow.of("time", Integer.toString(i), "x", Integer.toString(i)))) break;
              return null;
            })
        .when(provider)
        .scan(any(), any(), eq(true), any());
    ReadOptions options =
        new ReadOptions(
            "ndjson",
            "",
            "",
            Collections.emptyList(),
            3,
            2,
            80L,
            90L,
            Collections.emptyList(),
            "all");
    List<SqlRow> rows =
        new FsRowReader(provider).read(FsPath.absolute("/root/d"), options, true).getRows();
    assertEquals("86", rows.get(0).get("time"));
    assertEquals("88", rows.get(2).get("time"));
  }

  @Test
  public void streamedFormatsPreserveRenderingIncludingWideTableCells() throws Exception {
    List<FsColumn> columns =
        Arrays.asList(
            new FsColumn("time", "TIME", "TIMESTAMP"), new FsColumn("text", "FIELD", "STRING"));
    List<SqlRow> rows =
        Arrays.asList(
            SqlRow.of("time", "-1", "text", "中文,\nquoted\""),
            SqlRow.of("time", "2", "text", null),
            SqlRow.of("time", "3", "text", "x".repeat(70000)));
    for (String format : Arrays.asList("table", "csv", "ndjson")) {
      ByteArrayOutputStream expected = new ByteArrayOutputStream(),
          actual = new ByteArrayOutputStream();
      FsRowRenderer.print(new PrintStream(expected), columns, rows, format);
      FsRowRenderer.printStream(
          new PrintStream(actual),
          columns,
          format,
          visitor -> {
            for (SqlRow row : rows) visitor.visit(row);
          });
      assertArrayEquals(expected.toByteArray(), actual.toByteArray());
    }
  }
}
