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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Guards existing query output while the export lifecycle evolves. */
public class ExportDataRegressionTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void treeCsvKeepsHeaderNullValuesAndSplits() throws Exception {
    SessionDataSet dataSet = treeDataSet();
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("csv", false);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 3));
    }

    assertEquals("Time,root.sg.d.s\r\n100,11\r\n200,\r\n", read("dump3_0.csv"));
    assertEquals("Time,root.sg.d.s\r\n300,33\r\n", read("dump3_1.csv"));
    verify(dataSet).close();
  }

  @Test
  public void treeSqlKeepsTimestampAndSkipsRowsWithoutValues() throws Exception {
    SessionDataSet dataSet = treeDataSet();
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("sql", false);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 0));
      // The row at 200 has no value, so it writes no statement and is not counted.
      assertEquals(2, progress.rows());
    }

    assertEquals(
        "INSERT INTO root.sg.d(TIMESTAMP,s) VALUES (100,11);\n"
            + "INSERT INTO root.sg.d(TIMESTAMP,s) VALUES (300,33);\n",
        read("dump0_0.sql"));
    assertFalse(Files.exists(temporaryFolder.getRoot().toPath().resolve("dump0_1.sql")));
  }

  @Test
  public void treeSqlCountsAMultiDeviceRowOnceAndKeepsItInOneFile() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames())
        .thenReturn(new ArrayList<>(Arrays.asList("Time", "root.sg.d1.s", "root.sg.d2.s")));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(Arrays.asList("INT64", "INT32", "INT32"));
    AtomicInteger cursor = new AtomicInteger(-1);
    when(iterator.next()).thenAnswer(call -> cursor.incrementAndGet() < 3);
    when(iterator.getLong(1)).thenAnswer(call -> (cursor.get() + 1L) * 100);
    when(iterator.getString(2)).thenAnswer(call -> "1" + cursor.get());
    when(iterator.getString(3)).thenAnswer(call -> "2" + cursor.get());
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("sql", false);
    StringWriter log = new StringWriter();
    try (ExportProgressReporter progress = progress(log);
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 0));
      assertEquals(3, progress.rows());
    }

    // -lpf 2 counts data rows, so the second row's statements share the first file.
    assertEquals(
        "INSERT INTO root.sg.d1(TIMESTAMP,s) VALUES (100,10);\n"
            + "INSERT INTO root.sg.d2(TIMESTAMP,s) VALUES (100,20);\n"
            + "INSERT INTO root.sg.d1(TIMESTAMP,s) VALUES (200,11);\n"
            + "INSERT INTO root.sg.d2(TIMESTAMP,s) VALUES (200,21);\n",
        read("dump0_0.sql"));
    assertEquals(
        "INSERT INTO root.sg.d1(TIMESTAMP,s) VALUES (300,12);\n"
            + "INSERT INTO root.sg.d2(TIMESTAMP,s) VALUES (300,22);\n",
        read("dump0_1.sql"));
    String content = log.toString();
    assertTrue(content, content.contains(ExportDataFeatureTest.member("file_rows", 2)));
    assertTrue(content, content.contains(ExportDataFeatureTest.member("file_rows", 1)));
    assertTrue(content, content.contains(ExportDataFeatureTest.member("task_rows", 3)));
  }

  @Test
  public void treeSqlGroupsInterleavedColumnsByDevice() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames())
        .thenReturn(
            new ArrayList<>(Arrays.asList("Time", "root.sg.d1.a", "root.sg.d2.x", "root.sg.d1.b")));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList())
        .thenReturn(Arrays.asList("INT64", "INT32", "INT32", "INT32"));
    when(iterator.next()).thenReturn(true, false);
    when(iterator.getLong(1)).thenReturn(100L);
    when(iterator.getString(2)).thenReturn("1");
    when(iterator.getString(3)).thenReturn("2");
    when(iterator.getString(4)).thenReturn("3");
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("sql", false);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 0));
    }

    assertEquals(
        "INSERT INTO root.sg.d1(TIMESTAMP,a,b) VALUES (100,1,3);\n"
            + "INSERT INTO root.sg.d2(TIMESTAMP,x) VALUES (100,2);\n",
        read("dump0_0.sql"));
  }

  @Test
  public void treeSqlLeavesOutNullColumnsOfADevice() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames())
        .thenReturn(
            new ArrayList<>(Arrays.asList("Time", "root.sg.d.a", "root.sg.d.b", "root.sg.d.c")));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList())
        .thenReturn(Arrays.asList("INT64", "INT32", "INT32", "INT32"));
    String[][] rows = {{"1", null, "3"}, {null, null, "6"}, {"7", "8", "9"}};
    AtomicInteger cursor = new AtomicInteger(-1);
    when(iterator.next()).thenAnswer(call -> cursor.incrementAndGet() < rows.length);
    when(iterator.getLong(1)).thenAnswer(call -> (cursor.get() + 1L) * 100);
    when(iterator.isNull(anyInt()))
        .thenAnswer(call -> rows[cursor.get()][(int) call.getArgument(0) - 2] == null);
    when(iterator.getString(anyInt()))
        .thenAnswer(call -> rows[cursor.get()][(int) call.getArgument(0) - 2]);
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("sql", false);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 0));
    }

    assertEquals(
        "INSERT INTO root.sg.d(TIMESTAMP,a,c) VALUES (100,1,3);\n"
            + "INSERT INTO root.sg.d(TIMESTAMP,c) VALUES (200,6);\n",
        read("dump0_0.sql"));
    assertEquals(
        "INSERT INTO root.sg.d(TIMESTAMP,a,b,c) VALUES (300,7,8,9);\n", read("dump0_1.sql"));
  }

  @Test
  public void treeSqlAlignedByDeviceNamesEachRowsDevice() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames())
        .thenReturn(new ArrayList<>(Arrays.asList("Time", "Device", "s1", "s2")));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(Arrays.asList("INT64", "TEXT", "INT32", "TEXT"));
    AtomicInteger cursor = new AtomicInteger(-1);
    when(iterator.next()).thenAnswer(call -> cursor.incrementAndGet() < 2);
    when(iterator.getLong(1)).thenAnswer(call -> (cursor.get() + 1L) * 100);
    when(iterator.getString(2)).thenAnswer(call -> "root.sg.d" + (cursor.get() + 1));
    when(iterator.getString(3)).thenAnswer(call -> String.valueOf(cursor.get()));
    when(iterator.isNull(4)).thenAnswer(call -> cursor.get() == 1);
    when(iterator.getString(4)).thenReturn("a\"b");
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("sql", false);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(task(config, 0));
    }

    assertEquals(
        "INSERT INTO root.sg.d1(TIMESTAMP,s1,s2) ALIGNED VALUES (100,0,'a\"b');\n"
            + "INSERT INTO root.sg.d2(TIMESTAMP,s1,s2) ALIGNED VALUES (200,1,NULL);\n",
        read("dump0_0.sql"));
  }

  @Test
  public void tableCsvKeepsColumnOrderAndNullValues() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames()).thenReturn(Arrays.asList("time", "value"));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(Arrays.asList("TIMESTAMP", "INT32"));
    when(iterator.next()).thenReturn(true, true, true, false);
    when(iterator.getLong(1)).thenReturn(100L, 200L, 300L);
    when(iterator.isNull(2)).thenReturn(false, true, false);
    when(iterator.getString(2)).thenReturn("11", "33");
    ITableSession session = mock(ITableSession.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    ExportConfig config = config("csv", true);
    try (ExportProgressReporter progress = progress();
        ExportDataTable exporter = new ExportDataTable(config, progress, session)) {
      exporter.export(
          new ExportTask(
              "select * from metrics", config.targetDirectory.resolve("dump0"), null, "metrics"));
    }

    assertEquals("time,value\r\n100,11\r\n200,\r\n", read("dump0_0.csv"));
    assertEquals("time,value\r\n300,33\r\n", read("dump0_1.csv"));
    verify(dataSet).close();
  }

  private SessionDataSet treeDataSet() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.getColumnNames())
        .thenReturn(new ArrayList<>(Arrays.asList("Time", "root.sg.d.s")));
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(Arrays.asList("INT64", "INT32"));
    AtomicInteger cursor = new AtomicInteger(-1);
    when(iterator.next()).thenAnswer(call -> cursor.incrementAndGet() < 3);
    when(iterator.getLong(1)).thenAnswer(call -> (cursor.get() + 1L) * 100);
    when(iterator.isNull(2)).thenAnswer(call -> cursor.get() == 1);
    when(iterator.getString(2))
        .thenAnswer(call -> cursor.get() == 0 ? "11" : cursor.get() == 1 ? null : "33");
    return dataSet;
  }

  private String read(String name) throws Exception {
    return Files.readString(temporaryFolder.getRoot().toPath().resolve(name));
  }

  private ExportConfig config(String format, boolean table) throws Exception {
    List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "-ft",
                format,
                "-t",
                temporaryFolder.getRoot().toString(),
                "-lpf",
                "2",
                "-tf",
                "timestamp"));
    if (table) {
      args.addAll(Arrays.asList("-sql_dialect", "table", "-db", "db"));
    }
    return ExportConfig.parse(args.toArray(new String[0]), ZonedDateTime.now());
  }

  private ExportProgressReporter progress() {
    return progress(new StringWriter());
  }

  private ExportProgressReporter progress(StringWriter log) {
    return new ExportProgressReporter(
        log, "tree", System.nanoTime(), new PrintStream(OutputStream.nullOutputStream()));
  }

  private ExportTask task(ExportConfig config, int index) {
    return new ExportTask(
        "select s from root.sg.d", config.targetDirectory.resolve("dump" + index), null, null);
  }
}
