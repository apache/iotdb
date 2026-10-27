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
package org.apache.iotdb.tools.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Runs the actual export command and verifies files, exit status and round trips. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class ExportDataMeasurementIT {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void csvPatternsSplitsPrefixTimeAndEmptyMeasurements() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.export_csv.d.pre1 WITH DATATYPE=INT32");
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.export_csv.d.pre2 WITH DATATYPE=TEXT");
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.export_csv.d.empty WITH DATATYPE=INT32");
      for (int t = 100; t <= 300; t += 100) {
        session.executeNonQueryStatement(
            "INSERT INTO root.export_csv.d(time,pre1,pre2) VALUES(" + t + "," + t + ",'温度,计')");
      }
    }
    Path input =
        file(
            "\uFEFF# patterns\nroot.export_csv.d.pre*\nroot.export_csv.*.*\nroot.export_csv.**\nroot.missing.**\nroot.export_csv.d.pre*\n");
    Path target = folder.newFolder("csv output").toPath();
    Result result =
        export(
            target,
            "-ft",
            "csv",
            "-mf",
            input.toString(),
            "-pfn",
            "backup",
            "-lpf",
            "1",
            "-tf",
            "timestamp",
            "-start_time",
            "100",
            "-end_time",
            "200");
    assertSuccess(result);
    assertEquals(
        "Time,root.export_csv.d.pre1\r\n100,100\r\n",
        Files.readString(target.resolve("backup_root.export_csv.d.pre1_0.csv")));
    assertEquals(
        "Time,root.export_csv.d.pre1\r\n200,200\r\n",
        Files.readString(target.resolve("backup_root.export_csv.d.pre1_1.csv")));
    assertEquals(4, outputs(target, ".csv").size());
    String log = log(target);
    assertTrue(log, log.contains(member("total_measurement_num", 3)));
    assertTrue(log, log.contains(member("exported_measurement_num", 3)));
    assertTrue(log, log.contains(member("zero_row_tasks", 1)));
    assertTrue(log, log.contains(member("exported_percent", "100.00%")));
    assertTrue(log, log.contains(member("event", "WARNING")));
    assertTrue(log, log.contains(member("line", 5)));
    assertTrue(log, log.contains(member("exported_data_rows", 4)));
    // The console is for people: the warning and the summary, never the JSON events.
    assertTrue(
        result.text,
        result
            .text
            .lines()
            .anyMatch(
                "WARNING: No measurements matched root.missing.** (measurement file line 5)."
                    ::equals));
    assertTrue(
        result.text,
        result.text.contains("Export completely! Measurements: 3/3, rows: 4, files: 4, elapsed: "));
    assertFalse(result.text, result.text.contains("{\""));
  }

  @Test
  public void sqlAndTsFileRoundTripAlignedTypesAndQuotedPaths() throws Exception {
    for (String format : Arrays.asList("sql", "tsfile")) {
      String database = "root.export_" + format;
      String d = database + ".d";
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            "CREATE ALIGNED TIMESERIES "
                + d
                + "(s INT32,txt TEXT,stamp TIMESTAMP,day DATE,blob BLOB,`a.b` STRING)");
        session.executeNonQueryStatement(
            "CREATE TIMESERIES " + database + ".plain.n WITH DATATYPE=DOUBLE");
        session.executeNonQueryStatement(
            "INSERT INTO "
                + d
                + "(time,s,txt,stamp,day,blob,`a.b`) ALIGNED VALUES(100,10,'温度',123,'2026-10-14',X'CAFE','quoted'),(200,20,'温度2',456,'2026-10-15',X'BEEF','quoted2'),(300,30,'外部',789,'2026-10-16',X'ABCD','outside')");
        session.executeNonQueryStatement(
            "INSERT INTO " + database + ".plain(time,n) VALUES(100,1.5),(200,2.5),(300,3.5)");
      }
      Path target = folder.newFolder(format).toPath();
      Path input = file(database + ".**\n");
      assertSuccess(
          export(
              target,
              "-ft",
              format,
              "-mf",
              input.toString(),
              "-start_time",
              "100",
              "-end_time",
              "200"));
      List<Path> files = outputs(target, "." + format);
      assertEquals(7, files.size());
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            format.equals("sql")
                ? "DELETE FROM " + database + ".** WHERE time >= 0"
                : "DELETE DATABASE " + database);
        for (Path path : files) {
          if (format.equals("sql")) {
            for (String sql : Files.readAllLines(path)) {
              session.executeNonQueryStatement(sql);
            }
          } else {
            session.executeNonQueryStatement(
                "LOAD '" + path.toAbsolutePath() + "' WITH ('verify'='true')");
          }
        }
        try (SessionDataSet data =
            session.executeQueryStatement("SELECT s,txt,stamp,day,blob,`a.b` FROM " + d)) {
          SessionDataSet.DataIterator it = data.iterator();
          assertTrue(it.next());
          assertEquals(100, it.getLong(1));
          assertEquals(10, it.getInt(2));
          assertEquals("温度", it.getString(3));
          assertEquals(123, it.getLong(4));
          assertEquals("2026-10-14", it.getString(5));
          assertEquals("0xcafe", it.getString(6).toLowerCase());
          assertEquals("quoted", it.getString(7));
          assertTrue(it.next());
          assertEquals(200, it.getLong(1));
          assertEquals(20, it.getInt(2));
          assertFalse(it.next());
        }
        try (SessionDataSet data =
            session.executeQueryStatement("SELECT n FROM " + database + ".plain")) {
          assertTrue(data.hasNext());
          assertEquals(1.5, data.next().getField(0).getDoubleV(), 0.0);
          assertTrue(data.hasNext());
          assertEquals(2.5, data.next().getField(0).getDoubleV(), 0.0);
          assertFalse(data.hasNext());
        }
      }
    }
  }

  @Test
  public void originalQueryAndTableModesAndFailures() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "INSERT INTO root.export_query.d(time,s) VALUES(100,10),(200,20)");
    }
    Path query = folder.newFolder("query").toPath();
    assertSuccess(
        export(
            query,
            "-ft",
            "csv",
            "-q",
            "SELECT s FROM root.export_query.d",
            "-tf",
            "timestamp",
            "-start_time",
            "200"));
    assertTrue(Files.readString(query.resolve("dump0_0.csv")).contains("100,10"));
    String queryLog = log(query);
    assertFalse(queryLog.contains("exported_measurement_num"));
    // The query runs as written, so the ignored time range is a warning rather than a START field.
    assertTrue(queryLog, queryLog.contains(member("event", "WARNING")));
    assertTrue(
        queryLog,
        queryLog
            .lines()
            .filter(line -> line.contains(member("event", "START")))
            .noneMatch(line -> line.contains("\"start_time\":")));
    Path failure = folder.newFolder("failed").toPath();
    Result badQuery = export(failure, "-ft", "csv", "-q", "invalid sql");
    assertNotEquals(0, badQuery.code);
    assertTrue(badQuery.text, badQuery.text.contains("Export failed after "));
    assertFalse(badQuery.text, badQuery.text.contains("Export completely!"));
    assertTrue(log(failure).contains(member("status", "FAILED")));
    assertFalse(log(failure).contains(member("status", "SUCCESS")));
    Path empty = folder.newFolder("unmatched").toPath();
    assertNotEquals(
        0, export(empty, "-ft", "csv", "-mf", file("root.no_measurements.**\n").toString()).code);
    assertNotEquals(
        0, export(folder.newFolder("alias").toPath(), "-ft", "csv", "-f", "backup").code);
    assertNotEquals(
        0,
        export(
                folder.newFolder("badfile").toPath(),
                "-ft",
                "csv",
                "-mf",
                file("root.export_query.d.s\ninvalid..path\n").toString())
            .code);
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE export_table");
      session.executeNonQueryStatement("USE export_table");
      session.executeNonQueryStatement(
          "CREATE TABLE metrics(id STRING TAG, v INT32 FIELD, stamp TIMESTAMP FIELD, day DATE FIELD, blob BLOB FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO metrics(time,id,v,stamp,day,blob) VALUES(100,'d1',10,123,'2026-10-14',X'CAFE'),(200,'d1',20,456,'2026-10-15',X'BEEF'),(300,'d1',30,789,'2026-10-16',X'ABCD')");
    }
    for (String format : Arrays.asList("csv", "sql", "tsfile")) {
      Path target = folder.newFolder("table_" + format).toPath();
      assertSuccess(
          export(
              target,
              "-sql_dialect",
              "table",
              "-db",
              "export_table",
              "-ft",
              format,
              "-start_time",
              "100",
              "-end_time",
              "200"));
      assertTrue(log(target).contains(member("exported_data_rows", 2)));
      assertFalse(log(target).contains("exported_measurement_num"));
      assertEquals(1, outputs(target, "." + format).size());
      // SQL and TsFile output take the table from the query.
      Path custom = folder.newFolder("table_query_" + format).toPath();
      assertSuccess(
          export(
              custom,
              "-sql_dialect",
              "table",
              "-db",
              "export_table",
              "-ft",
              format,
              "-q",
              "SELECT * FROM metrics WHERE time=100"));
      assertTrue(log(custom).contains(member("exported_data_rows", 1)));
    }
    // SQL and TsFile rows are written back to the table, so neither computed values nor rows
    // without a FIELD column are exported.
    for (String format : Arrays.asList("sql", "tsfile")) {
      for (String sql :
          Arrays.asList(
              "SELECT time, id, v + 1 AS v, stamp, day, blob FROM metrics",
              "SELECT time, id FROM metrics")) {
        Path rejected = folder.newFolder().toPath();
        Result result =
            export(
                rejected, "-sql_dialect", "table", "-db", "export_table", "-ft", format, "-q", sql);
        assertNotEquals(result.text, 0, result.code);
        assertTrue(result.text, outputs(rejected, "." + format).isEmpty());
      }
    }
    assertNotEquals(
        0,
        export(
                folder.newFolder("table_mf").toPath(),
                "-sql_dialect",
                "table",
                "-db",
                "export_table",
                "-ft",
                "csv",
                "-mf",
                file("root.**\n").toString())
            .code);
  }

  @Test
  public void cancellationHasOneTerminalEvent() throws Exception {
    org.junit.Assume.assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("windows"));
    StringBuilder patterns = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      patterns.append("root.cancel.d").append(i).append(".s\n");
    }
    Path input = file(patterns.toString());
    Path target = folder.newFolder("cancelled").toPath();
    Path console = folder.newFile().toPath();
    List<String> command = command(target, "-ft", "csv", "-mf", input.toString());
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(console.toFile())
            .start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (!Files.readString(console).contains("Export started")
          && System.nanoTime() < deadline
          && process.isAlive()) {
        Thread.sleep(25);
      }
      assertTrue(Files.readString(console), process.isAlive());
      process.destroy();
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      String output = Files.readString(console);
      assertEquals(output, 1, output.split("Export cancelled after ", -1).length - 1);
      assertFalse(output, output.contains("Export completely!"));
      String log = log(target);
      assertEquals(log, 1, log.split(member("event", "FINISH"), -1).length - 1);
      assertTrue(log, log.contains(member("status", "CANCELLED")));
      // destroy() sends SIGTERM; the process exits with its status, which FINISH reports.
      assertEquals(output, 143, process.exitValue());
      assertTrue(log, log.contains(member("exit_code", 143)));
      // The worker closes its own session, so no transport is closed under a request.
      assertFalse(output, output.contains("SocketException"));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  @Test
  public void benchmarkTwentyThousandMeasurements() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean("export.measurements.benchmark"));
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
    }
    StringBuilder patterns = new StringBuilder();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int d = 0; d < 200; d++) {
        String device = "root.export_bench.d" + d;
        Tablet tablet = new Tablet(device, schemas, 1440);
        for (int row = 0; row < 1440; row++) {
          tablet.addTimestamp(row, row * 60000L);
          for (int s = 0; s < 100; s++) {
            tablet.addValue("s" + s, row, row);
          }
        }
        session.insertTablet(tablet);
        for (int s = 0; s < 100; s++) {
          patterns.append(device).append(".s").append(s).append('\n');
        }
      }
    }
    Path input = file(patterns.toString());
    Path target = folder.newFolder("benchmark").toPath();
    long started = System.nanoTime();
    assertSuccess(export(target, "-ft", "csv", "-mf", input.toString(), "-tf", "timestamp"));
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    List<Path> outputs = outputs(target, ".csv");
    assertEquals(20000, outputs.size());
    long rows = 0;
    for (Path output : outputs) {
      try (Stream<String> lines = Files.lines(output)) {
        rows += lines.count() - 1;
      }
    }
    assertEquals(28800000, rows);
    String log = log(target);
    assertTrue(log.contains(member("exported_measurement_num", 20000)));
    assertTrue(log.contains(member("exported_data_rows", 28800000)));
    System.out.println(
        "export_measurement_benchmark devices=200 measurements=20000 points="
            + rows
            + " elapsed_ms="
            + elapsedMillis);
    Files.writeString(
        Path.of("target/export-measurement-benchmark.txt"),
        "devices=200 measurements=20000 points=" + rows + " elapsed_ms=" + elapsedMillis + "\n");
  }

  private Result export(Path target, String... args) throws Exception {
    List<String> command = command(target, args);
    Path console = folder.newFile().toPath();
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(console.toFile())
            .start();
    try {
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        // A benchmark may legitimately take longer; regular tests retain the bounded wait.
        if (Boolean.getBoolean("export.measurements.benchmark")) {
          while (!process.waitFor(60, TimeUnit.SECONDS)) {
            System.out.println("Export benchmark is running.");
          }
        } else {
          fail("Export command timed out: " + Files.readString(console));
        }
      }
      return new Result(process.exitValue(), Files.readString(console));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  private List<String> command(Path target, String... args) {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-cp");
    command.add(EnvFactory.getEnv().getLibPath());
    command.add("org.apache.iotdb.tool.data.ExportData");
    command.addAll(
        Arrays.asList(
            "-h",
            EnvFactory.getEnv().getIP(),
            "-p",
            EnvFactory.getEnv().getPort(),
            "-u",
            SessionConfig.DEFAULT_USER,
            "-pw",
            SessionConfig.DEFAULT_PASSWORD,
            "-t",
            target.toString()));
    command.addAll(Arrays.asList(args));
    return command;
  }

  private void assertSuccess(Result result) {
    assertEquals(result.text, 0, result.code);
    assertEquals(result.text, 1, result.text.split("Export completely!", -1).length - 1);
    assertFalse(result.text, result.text.contains("Export failed"));
  }

  /** A member as the JSON Lines log writes it, such as "status":"FAILED" or "exit_code":130. */
  private static String member(String key, Object value) {
    return '"' + key + "\":" + (value instanceof Number ? value : "\"" + value + '"');
  }

  private Path file(String content) throws Exception {
    return Files.writeString(folder.newFile().toPath(), content, StandardCharsets.UTF_8);
  }

  private List<Path> outputs(Path directory, String suffix) throws Exception {
    try (Stream<Path> files = Files.list(directory)) {
      return files
          .filter(file -> file.toString().endsWith(suffix))
          .sorted()
          .collect(Collectors.toList());
    }
  }

  private String log(Path directory) throws Exception {
    return Files.readString(outputs(directory, ".log").get(0));
  }

  private static class Result {
    final int code;
    final String text;

    Result(int code, String text) {
      this.code = code;
      this.text = text;
    }
  }
}
