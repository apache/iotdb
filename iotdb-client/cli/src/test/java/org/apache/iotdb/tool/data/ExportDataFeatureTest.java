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

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;

import org.apache.commons.cli.ParseException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.jline.reader.EndOfFileException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Input, planning and lifecycle contracts exercised without a server. */
public class ExportDataFeatureTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private static final ZonedDateTime START = ZonedDateTime.parse("2026-10-14T12:34:56+08:00");

  @Test
  public void preservesPrefixOptionsAndRejectsFAlias() throws Exception {
    ExportConfig defaults = config("-mf", "measurements.txt");
    assertEquals("", defaults.prefix);
    assertEquals("log_export_data_20261014123456.log", defaults.logFile.getFileName().toString());
    assertEquals("backup", config("-mf", "measurements.txt", "-pfn", "backup").prefix);
    assertEquals(
        "backup", config("-mf", "measurements.txt", "--prefix_file_name", "backup").prefix);
    assertEquals("dump", config("-q", "select * from root.**").prefix);
    assertThrows(ParseException.class, () -> config("-f", "backup"));
    assertThrows(ArgsErrorException.class, () -> config("-progress"));
    assertThrows(
        ArgsErrorException.class, () -> config("-mf", "file", "-q", "select s from root.sg.d"));
    assertThrows(
        ArgsErrorException.class,
        () -> config("-sql_dialect", "table", "-db", "db", "-mf", "file"));
    assertThrows(ArgsErrorException.class, () -> config("-lpf", "0"));
    assertThrows(ArgsErrorException.class, () -> config("-dt", "maybe"));
    assertThrows(ParseException.class, () -> config("-p"));
  }

  @Test
  public void acceptsBomCommentsQuotedPathsAndIoTDBPatterns() throws Exception {
    Path input =
        input(
            "\uFEFF# comment\r\n\n root.sg.d.pre* \nroot.sg.**\rroot.sg.d.`a.b`\nroot.sg.d.pre*\n");
    MeasurementFile file = MeasurementFile.read(input);
    assertEquals(6, file.lines());
    assertEquals(3, file.patterns().size());
    assertEquals(1, file.duplicates());
    assertEquals("root.sg.d.pre*", file.patterns().get(0).path().getFullPath());
    assertEquals("`a.b`", file.patterns().get(2).path().getMeasurement());
  }

  @Test
  public void reportsInvalidLineAndUtf8AtActualLine() throws Exception {
    Path input = input("root.sg.d.s\n# valid\nroot.sg..s\n");
    ArgsErrorException invalid =
        assertThrows(ArgsErrorException.class, () -> MeasurementFile.read(input));
    assertTrue(invalid.getMessage().contains("3"));
    assertTrue(invalid.getMessage().contains("root.sg..s"));
    byte[] bytes = "root.sg.d.s\n# comment\n".getBytes(StandardCharsets.UTF_8);
    byte[] bad = Arrays.copyOf(bytes, bytes.length + 2);
    bad[bytes.length] = (byte) 0xc3;
    bad[bytes.length + 1] = 0x28;
    Files.write(input, bad);
    ArgsErrorException utf8 =
        assertThrows(ArgsErrorException.class, () -> MeasurementFile.read(input));
    assertTrue(utf8.getMessage().contains("3"));
    assertThrows(ArgsErrorException.class, () -> MeasurementFile.read(input("# empty\n \n")));
    assertThrows(ArgsErrorException.class, () -> MeasurementFile.read(input("other.sg.s\n")));
  }

  @Test
  public void plansEachDistinctPatternAndDeduplicatesActualMeasurements() throws Exception {
    Path input = input("root.sg.d.pre*\nroot.sg.**\nroot.sg.d.pre*\nroot.empty.**\n");
    ExportConfig config = config("-mf", input.toString(), "-pfn", "backup");
    StringWriter log = new StringWriter();
    Session session = mock(Session.class);
    SessionDataSet first = metadata("root.sg.d.pre1");
    SessionDataSet second = metadata("root.sg.d.pre1", "root.sg.d.pre2");
    SessionDataSet empty = metadata();
    when(session.executeQueryStatement("show timeseries root.sg.d.pre*", config.timeout))
        .thenReturn(first);
    when(session.executeQueryStatement("show timeseries root.sg.**", config.timeout))
        .thenReturn(second);
    when(session.executeQueryStatement("show timeseries root.empty.**", config.timeout))
        .thenReturn(empty);
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    try (ExportProgressReporter reporter =
            new ExportProgressReporter(
                log,
                "tree",
                System.nanoTime(),
                new PrintStream(console, true, StandardCharsets.UTF_8));
        ExportDataTree exporter = new ExportDataTree(config, reporter, session)) {
      List<ExportTask> tasks =
          exporter.planMeasurements(
              MeasurementFile.read(input),
              ExportTimeRange.parse("100", "200", ZoneId.of("UTC"), "ms"));
      assertEquals(2, tasks.size());
      assertEquals(
          "SELECT pre1 FROM root.sg.d WHERE time >= 100 AND time <= 200", tasks.get(0).sql());
      assertEquals("backup_root.sg.d.pre1", tasks.get(0).outputBase().getFileName().toString());
      assertEquals("root.sg.d.pre2", tasks.get(1).measurement().path().getFullPath());
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    verify(session, times(3)).executeQueryStatement(anyString(), anyLong());
    verify(first).close();
    verify(second).close();
    verify(empty).close();
    verify(session).close();
    String content = log.toString();
    String warning = event(log, "WARNING");
    assertTrue(warning, warning.contains(member("line", 4)));
    assertTrue(warning, warning.contains(member("pattern", "root.empty.**")));
    // JSON escapes the backslashes of a Windows path.
    assertTrue(warning, warning.contains(member("source", input.toString().replace("\\", "\\\\"))));
    // The console names the unmatched pattern itself, without the long source path.
    String expected =
        String.format(
            CliMessages.LOG_WARNING_ARG_05BE32E9,
            String.format(
                CliMessages.LOG_NO_MEASUREMENTS_MATCHED_ARG_MEASUREMENT_FILE_LINE_ARG_94C4EF10,
                "root.empty.**",
                4));
    String shown = console.toString(StandardCharsets.UTF_8);
    assertTrue(shown, shown.lines().anyMatch(expected::equals));
    // Measurement fields keep the requirement's order between the rows and the elapsed time.
    assertTrue(
        content,
        content.contains(
            String.join(
                    ",",
                    member("status", "RUNNING"),
                    member("exported_data_rows", 0),
                    member("exported_measurement_num", 0),
                    member("total_measurement_num", 2),
                    member("exported_percent", "0.00%"),
                    member("estimated_remaining", "UNKNOWN"))
                + ",\"elapsed_time\":"));
    assertTrue(content, content.contains(member("duplicate_matches", 1)));
  }

  @Test
  public void failsOnlyWhenNoPatternMatches() throws Exception {
    Path input = input("root.empty.**\n");
    ExportConfig config = config("-mf", input.toString());
    Session session = mock(Session.class);
    SessionDataSet empty = metadata();
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(empty);
    try (ExportProgressReporter reporter = reporter(new StringWriter());
        ExportDataTree exporter = new ExportDataTree(config, reporter, session)) {
      assertThrows(
          ArgsErrorException.class,
          () ->
              exporter.planMeasurements(
                  MeasurementFile.read(input),
                  ExportTimeRange.parse(null, null, ZoneId.of("UTC"), "ms")));
    }
  }

  @Test
  public void timeRangesUsePrecisionTimezoneAndInclusiveBounds() throws Exception {
    ZoneId zone = ZoneId.of("Asia/Shanghai");
    assertEquals(
        " WHERE time >= 100 AND time <= 100",
        ExportTimeRange.parse("100", "100", zone, "ns").whereClause());
    assertEquals(" WHERE time <= 99", ExportTimeRange.parse(null, "99", zone, "ms").whereClause());
    assertEquals(
        " WHERE time >= 1000",
        ExportTimeRange.parse("1970-01-01T08:00:00.001", null, zone, "us").whereClause());
    assertEquals(
        " WHERE time >= 1000000",
        ExportTimeRange.parse("1970-01-01T00:00:00.001Z", null, zone, "ns").whereClause());
    assertThrows(ArgsErrorException.class, () -> ExportTimeRange.parse("200", "100", zone, "ms"));
    assertThrows(ArgsErrorException.class, () -> ExportTimeRange.parse("oops", null, zone, "ms"));
    ExportConfig config = config("-q", "select s from root.sg.d", "-start_time", "oops");
    assertEquals(
        "select s from root.sg.d", ExportData.queryTasks(config, config.query).get(0).sql());
  }

  @Test
  public void fileNamesEscapeAndRejectCollisionsAndLogAliases() throws Exception {
    ExportConfig config = config("-mf", "file");
    ExportFileNames names = new ExportFileNames();
    assertEquals(
        "root.sg.d.`a%3Ab`", names.measurement(config, "root.sg.d.`a:b`").getFileName().toString());
    names.measurement(config, "root.sg.d.A");
    assertThrows(ArgsErrorException.class, () -> names.measurement(config, "root.sg.d.a"));
    assertThrows(
        ArgsErrorException.class, () -> names.measurement(config, "root." + "x".repeat(230)));
    Path file = folder.newFile().toPath();
    Path alias = folder.getRoot().toPath().resolve("alias");
    Files.createLink(alias, file);
    assertThrows(ArgsErrorException.class, () -> ExportFileNames.checkLogCollision(alias, file));
  }

  @Test
  public void independentTimerReportsWithoutRowsAndStopsAfterOneTerminal() throws Exception {
    CountDownLatch tick = new CountDownLatch(1);
    StringWriter log =
        new StringWriter() {
          @Override
          public void write(String value) {
            super.write(value);
            if (value.contains(member("event", "PROGRESS"))) {
              tick.countDown();
            }
          }
        };
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    try (ExportProgressReporter reporter =
        new ExportProgressReporter(
            log,
            "tree",
            System.nanoTime(),
            new PrintStream(console, true, StandardCharsets.UTF_8))) {
      reporter.start(
          config("-q", "select * from root.**"), true, () -> fail("unexpected log error"));
      assertTrue(tick.await(5, TimeUnit.SECONDS));
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
      reporter.progress();
      reporter.finish(ExportProgressReporter.Status.FAILED, new IOException("later"));
    }
    String content = log.toString();
    assertTrue(
        content,
        content.startsWith(
            "{" + member("progress_id", ProcessHandle.current().pid()) + ",\"timestamp\":"));
    assertTrue(content, content.contains(member("mode", "query")));
    // The requirement's fields lead each event, in its order; event details follow them.
    assertTrue(
        content,
        Pattern.compile(
                Pattern.quote(
                        String.join(
                            ",",
                            member("model", "tree"),
                            member("status", "RUNNING"),
                            member("exported_data_rows", 0)))
                    + ",\"elapsed_time\":\"00:00:0\\d\","
                    + Pattern.quote(
                        String.join(
                            ",",
                            member("event", "PROGRESS"),
                            member("level", "INFO"),
                            member("phase", "VALIDATING"))))
            .matcher(content)
            .find());
    assertFalse(content.contains("exported_measurement_num"));
    assertEquals(1, content.split(member("event", "FINISH"), -1).length - 1);
    // Every event is one JSON object on its own line.
    content.lines().forEach(line -> assertTrue(line, line.startsWith("{") && line.endsWith("}")));

    // A query has no total, so the console shows rows, elapsed time and rate, then one summary.
    String shown = console.toString(StandardCharsets.UTF_8);
    assertTrue(
        shown, printed(shown, CliMessages.LOG_EXPORT_STARTED_ARG_ARG_TARGET_ARG_LOG_ARG_BD58AECB));
    assertTrue(shown, printed(shown, CliMessages.LOG_ARG_ROWS_ELAPSED_ARG_ARG_ROWS_S_AE3CF660));
    assertTrue(
        shown,
        printed(shown, CliMessages.LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B));
    assertFalse(shown, printed(shown, CliMessages.LOG_EXPORT_FAILED_AFTER_ARG_ARG_C123F0EF));
    assertFalse(shown, shown.contains("{"));
    // Scripts check the last line for the outcome, so the log path comes before the summary.
    List<String> lines = shown.lines().collect(Collectors.toList());
    assertTrue(
        shown,
        printed(
            lines.get(lines.size() - 1),
            CliMessages.LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B));
    assertTrue(shown, printed(lines.get(lines.size() - 2), CliMessages.LOG_LOG_ARG_FBF61F63));
  }

  @Test
  public void remainingTimeWaitsForSeveralMeasurements() throws Exception {
    StringWriter log = new StringWriter();
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    ExportConfig config = config();
    try (ExportProgressReporter reporter =
        new ExportProgressReporter(
            log,
            "tree",
            System.nanoTime(),
            new PrintStream(console, true, StandardCharsets.UTF_8))) {
      reporter.discovered(MeasurementFile.read(input("root.sg.**\n")), 40, 40, 0);
      for (int i = 0; i < 3; i++) {
        reporter.taskStarted(
            new ExportTask(
                "select s from root.sg.d" + i,
                config.targetDirectory.resolve("m" + i),
                null,
                null));
        reporter.taskCompleted();
        reporter.progress();
      }
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    List<String> progress =
        log.toString()
            .lines()
            .filter(line -> line.contains(member("event", "PROGRESS")))
            .collect(Collectors.toList());
    // 5% of 40 is two measurements, yet at least three must complete before estimating.
    assertTrue(progress.get(1), progress.get(1).contains(member("estimated_remaining", "UNKNOWN")));
    assertFalse(
        progress.get(2), progress.get(2).contains(member("estimated_remaining", "UNKNOWN")));
    List<String> shown =
        console
            .toString(StandardCharsets.UTF_8)
            .lines()
            .filter(
                line ->
                    printed(
                        line,
                        CliMessages
                            .LOG_ARG_ARG_ARG_MEASUREMENTS_ARG_ROWS_ELAPSED_ARG_ETA_ARG_CURRENT_ARG_B6DEA0BD))
            .collect(Collectors.toList());
    assertEquals(3, shown.size());
    assertTrue(shown.get(0), shown.get(0).startsWith("[2%] 1/40 "));
    assertTrue(shown.get(1), shown.get(1).contains(" --"));
    assertFalse(shown.get(2), shown.get(2).contains(" --"));
  }

  @Test
  public void terminalRedrawsOneFittedLineAndRedirectedOutputGetsLines() {
    ByteArrayOutputStream terminalBytes = new ByteArrayOutputStream();
    ExportConsole terminal =
        new ExportConsole(new PrintStream(terminalBytes, true, StandardCharsets.UTF_8), true, 30);
    terminal.progress("Exporting 10%", "root.factory.line19.s0", "unused");
    terminal.progress("Exporting 20%", "", "unused");
    terminal.line("done");
    // The tail keeps whole path nodes from the right so the line, with its last column free, fits
    // 30 columns. A shorter redraw blanks what the previous one left behind, and a lasting line
    // first erases the progress line instead of continuing it.
    assertEquals(
        "\rExporting 10%  ...line19.s0"
            + "\rExporting 20%"
            + " ".repeat(14)
            + "\r"
            + " ".repeat(13)
            + "\rdone"
            + System.lineSeparator(),
        terminalBytes.toString(StandardCharsets.UTF_8));

    ByteArrayOutputStream plainBytes = new ByteArrayOutputStream();
    ExportConsole plain =
        new ExportConsole(new PrintStream(plainBytes, true, StandardCharsets.UTF_8), false, 30);
    plain.progress("Exporting 10%", "root.factory.line19.s0", "[10%] plain");
    plain.line("done");
    assertEquals(
        "[10%] plain" + System.lineSeparator() + "done" + System.lineSeparator(),
        plainBytes.toString(StandardCharsets.UTF_8));

    // CJK characters take two columns, so cutting counts columns rather than characters.
    assertEquals(10, ExportConsole.width("导出中 59%"));
    assertEquals("...乙丙", ExportConsole.left("测点甲乙丙", 7));
    assertEquals("root.sg", ExportConsole.left("root.sg", 7));
  }

  @Test
  public void terminalTooNarrowForTheLineGetsLinesInsteadOfRedraws() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ExportConsole console =
        new ExportConsole(new PrintStream(bytes, true, StandardCharsets.UTF_8), true, 14);
    console.progress("Exporting 10%", "root.factory.line19.s0", "[10%] first");
    console.progress("Exporting 100%", "root.factory.line19.s0", "[100%] second");
    console.line("done");
    // 13 columns fit beside the free last column and are redrawn in place. A carriage return
    // reaches only the last row of a wrapped line, so the 14-column line erases the redrawn one
    // and becomes a line of its own.
    assertEquals(
        "\rExporting 10%"
            + "\r"
            + " ".repeat(13)
            + "\r"
            + "[100%] second"
            + System.lineSeparator()
            + "done"
            + System.lineSeparator(),
        bytes.toString(StandardCharsets.UTF_8));
  }

  @Test
  public void jsonEscapesStringsAndKeepsIntegersAsNumbers() {
    assertEquals(
        "{\"text\":\"a\\\"b\\\\c\\nd\\te\\u0001中\",\"rows\":3,\"code\":130,\"path\":\"x y\"}",
        ExportProgressReporter.json(
            ExportProgressReporter.fields(
                "text",
                "a\"b\\c\nd\te" + (char) 1 + "中",
                "rows",
                3L,
                "code",
                130,
                "skipped",
                null,
                "path",
                Path.of("x y"))));
  }

  @Test
  public void interactiveQueryIsReadBeforeProgressAndLogStart() throws Exception {
    Path target = folder.getRoot().toPath();
    PrintStream originalOut = System.out;
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    AtomicBoolean prompted = new AtomicBoolean();
    int code;
    try {
      System.setOut(new PrintStream(console, true, StandardCharsets.UTF_8));
      code =
          ExportData.run(
              new String[] {"-ft", "csv", "-t", target.toString()},
              config -> {
                prompted.set(true);
                // Neither progress events nor the log may exist while the user types the query.
                assertEquals("", console.toString(StandardCharsets.UTF_8));
                assertFalse(hasLog(target));
                // End of input abandons the export before anything starts.
                throw new EndOfFileException();
              });
    } finally {
      System.setOut(originalOut);
    }
    assertTrue(prompted.get());
    assertEquals(Constants.CODE_ERROR, code);
    assertEquals(
        CliMessages.EXCEPTION_EXPORT_CANCELLED_85D44F54 + System.lineSeparator(),
        console.toString(StandardCharsets.UTF_8));
    assertFalse(hasLog(target));
  }

  private static boolean hasLog(Path directory) throws IOException {
    try (Stream<Path> files = Files.list(directory)) {
      return files.anyMatch(file -> file.getFileName().toString().endsWith(".log"));
    }
  }

  @Test
  public void cancelledFinishReportsSignalStatusWithoutUnopenedOutput() throws Exception {
    StringWriter log = new StringWriter();
    ExportConfig config = config();
    try (ExportProgressReporter reporter = reporter(log)) {
      reporter.taskStarted(
          new ExportTask(
              "select s from root.sg.d", config.targetDirectory.resolve("dump0"), null, null));
      reporter.cancel(130);
      assertThrows(IOException.class, reporter::checkActive);
      reporter.finish(ExportProgressReporter.Status.CANCELLED, null);
    }
    String content = log.toString();
    // The task has not opened a file, so no output is named.
    assertFalse(content, content.contains("\"output\":"));
    assertTrue(content, content.contains(member("status", "CANCELLED")));
    // The terminating signal decides the process status, which FINISH reports.
    assertTrue(content, content.contains(member("exit_code", 130)));
  }

  @Test
  public void queryModeReportsItsTimeRangeAsIgnored() throws Exception {
    StringWriter queryLog = new StringWriter();
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    try (ExportProgressReporter reporter =
        new ExportProgressReporter(
            queryLog,
            "tree",
            System.nanoTime(),
            new PrintStream(console, true, StandardCharsets.UTF_8))) {
      reporter.start(
          config("-q", "select * from root.**", "-start_time", "100"),
          true,
          () -> fail("unexpected log error"));
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    // The query runs as written, so START must not suggest a time filter.
    String start = event(queryLog, "START");
    assertFalse(start, start.contains("\"start_time\":"));
    String warning = event(queryLog, "WARNING");
    assertTrue(warning, warning.contains(member("level", "WARN")));
    assertTrue(warning, warning.contains(member("start_time", "100")));
    String shown = console.toString(StandardCharsets.UTF_8);
    assertTrue(
        shown,
        shown
            .lines()
            .anyMatch(
                String.format(
                        CliMessages.LOG_WARNING_ARG_05BE32E9,
                        CliMessages
                            .LOG_START_TIME_AND_END_TIME_DO_NOT_APPLY_TO_QUERIES_AND_ARE_IGNORED_02DC37CC)
                    ::equals));

    StringWriter measurementLog = new StringWriter();
    try (ExportProgressReporter reporter = reporter(measurementLog)) {
      reporter.start(
          config("-mf", input("root.sg.d.s\n").toString(), "-start_time", "100"),
          false,
          () -> fail("unexpected log error"));
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    assertTrue(event(measurementLog, "START").contains(member("start_time", "100")));
    assertFalse(measurementLog.toString().contains(member("event", "WARNING")));
  }

  @Test
  public void tableQueryReportsItsTableAsIgnored() throws Exception {
    StringWriter queryLog = new StringWriter();
    try (ExportProgressReporter reporter = reporter(queryLog)) {
      reporter.start(
          config("-sql_dialect", "table", "-db", "db", "-table", "t", "-q", "select * from t"),
          true,
          () -> fail("unexpected log error"));
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    // SQL and TsFile output take the table from the query, so -table has no effect.
    String warning = event(queryLog, "WARNING");
    assertTrue(
        warning,
        warning.contains(
            member(
                "message",
                CliMessages.LOG_TABLE_DOES_NOT_APPLY_TO_QUERIES_AND_IS_IGNORED_DBA31209)));
    assertTrue(warning, warning.contains(member("table", "t")));

    StringWriter tableLog = new StringWriter();
    try (ExportProgressReporter reporter = reporter(tableLog)) {
      reporter.start(
          config("-sql_dialect", "table", "-db", "db", "-table", "t"),
          false,
          () -> fail("unexpected log error"));
      reporter.finish(ExportProgressReporter.Status.SUCCESS, null);
    }
    assertFalse(tableLog.toString().contains(member("event", "WARNING")));
  }

  private static String event(StringWriter log, String event) {
    return log.toString()
        .lines()
        .filter(line -> line.contains(member("event", event)))
        .findFirst()
        .orElseThrow(() -> new AssertionError(log.toString()));
  }

  /** A member as the JSON Lines log writes it, such as "status":"FAILED" or "exit_code":130. */
  static String member(String key, Object value) {
    return '"' + key + "\":" + (value instanceof Number ? value : "\"" + value + '"');
  }

  /** Whether some console line came from {@code template}, whatever values filled it in. */
  static boolean printed(String console, String template) {
    String pattern =
        Arrays.stream(template.split("%[sd]", -1))
            .map(Pattern::quote)
            .collect(Collectors.joining(".*?"));
    return Pattern.compile(pattern).matcher(console).find();
  }

  @Test
  public void timerLogFailureIsVisibleToWorkerAndFailureHandler() throws Exception {
    CountDownLatch failed = new CountDownLatch(1);
    AtomicBoolean failWrites = new AtomicBoolean();
    Writer log =
        new Writer() {
          @Override
          public void write(char[] data, int offset, int length) throws IOException {
            if (failWrites.get()) {
              throw new IOException("disk failure");
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    try (ExportProgressReporter reporter =
        new ExportProgressReporter(log, "tree", System.nanoTime(), new PrintStream(console))) {
      reporter.start(config(), false, failed::countDown);
      failWrites.set(true);
      assertTrue(failed.await(5, TimeUnit.SECONDS));
      assertThrows(IOException.class, reporter::checkActive);
      assertThrows(
          IOException.class, () -> reporter.finish(ExportProgressReporter.Status.SUCCESS, null));
      assertEquals(ExportProgressReporter.Status.FAILED, reporter.status());
    }
    // The log cannot hold its own failure, so the console summary names it.
    String shown = console.toString(StandardCharsets.UTF_8);
    assertTrue(shown, printed(shown, CliMessages.LOG_EXPORT_FAILED_AFTER_ARG_ARG_C123F0EF));
    assertTrue(shown, shown.contains("disk failure"));
    assertFalse(
        shown,
        printed(shown, CliMessages.LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B));
  }

  @Test
  public void resultCloseFailureNeverCompletesTheTask() throws Exception {
    Session session = mock(Session.class);
    SessionDataSet data = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(data.getColumnNames()).thenReturn(Arrays.asList("Time", "root.sg.d.s"));
    when(data.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(Arrays.asList("INT64", "INT32"));
    doThrow(new IOExceptionForTest()).when(data).close();
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(data);
    StringWriter log = new StringWriter();
    ExportConfig config = config();
    try (ExportProgressReporter reporter = reporter(log);
        ExportDataTree exporter = new ExportDataTree(config, reporter, session)) {
      assertThrows(
          org.apache.iotdb.rpc.IoTDBConnectionException.class,
          () ->
              exporter.export(
                  new ExportTask(
                      "select s from root.sg.d",
                      config.targetDirectory.resolve("dump0"),
                      null,
                      null)));
      reporter.finish(ExportProgressReporter.Status.FAILED, new IOException("close failure"));
    }
    assertFalse(log.toString().contains(member("event", "TASK_COMPLETE")));
    verify(data, times(1)).close();
    verify(session).close();
  }

  private static class IOExceptionForTest extends org.apache.iotdb.rpc.IoTDBConnectionException {
    IOExceptionForTest() {
      super("close failure");
    }
  }

  private ExportConfig config(String... extras) throws Exception {
    List<String> args =
        new ArrayList<>(Arrays.asList("-ft", "csv", "-t", folder.getRoot().toString()));
    args.addAll(Arrays.asList(extras));
    return ExportConfig.parse(args.toArray(new String[0]), START);
  }

  private Path input(String contents) throws IOException {
    Path path = folder.newFile().toPath();
    return Files.writeString(path, contents, StandardCharsets.UTF_8);
  }

  private ExportProgressReporter reporter(StringWriter log) {
    return new ExportProgressReporter(
        log, "tree", System.nanoTime(), new PrintStream(new ByteArrayOutputStream()));
  }

  private SessionDataSet metadata(String... paths) throws Exception {
    SessionDataSet data = mock(SessionDataSet.class);
    when(data.getColumnNames())
        .thenReturn(Arrays.asList("Timeseries", "DataType", "Encoding", "Compression"));
    List<RowRecord> records = new ArrayList<>();
    for (String path : paths) {
      List<Field> fields = new ArrayList<>();
      for (String value : Arrays.asList(path, "INT32", "RLE", "LZ4")) {
        Field field = new Field(TSDataType.TEXT);
        field.setBinaryV(new Binary(value, StandardCharsets.UTF_8));
        fields.add(field);
      }
      records.add(new RowRecord(0, fields));
    }
    java.util.concurrent.atomic.AtomicInteger cursor =
        new java.util.concurrent.atomic.AtomicInteger();
    when(data.hasNext()).thenAnswer(call -> cursor.get() < records.size());
    when(data.next()).thenAnswer(call -> records.get(cursor.getAndIncrement()));
    return data;
  }
}
