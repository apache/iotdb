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
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.tool.common.Constants;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One task's counters, its JSON Lines log and its console view. The timer never touches a Session
 * or result set.
 */
final class ExportProgressReporter implements AutoCloseable {
  enum Status {
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED
  }

  enum Phase {
    VALIDATING,
    CONNECTING,
    DISCOVERING,
    EXPORTING,
    CLOSING
  }

  private final Writer log;
  private final ExportConsole console;
  private final String model;
  private final long startNanos;
  private final AtomicLong rows = new AtomicLong();
  private final ScheduledExecutorService timer;
  private volatile IOException logFailure;
  private volatile boolean stopping;
  private volatile int cancelExitCode = Constants.CODE_ERROR;
  private volatile Status status = Status.RUNNING;
  private volatile Phase phase = Phase.VALIDATING;
  private long totalMeasurements = -1;
  private long completedMeasurements;
  private long completedTasks;
  private long zeroRowTasks;
  private long files;
  private long taskStartRows;
  private long exportStartNanos;
  private String currentMeasurement = "";
  private String output = "";
  private Path logFile;
  private boolean closed;
  private Runnable failureHandler = () -> {};

  ExportProgressReporter(ExportConfig config, long startNanos, PrintStream console)
      throws IOException {
    this(
        Files.newBufferedWriter(
            config.logFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND),
        config.tree ? "tree" : "table",
        startNanos,
        new ExportConsole(console, ExportConsole.isTerminal()));
  }

  // Writer injection exercises real write/flush failures without relying on filesystem permissions.
  ExportProgressReporter(Writer log, String model, long startNanos, PrintStream console) {
    this(log, model, startNanos, new ExportConsole(console, false));
  }

  ExportProgressReporter(Writer log, String model, long startNanos, ExportConsole console) {
    this.log = log;
    this.console = console;
    this.model = model;
    this.startNanos = startNanos;
    timer =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "export-data-progress");
              thread.setDaemon(true);
              return thread;
            });
  }

  synchronized void start(ExportConfig config, boolean queryMode, Runnable onFailure)
      throws IOException {
    failureHandler = onFailure;
    logFile = config.logFile;
    if (emit(
        "INFO",
        "START",
        Status.RUNNING,
        fields(
            "format",
            config.format,
            "input",
            config.measurementFile,
            "mode",
            config.measurementFile != null ? "measurements" : queryMode ? "query" : "automatic",
            // A query runs as written, so a time range here would misstate what it exports.
            "start_time",
            queryMode ? null : config.startTime,
            "end_time",
            queryMode ? null : config.endTime,
            "target",
            config.targetDirectory,
            "log",
            config.logFile,
            "lines_per_file",
            config.linesPerFile,
            "message",
            CliMessages.LOG_EXPORT_STARTED_45465087))) {
      console.line(
          String.format(
              Locale.ROOT,
              CliMessages.LOG_EXPORT_STARTED_ARG_ARG_TARGET_ARG_LOG_ARG_BD58AECB,
              model,
              config.format,
              config.targetDirectory,
              config.logFile));
    }
    if (queryMode && (config.startTime != null || config.endTime != null)) {
      warning(
          CliMessages.LOG_START_TIME_AND_END_TIME_DO_NOT_APPLY_TO_QUERIES_AND_ARE_IGNORED_02DC37CC,
          "start_time",
          config.startTime,
          "end_time",
          config.endTime);
    }
    if (queryMode && config.table != null) {
      warning(
          CliMessages.LOG_TABLE_DOES_NOT_APPLY_TO_QUERIES_AND_IS_IGNORED_DBA31209,
          "table",
          config.table);
    }
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(timer, this::tick, 2, 2, TimeUnit.SECONDS);
  }

  private void tick() {
    try {
      progress();
    } catch (Exception e) {
      logFailure =
          e instanceof IOException
              ? (IOException) e
              : new IOException(CliMessages.LOG_CANNOT_WRITE_OR_CLOSE_THE_EXPORT_LOG_32579AC6, e);
      stopping = true;
      failureHandler.run();
    }
  }

  synchronized void progress() throws IOException {
    if (!stopping
        && status == Status.RUNNING
        && !closed
        && emit("INFO", "PROGRESS", Status.RUNNING, fields())) {
      showProgress();
    }
  }

  private void showProgress() {
    String exported = count(rows.get());
    String elapsed = duration(elapsedSeconds());
    if (totalMeasurements < 0) {
      // Without a measurement file there is no total, so no percentage or remaining time either.
      String rate = count(rowsPerSecond());
      console.progress(
          String.format(
              Locale.ROOT,
              CliMessages.LOG_EXPORTING_ARG_ROWS_ARG_ARG_ROWS_S_56BD441C,
              exported,
              elapsed,
              rate),
          "",
          String.format(
              Locale.ROOT,
              CliMessages.LOG_ARG_ROWS_ELAPSED_ARG_ARG_ROWS_S_AE3CF660,
              exported,
              elapsed,
              rate));
      return;
    }
    // Whole points, so the console never shows 100% before the last measurement completes.
    String percent =
        (totalMeasurements == 0 ? 0 : completedMeasurements * 100 / totalMeasurements) + "%";
    long remaining = remainingSeconds();
    String eta = remaining < 0 ? "--" : duration(remaining);
    console.progress(
        String.format(
            Locale.ROOT,
            CliMessages.LOG_EXPORTING_ARG_ARG_ARG_ARG_ROWS_ARG_ETA_ARG_C41CA790,
            percent,
            completedMeasurements,
            totalMeasurements,
            exported,
            elapsed,
            eta),
        currentMeasurement,
        String.format(
            Locale.ROOT,
            CliMessages
                .LOG_ARG_ARG_ARG_MEASUREMENTS_ARG_ROWS_ELAPSED_ARG_ETA_ARG_CURRENT_ARG_B6DEA0BD,
            percent,
            completedMeasurements,
            totalMeasurements,
            exported,
            elapsed,
            eta,
            currentMeasurement.isEmpty() ? "-" : currentMeasurement));
  }

  void phase(Phase phase) throws IOException {
    checkActive();
    this.phase = phase;
  }

  synchronized void discovered(MeasurementFile input, long matches, int measurements, int unmatched)
      throws IOException {
    checkActive();
    totalMeasurements = measurements;
    exportStartNanos = System.nanoTime();
    if (emit(
        "INFO",
        "DISCOVERY",
        Status.RUNNING,
        fields(
            "source",
            input.source(),
            "input_lines",
            input.lines(),
            "patterns",
            input.patterns().size(),
            "duplicate_patterns",
            input.duplicates(),
            "matches",
            matches,
            "duplicate_matches",
            matches - measurements,
            "unmatched_patterns",
            unmatched))) {
      console.line(
          String.format(
              Locale.ROOT,
              CliMessages.LOG_MEASUREMENTS_TO_EXPORT_ARG_PATH_PATTERNS_ARG_A0AC5F31,
              measurements,
              input.patterns().size()));
    }
  }

  /** {@code message} says on its own what the warning is about; its details go to the log only. */
  synchronized void warning(String message, Object... details) throws IOException {
    Map<String, Object> values = fields(details);
    values.put("message", message);
    if (emit("WARN", "WARNING", Status.RUNNING, values)) {
      console.line(String.format(Locale.ROOT, CliMessages.LOG_WARNING_ARG_05BE32E9, message));
    }
  }

  synchronized void taskStarted(ExportTask task) throws IOException {
    checkActive();
    phase = Phase.EXPORTING;
    currentMeasurement = task.measurement() == null ? "" : task.measurement().path().getFullPath();
    // FILE_START names the output once the task opens a file; a task without rows opens none.
    output = "";
    taskStartRows = rows.get();
    emit("INFO", "TASK_START", Status.RUNNING, fields("sql", task.sql(), "table", task.table()));
  }

  synchronized void taskCompleted() throws IOException {
    checkActive();
    long taskRows = rows.get() - taskStartRows;
    completedTasks++;
    if (totalMeasurements >= 0) {
      completedMeasurements++;
    }
    if (taskRows == 0) {
      zeroRowTasks++;
    }
    emit("INFO", "TASK_COMPLETE", Status.RUNNING, fields("task_rows", taskRows));
  }

  synchronized void fileStarted(Path file) throws IOException {
    checkActive();
    output = file.toString();
    emit("INFO", "FILE_START", Status.RUNNING, fields());
  }

  synchronized void fileCompleted(Path file, long fileRows) throws IOException {
    checkActive();
    files++;
    emit("INFO", "FILE_COMPLETE", Status.RUNNING, fields("output", file, "file_rows", fileRows));
  }

  void rowsWritten(long count) throws IOException {
    checkActive();
    rows.addAndGet(count);
  }

  long rows() {
    return rows.get();
  }

  void checkActive() throws IOException {
    if (logFailure != null) {
      throw logFailure;
    }
    if (stopping || Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException(CliMessages.EXCEPTION_EXPORT_CANCELLED_85D44F54);
    }
  }

  /** Stops at the next check; a cancelled finish reports the status the process exits with. */
  void cancel(int exitCode) {
    cancelExitCode = exitCode;
    stopping = true;
  }

  Status status() {
    return status;
  }

  synchronized void finish(Status terminal, Throwable error) throws IOException {
    if (status != Status.RUNNING || closed) {
      return;
    }
    stopping = true;
    timer.shutdownNow();
    if (terminal == Status.SUCCESS && logFailure != null) {
      terminal = Status.FAILED;
      error = logFailure;
    }
    String reason = reason(error);
    String line =
        event(
            terminal == Status.SUCCESS ? "INFO" : "ERROR",
            "FINISH",
            terminal,
            fields(
                "exit_code",
                exitCode(terminal),
                "completed_tasks",
                completedTasks,
                "zero_row_tasks",
                zeroRowTasks,
                "message",
                terminal == Status.SUCCESS
                    ? CliMessages.LOG_EXPORT_COMPLETELY_34200D19
                    : terminal == Status.CANCELLED
                        ? CliMessages.EXCEPTION_EXPORT_CANCELLED_85D44F54
                        : CliMessages.LOG_EXPORT_FAILED_77EE72CD,
                "error",
                reason));
    IOException writeError = null;
    try {
      log.write(line + System.lineSeparator());
      log.flush();
    } catch (IOException e) {
      writeError = e;
    }
    try {
      log.close();
    } catch (IOException e) {
      if (writeError == null) {
        writeError = e;
      } else {
        writeError.addSuppressed(e);
      }
    }
    closed = true;
    if (writeError != null) {
      logFailure = writeError;
      // The log cannot record its own failure, so only the console reports it.
      reason =
          CliMessages.LOG_CANNOT_WRITE_OR_CLOSE_THE_EXPORT_LOG_32579AC6 + ' ' + reason(writeError);
      if (terminal == Status.CANCELLED) {
        console.line(String.format(Locale.ROOT, CliMessages.LOG_WARNING_ARG_05BE32E9, reason));
      } else {
        terminal = Status.FAILED;
      }
    }
    status = terminal;
    // Scripts read the outcome from the last console line, so the log path is printed first.
    if (logFile != null) {
      console.line(String.format(Locale.ROOT, CliMessages.LOG_LOG_ARG_FBF61F63, logFile));
    }
    console.line(summary(terminal, reason));
    if (writeError != null) {
      throw writeError;
    }
  }

  private String summary(Status terminal, String reason) {
    String exported = count(rows.get());
    String elapsed = duration(elapsedSeconds());
    if (terminal == Status.SUCCESS) {
      return totalMeasurements >= 0
          ? String.format(
              Locale.ROOT,
              CliMessages
                  .LOG_EXPORT_COMPLETELY_MEASUREMENTS_ARG_ARG_ROWS_ARG_FILES_ARG_ELAPSED_ARG_36D9F8E5,
              completedMeasurements,
              totalMeasurements,
              exported,
              count(files),
              elapsed)
          : String.format(
              Locale.ROOT,
              CliMessages.LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B,
              exported,
              count(files),
              elapsed);
    }
    if (terminal == Status.CANCELLED) {
      return String.format(
          Locale.ROOT,
          CliMessages.LOG_EXPORT_CANCELLED_AFTER_ARG_ROWS_EXPORTED_ARG_0999F99F,
          elapsed,
          exported);
    }
    return reason == null
        ? String.format(Locale.ROOT, CliMessages.LOG_EXPORT_FAILED_AFTER_ARG_DA6D56F9, elapsed)
        : String.format(
            Locale.ROOT, CliMessages.LOG_EXPORT_FAILED_AFTER_ARG_ARG_C123F0EF, elapsed, reason);
  }

  private static String reason(Throwable error) {
    if (error == null) {
      return null;
    }
    return error.getMessage() == null ? error.toString() : error.getMessage();
  }

  private int exitCode(Status terminal) {
    return terminal == Status.SUCCESS
        ? Constants.CODE_OK
        : terminal == Status.CANCELLED ? cancelExitCode : Constants.CODE_ERROR;
  }

  /** Appends one event to the log; false once the log has closed and records nothing more. */
  private boolean emit(String level, String event, Status eventStatus, Map<String, Object> details)
      throws IOException {
    if (closed || status != Status.RUNNING) {
      return false;
    }
    String line = event(level, event, eventStatus, details);
    try {
      log.write(line + System.lineSeparator());
      log.flush();
    } catch (IOException e) {
      logFailure = e;
      throw e;
    }
    return true;
  }

  private String event(
      String level, String event, Status eventStatus, Map<String, Object> details) {
    // The export requirement's progress fields come first, in its order; event details follow.
    // These keys are the stable, locale-independent log schema, not translated prose.
    Map<String, Object> values =
        fields(
            "progress_id",
            ProcessHandle.current().pid(),
            "timestamp",
            ZonedDateTime.now()
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            "model",
            model,
            "status",
            eventStatus,
            "exported_data_rows",
            rows.get());
    if (totalMeasurements >= 0) {
      values.put("exported_measurement_num", completedMeasurements);
      values.put("total_measurement_num", totalMeasurements);
      values.put(
          "exported_percent",
          String.format(
              Locale.ROOT,
              CliMessages.LOG_ARG_82AC62F7,
              totalMeasurements == 0 ? 0.0 : completedMeasurements * 100.0 / totalMeasurements));
      long remaining = remainingSeconds();
      values.put("estimated_remaining", remaining < 0 ? "UNKNOWN" : duration(remaining));
    }
    values.put("elapsed_time", duration(elapsedSeconds()));
    values.put("output", output.isEmpty() ? null : output);
    values.put("event", event);
    values.put("level", level);
    values.put("phase", phase);
    values.put("rows_per_second", rowsPerSecond());
    values.put("files", files);
    values.put("current_measurement", currentMeasurement.isEmpty() ? null : currentMeasurement);
    values.putAll(details);
    return json(values);
  }

  /** Seconds left at the average measurement duration, or -1 while too few have completed. */
  private long remainingSeconds() {
    // Measurements differ widely in size, so the first few say little about the rest.
    long sample = Math.min(totalMeasurements, Math.max(3, (totalMeasurements + 19) / 20));
    if (completedMeasurements == 0 || completedMeasurements < sample) {
      return -1;
    }
    // Rounded up, so work still left never shows as 00:00:00.
    return (long)
        Math.ceil(
            (System.nanoTime() - exportStartNanos)
                / 1_000_000_000.0
                * (totalMeasurements - completedMeasurements)
                / completedMeasurements);
  }

  private long elapsedSeconds() {
    return Math.max(0, System.nanoTime() - startNanos) / 1_000_000_000;
  }

  private long rowsPerSecond() {
    return Math.round(
        rows.get() / Math.max(1.0, Math.max(0, System.nanoTime() - startNanos) / 1_000_000_000.0));
  }

  static Map<String, Object> fields(Object... values) {
    Map<String, Object> fields = new LinkedHashMap<>();
    for (int i = 0; i < values.length; i += 2) {
      fields.put((String) values[i], values[i + 1]);
    }
    return fields;
  }

  /** One flat JSON object without null members; integers stay numbers, the rest are strings. */
  static String json(Map<String, Object> values) {
    StringBuilder json = new StringBuilder("{");
    values.forEach(
        (key, value) -> {
          if (value == null) {
            return;
          }
          if (json.length() > 1) {
            json.append(',');
          }
          string(json, key).append(':');
          if (value instanceof Integer || value instanceof Long) {
            json.append(value);
          } else {
            string(json, String.valueOf(value));
          }
        });
    return json.append('}').toString();
  }

  private static StringBuilder string(StringBuilder json, String value) {
    json.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '"' || c == '\\') {
        json.append('\\').append(c);
      } else if (c == '\n') {
        json.append("\\n");
      } else if (c == '\r') {
        json.append("\\r");
      } else if (c == '\t') {
        json.append("\\t");
      } else if (c < 0x20) {
        json.append("\\u00")
            .append(Character.forDigit(c >> 4, 16))
            .append(Character.forDigit(c & 0xF, 16));
      } else {
        json.append(c);
      }
    }
    return json.append('"');
  }

  private static String count(long value) {
    return NumberFormat.getIntegerInstance(Locale.ROOT).format(value);
  }

  private static String duration(long seconds) {
    long hours = seconds / 3600;
    long minutes = seconds / 60 % 60;
    long remainder = seconds % 60;
    return (hours < 10 ? "0" : "")
        + hours
        + ":"
        + (minutes < 10 ? "0" : "")
        + minutes
        + ":"
        + (remainder < 10 ? "0" : "")
        + remainder;
  }

  @Override
  public synchronized void close() throws IOException {
    timer.shutdownNow();
    if (!closed) {
      closed = true;
      log.close();
    }
  }
}
