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
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.tool.common.Constants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.utils.Signals;

import java.io.IOException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Entry point and lifecycle for one export invocation. */
public final class ExportData {
  /** How long cancellation waits for the worker to close its own query and session. */
  private static final long CANCEL_GRACE_SECONDS = 3;

  /** POSIX numbers of the signals that shut the JVM down; it then exits with 128 + number. */
  private static final Map<String, Integer> TERMINATING_SIGNALS =
      Map.of("HUP", 1, "INT", 2, "TERM", 15);

  /** Exit status chosen by a terminating signal, or 0 before one arrives. */
  private static final AtomicInteger SIGNAL_EXIT_CODE = new AtomicInteger();

  /** Reads the Tree query when neither a query nor a measurement file is supplied. */
  @FunctionalInterface
  interface QueryPrompt {
    String read(ExportConfig config) throws IOException;
  }

  private ExportData() {}

  public static void main(String[] args) {
    System.exit(run(args));
  }

  static int run(String[] args) {
    return run(args, ExportData::promptQuery);
  }

  // Tests replace the prompt: JLine reads the process terminal rather than a replaced System.in.
  static int run(String[] args, QueryPrompt queryPrompt) {
    ZonedDateTime startedAt = ZonedDateTime.now();
    ExportProgressReporter progress = null;
    AbstractExportData exporter = null;
    Thread hook = null;
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch finished = new CountDownLatch(1);
    Throwable failure = null;
    try {
      if (args == null || args.length == 0) {
        help(true, null);
        return Constants.CODE_ERROR;
      }
      CommandLine inspected = ExportConfig.inspect(args);
      if (inspected.hasOption(Constants.HELP_ARGS)) {
        help(
            !"table".equalsIgnoreCase(inspected.getOptionValue(Constants.SQL_DIALECT_ARGS)),
            inspected.getOptionValue(Constants.HELP_ARGS));
        return Constants.CODE_OK;
      }
      ExportConfig config = ExportConfig.parse(args, startedAt);
      if (config.measurementFile != null
          && Files.exists(config.measurementFile)
          && Files.exists(config.logFile)
          && Files.isSameFile(config.measurementFile, config.logFile)) {
        throw new ArgsErrorException(
            CliMessages
                .EXCEPTION_THE_EXPORT_LOG_MUST_DIFFER_FROM_THE_MEASUREMENT_INPUT_FILE_088A542C);
      }
      String query = config.query;
      if (query == null && config.tree && config.measurementFile == null) {
        // Prompt before the progress timer starts, so periodic events cannot interleave with input.
        query = queryPrompt.read(config);
      }
      Files.createDirectories(config.targetDirectory);
      progress = new ExportProgressReporter(config, System.nanoTime(), System.out);
      exporter =
          config.tree
              ? new ExportDataTree(config, progress)
              : new ExportDataTable(config, progress);
      ExportProgressReporter reporter = progress;
      AbstractExportData ownedExporter = exporter;
      Thread worker = Thread.currentThread();
      hook =
          new Thread(
              () -> {
                int signalExitCode = SIGNAL_EXIT_CODE.get();
                cancelled.set(true);
                reporter.cancel(signalExitCode == 0 ? Constants.CODE_ERROR : signalExitCode);
                worker.interrupt();
                try {
                  // The worker closes its query and session once an in-flight call returns.
                  // Closing them underneath that call makes Thrift log a socket error.
                  if (finished.await(CANCEL_GRACE_SECONDS, TimeUnit.SECONDS)) {
                    return;
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                // The worker is blocked in a call that ignores interrupts; closing ends it.
                Throwable closeFailure = null;
                try {
                  ownedExporter.close();
                } catch (Exception e) {
                  closeFailure = e;
                }
                try {
                  reporter.finish(ExportProgressReporter.Status.CANCELLED, closeFailure);
                } catch (Exception ignored) {
                  // finish already reports a log failure to the console.
                }
              },
              "export-data-shutdown");
      recordTerminatingSignals();
      Runtime.getRuntime().addShutdownHook(hook);
      progress.start(
          config,
          query != null,
          () -> {
            worker.interrupt();
            // Unblock IO cleanup without making the progress timer access the session.
            Thread cleanup =
                new Thread(
                    () -> {
                      try {
                        ownedExporter.close();
                      } catch (Exception ignored) {
                        // The reporter retains the original log failure.
                      }
                    },
                    "export-data-log-failure");
            cleanup.setDaemon(true);
            cleanup.start();
          });
      MeasurementFile input =
          config.measurementFile == null ? null : MeasurementFile.read(config.measurementFile);
      List<ExportTask> tasks = query == null ? null : queryTasks(config, query);
      progress.phase(ExportProgressReporter.Phase.CONNECTING);
      exporter.init();
      if (input != null) {
        tasks = ((ExportDataTree) exporter).planMeasurements(input, exporter.timeRange());
      } else if (tasks == null) {
        tasks = ((ExportDataTable) exporter).planTables(exporter.timeRange());
      } else if (!config.tree) {
        tasks = ((ExportDataTable) exporter).planQueries(tasks);
      }
      for (ExportTask task : tasks) {
        progress.checkActive();
        exporter.export(task);
      }
      progress.checkActive();
      progress.phase(ExportProgressReporter.Phase.CLOSING);
    } catch (Exception e) {
      failure = e;
      if (e instanceof UserInterruptException || e instanceof EndOfFileException) {
        cancelled.set(true);
      }
    } finally {
      Exception closeFailure = null;
      if (exporter != null) {
        try {
          exporter.close();
        } catch (Exception e) {
          closeFailure = e;
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (progress != null) {
        try {
          // A cancelled export reports cleanup failures, not the interruption it caused.
          progress.finish(
              cancelled.get()
                  ? ExportProgressReporter.Status.CANCELLED
                  : failure == null
                      ? ExportProgressReporter.Status.SUCCESS
                      : ExportProgressReporter.Status.FAILED,
              cancelled.get() ? closeFailure : failure);
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      } else if (cancelled.get()) {
        // Input was abandoned at a prompt before the export started.
        System.out.println(CliMessages.EXCEPTION_EXPORT_CANCELLED_85D44F54);
      } else if (failure != null) {
        System.out.println(
            String.format(CliMessages.LOG_EXPORT_FAILED_ARG_57F9420F, failure.getMessage()));
      }
      if (hook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException ignored) {
          // JVM shutdown owns the cancellation hook once it has started.
        }
      }
      // A running cancellation hook waits for this before the JVM exits.
      finished.countDown();
    }
    return failure == null
            && !cancelled.get()
            && (progress == null || progress.status() == ExportProgressReporter.Status.SUCCESS)
        ? Constants.CODE_OK
        : Constants.CODE_ERROR;
  }

  // Shutdown hooks cannot see the signal, so record it to report the status the JVM exits with.
  private static void recordTerminatingSignals() {
    TERMINATING_SIGNALS.forEach(
        (name, number) ->
            // Without the signal on this platform, the JVM default handler stays in place.
            Signals.register(
                name,
                () -> {
                  SIGNAL_EXIT_CODE.set(128 + number);
                  System.exit(128 + number);
                }));
  }

  private static String promptQuery(ExportConfig config) throws IOException {
    return JlineUtils.getLineReader(
            new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
            config.username,
            config.host,
            Integer.toString(config.port))
        .readLine(CliMessages.LOG_PLEASE_INPUT_QUERY_EEAD5433);
  }

  static List<ExportTask> queryTasks(ExportConfig config, String query) throws ArgsErrorException {
    if (query == null || query.trim().isEmpty()) {
      throw new ArgsErrorException(CliMessages.EXCEPTION_NO_EXPORT_QUERY_WAS_PROVIDED_09E24A92);
    }
    List<ExportTask> tasks = new ArrayList<>();
    for (String sql : query.trim().split(";")) {
      if (sql.trim().isEmpty()) {
        continue;
      }
      tasks.add(
          new ExportTask(
              sql.trim(),
              config.targetDirectory.resolve(config.prefix + tasks.size()),
              null,
              null));
    }
    if (tasks.isEmpty()) {
      throw new ArgsErrorException(CliMessages.EXCEPTION_NO_EXPORT_QUERY_WAS_PROVIDED_09E24A92);
    }
    return tasks;
  }

  private static void help(boolean tree, String format) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(null);
    formatter.setWidth(Constants.MAX_HELP_CONSOLE_WIDTH);
    boolean all =
        format == null
            || (!"csv".equalsIgnoreCase(format)
                && !"sql".equalsIgnoreCase(format)
                && !"tsfile".equalsIgnoreCase(format));
    AbstractDataTool.printHelpOptions(
        all ? Constants.EXPORT_CLI_HEAD : null,
        Constants.EXPORT_CLI_PREFIX,
        formatter,
        all || "tsfile".equalsIgnoreCase(format) ? ExportConfig.options(tree, "tsfile") : null,
        all || "csv".equalsIgnoreCase(format) ? ExportConfig.options(tree, "csv") : null,
        all || "sql".equalsIgnoreCase(format) ? ExportConfig.options(tree, "sql") : null,
        all);
  }
}
