/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tool.pipe;

import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.external.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

/**
 * CLI tool: register {@code tsfile-remote-sink} if missing, then create a Pipe that exports TsFiles
 * to a remote directory via SCP.
 */
public final class TsFileBackup {

  // ==============================================================================================
  // Constants dictionary: plugin names, keys, defaults, and CLI options
  // ==============================================================================================
  public static final String SINK_PLUGIN_NAME = "TSFILE_REMOTE_SINK";
  public static final String SINK_PLUGIN_CLASS =
      "org.apache.iotdb.pipe.plugin.sink.tsfile.PipeTsFileRemoteSink";

  private static final String IOTDB_HOME_PROPERTY = "IOTDB_HOME";
  private static final String DEFAULT_PLUGIN_DIR = "ext/pipe";
  private static final String DEFAULT_PLUGIN_JAR_PREFIX = "tsfile-remote-sink-";
  private static final String DEFAULT_PLUGIN_JAR_SUFFIX = "-jar-with-dependencies.jar";

  private static final IoTPrinter OUT = new IoTPrinter(System.out);

  /** Business default values. */
  private static final class DefaultValues {
    static final String TREE_PATH = "root.**";
    static final String REGEX_ALL = ".*";
    static final String SCP_USER = "root";
    static final String SSH_PORT = "22";
    static final String FALLBACK_IP = "127.0.0.1";
  }

  /** Pipe attribute keys (source side). */
  private static final class PipeKeys {
    static final String SOURCE_HISTORY_ENABLE = "source.history.enable";
    static final String SOURCE_REALTIME_ENABLE = "source.realtime.enable";
    static final String SOURCE_MODE = "source.mode";
    static final String SOURCE_MODE_SNAPSHOT = "snapshot";
    static final String SOURCE_INCLUSION = "source.inclusion";
    static final String SOURCE_INCLUSION_INSERT = "data.insert,data.delete";
    static final String SOURCE_START_TIME = "source.start-time";
    static final String SOURCE_END_TIME = "source.end-time";

    static final String SOURCE_CAPTURE_TREE = "source.capture.tree";
    static final String SOURCE_CAPTURE_TABLE = "source.capture.table";
    static final String SOURCE_PATTERN = "source.path";
    static final String SOURCE_DB_NAME = "source.database-name";
    static final String SOURCE_TABLE_NAME = "source.table-name";

    /** When true, deletion mod files are transferred together with TsFiles. */
    static final String SOURCE_MODS_ENABLE = "source.mods.enable";

    static final String SINK = "sink";
    static final String SINK_SCP_HOST = "sink.scp.host";
    static final String SINK_SCP_PORT = "sink.scp.port";
    static final String SINK_SCP_USER = "sink.scp.user";
    static final String SINK_SCP_PASSWORD = "sink.scp.password";
    static final String SINK_SCP_REMOTE_PATH = "sink.scp.remote-path";
    static final String SINK_RATE_LIMIT = "sink.rate-limit-bytes-per-second";
    static final String SINK_SCP_OBJECT_BATCH_SIZE_BYTES = "sink.scp.object-batch-size-bytes";
    static final String SINK_SCP_OBJECT_PARALLELISM = "sink.scp.object-parallelism";
    static final String SINK_SCP_OBJECT_WAITING_QUEUE_SIZE = "sink.scp.object-waiting-queue-size";
    static final String SINK_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS =
        "sink.scp.object-thread-keep-alive-seconds";
  }

  /** CLI option names and descriptions. */
  private static final class CliOptions {
    static final String HELP_OPT = "help";
    static final String HELP_LONG = "help";
    static final String HELP_DESC = "Display help and exit.";

    static final String HOST_OPT = "h";
    static final String HOST_LONG = "host";
    static final String HOST_ARG = "host";
    static final String HOST_DESC = "IoTDB host.";

    static final String PORT_OPT = "p";
    static final String PORT_LONG = "port";
    static final String PORT_ARG = "port";
    static final String PORT_DESC = "IoTDB RPC port.";

    static final String USER_OPT = "u";
    static final String USER_LONG = "user";
    static final String USER_ARG = "user";
    static final String USER_DESC = "IoTDB username.";

    static final String PW_OPT = "pw";
    static final String PW_LONG = "pw";
    static final String PW_ARG = "password";
    static final String PW_DESC = "IoTDB password; omit value for silent prompt.";

    static final String SQL_OPT = "sql_dialect";
    static final String SQL_LONG = "sql_dialect";
    static final String SQL_ARG = "tree|table";
    static final String SQL_DESC = "SQL dialect: tree or table.";

    static final String PATH_OPT = "path";
    static final String PATH_LONG = "path";
    static final String PATH_ARG = "pattern";
    static final String PATH_DESC = "Tree path pattern, default root.**.";

    static final String DB_OPT = "db";
    static final String DB_LONG = "database";
    static final String DB_ARG = "regex";
    static final String DB_DESC = "Table model database name regex, default .*";

    static final String TABLE_OPT = "table";
    static final String TABLE_LONG = "table";
    static final String TABLE_ARG = "regex";
    static final String TABLE_DESC = "Table model table name regex, default .*";

    static final String START_TIME_OPT = "s";
    static final String START_TIME_LONG = "start_time";
    static final String START_TIME_ARG = "ISO8601/Timestamp";
    static final String START_TIME_DESC =
        "Start time for data extraction (e.g. 2023-01-01T00:00:00 or timestamp).";

    static final String END_TIME_OPT = "e";
    static final String END_TIME_LONG = "end_time";
    static final String END_TIME_ARG = "ISO8601/Timestamp";
    static final String END_TIME_DESC = "End time for data extraction.";

    static final String TARGET_DIR_OPT = "t";
    static final String TARGET_DIR_LONG = "target";
    static final String TARGET_DIR_ARG = "path";
    static final String TARGET_DIR_DESC = "Remote absolute directory for export (SCP).";

    static final String TARGET_HOST_OPT = "th";
    static final String TARGET_HOST_LONG = "target_host";
    static final String TARGET_HOST_ARG = "host";
    static final String TARGET_HOST_DESC = "SCP target host; default: local IPv4.";

    static final String TARGET_USER_OPT = "tu";
    static final String TARGET_USER_LONG = "target_host_user";
    static final String TARGET_USER_ARG = "user";
    static final String TARGET_USER_DESC = "SCP/SSH username, default root.";

    static final String TARGET_PW_OPT = "tpw";
    static final String TARGET_PW_LONG = "target_host_pw";
    static final String TARGET_PW_ARG = "password";
    static final String TARGET_PW_DESC = "SCP password; omit for prompt.";

    static final String TARGET_PORT_OPT = "tp";
    static final String TARGET_PORT_LONG = "target_host_port";
    static final String TARGET_PORT_ARG = "port";
    static final String TARGET_PORT_DESC = "SSH port, default 22.";

    static final String RATE_LIMIT_LONG = "rate_limit";
    static final String RATE_LIMIT_ARG = "bytes/s";
    static final String RATE_LIMIT_DESC = "Sink rate limit in bytes/s; omit for unlimited.";

    static final String OBJECT_BATCH_SIZE_LONG = "object_batch_size";
    static final String OBJECT_BATCH_SIZE_ARG = "bytes";
    static final String OBJECT_BATCH_SIZE_DESC =
        "Maximum bytes per SCP object upload batch; omit to use plugin default.";

    static final String OBJECT_PARALLELISM_LONG = "object_parallelism";
    static final String OBJECT_PARALLELISM_ARG = "object_parallelism";
    static final String OBJECT_PARALLELISM_DESC =
        "Maximum parallel SCP uploads for object-file batches; "
            + "worker threads are created on demand.";

    static final String OBJECT_WAITING_QUEUE_SIZE_LONG = "object_waiting_queue_size";
    static final String OBJECT_WAITING_QUEUE_SIZE_ARG = "object_waiting_queue_size";
    static final String OBJECT_WAITING_QUEUE_SIZE_DESC =
        "Maximum queued async SCP object-upload tasks; "
            + "submission waits when the queue limit is reached.";

    static final String OBJECT_THREAD_KEEP_ALIVE_SECONDS_LONG = "object_thread_keep_alive_seconds";
    static final String OBJECT_THREAD_KEEP_ALIVE_SECONDS_ARG = "object_thread_keep_alive_seconds";
    static final String OBJECT_THREAD_KEEP_ALIVE_SECONDS_DESC =
        "Idle timeout in seconds before async SCP object-upload worker threads are reclaimed.";

    static final String PLUGIN_JAR_LONG = "plugin_jar";
    static final String PLUGIN_JAR_ARG = "path";
    static final String PLUGIN_JAR_DESC = "Override path to plugin jar.";
  }

  private TsFileBackup() {}

  public static void main(String[] args) {
    try {
      BackupConfig config = CliParser.parse(args);
      if (config == null) {
        return;
      }
      IoTDBPipeExecutor.execute(config);
    } catch (ParseException | IllegalArgumentException e) {
      OUT.println("Validation Error: " + e.getMessage());
      CliParser.printHelp();
      System.exit(Constants.CODE_ERROR);
    } catch (Exception e) {
      OUT.println("Execution Error: " + e.getMessage());
      System.exit(Constants.CODE_ERROR);
    }
  }

  // ==============================================================================================
  // 1. Domain Object: strongly-typed runtime configuration
  // ==============================================================================================
  public static class BackupConfig {
    final String host;
    final int port;
    final String user;
    final String password;

    final boolean isTreeModel;
    final String targetDir;
    final String treePath;
    final String dbRegex;
    final String tableRegex;
    final String startTime;
    final String endTime;

    final String scpHost;
    final int sshPort;
    final String scpUser;
    final String scpPassword;
    final Double rateLimitBytesPerSecond;
    final Long objectBatchSizeBytes;
    final Integer objectParallelism;
    final Integer objectWaitingQueueSize;
    final Long objectThreadKeepAliveSeconds;
    final File pluginJar;

    public BackupConfig(CommandLine line) throws ParseException {
      this.host = line.getOptionValue(CliOptions.HOST_OPT, Constants.HOST_DEFAULT_VALUE);
      this.port =
          Integer.parseInt(line.getOptionValue(CliOptions.PORT_OPT, Constants.PORT_DEFAULT_VALUE));
      this.user = line.getOptionValue(CliOptions.USER_OPT, Constants.USERNAME_DEFAULT_VALUE);
      this.password =
          resolvePassword(line, CliOptions.PW_OPT, "IoTDB Password: ", Constants.PW_DEFAULT_VALUE);

      this.isTreeModel = parseSqlDialect(line.getOptionValue(CliOptions.SQL_OPT));
      this.targetDir =
          requireNonBlank(
              line.getOptionValue(CliOptions.TARGET_DIR_OPT), "Target directory (-t) is required.");
      this.treePath = line.getOptionValue(CliOptions.PATH_LONG, DefaultValues.TREE_PATH).trim();
      this.dbRegex = line.getOptionValue(CliOptions.DB_OPT, DefaultValues.REGEX_ALL).trim();
      this.tableRegex = line.getOptionValue(CliOptions.TABLE_OPT, DefaultValues.REGEX_ALL).trim();
      this.startTime = line.getOptionValue(CliOptions.START_TIME_LONG);
      this.endTime = line.getOptionValue(CliOptions.END_TIME_LONG);

      this.scpHost =
          StringUtils.isBlank(line.getOptionValue(CliOptions.TARGET_HOST_OPT))
              ? getLocalIpv4()
              : line.getOptionValue(CliOptions.TARGET_HOST_OPT);
      this.scpUser = line.getOptionValue(CliOptions.TARGET_USER_OPT, DefaultValues.SCP_USER).trim();
      this.sshPort =
          Integer.parseInt(line.getOptionValue(CliOptions.TARGET_PORT_OPT, DefaultValues.SSH_PORT));
      this.scpPassword =
          resolveRemotePassword(
              line, String.format("Remote Host (%s, user %s) Password: ", scpHost, this.scpUser));

      String rateStr = line.getOptionValue(CliOptions.RATE_LIMIT_LONG);
      this.rateLimitBytesPerSecond =
          StringUtils.isNotBlank(rateStr) ? Double.parseDouble(rateStr.trim()) : null;

      String objectBatchSizeStr = line.getOptionValue(CliOptions.OBJECT_BATCH_SIZE_LONG);
      this.objectBatchSizeBytes =
          StringUtils.isNotBlank(objectBatchSizeStr)
              ? Long.parseLong(objectBatchSizeStr.trim())
              : null;
      if (this.objectBatchSizeBytes != null && this.objectBatchSizeBytes <= 0) {
        throw new IllegalArgumentException("object_batch_size must be a positive integer.");
      }
      String objectParallelismStr = line.getOptionValue(CliOptions.OBJECT_PARALLELISM_LONG);
      this.objectParallelism =
          StringUtils.isNotBlank(objectParallelismStr)
              ? Integer.parseInt(objectParallelismStr.trim())
              : null;
      if (this.objectParallelism != null && this.objectParallelism <= 0) {
        throw new IllegalArgumentException("object_parallelism must be a positive integer.");
      }
      String objectWaitingQueueSizeStr =
          line.getOptionValue(CliOptions.OBJECT_WAITING_QUEUE_SIZE_LONG);
      this.objectWaitingQueueSize =
          StringUtils.isNotBlank(objectWaitingQueueSizeStr)
              ? Integer.parseInt(objectWaitingQueueSizeStr.trim())
              : null;
      if (this.objectWaitingQueueSize != null && this.objectWaitingQueueSize < 0) {
        throw new IllegalArgumentException(
            "object_waiting_queue_size must be a non-negative integer.");
      }
      String objectThreadKeepAliveSecondsStr =
          line.getOptionValue(CliOptions.OBJECT_THREAD_KEEP_ALIVE_SECONDS_LONG);
      this.objectThreadKeepAliveSeconds =
          StringUtils.isNotBlank(objectThreadKeepAliveSecondsStr)
              ? Long.parseLong(objectThreadKeepAliveSecondsStr.trim())
              : null;
      if (this.objectThreadKeepAliveSeconds != null && this.objectThreadKeepAliveSeconds <= 0) {
        throw new IllegalArgumentException(
            "object_thread_keep_alive_seconds must be a positive integer.");
      }

      this.pluginJar =
          resolvePluginJar(
              line.getOptionValue(CliOptions.PLUGIN_JAR_LONG),
              System.getProperty(IOTDB_HOME_PROPERTY));
    }

    private String resolvePassword(CommandLine line, String opt, String prompt, String defaultPw) {
      if (!line.hasOption(opt)) {
        return defaultPw;
      }
      final String value = line.getOptionValue(opt);
      return StringUtils.isNotBlank(value) ? value : CliAuthHelper.readPasswordInteractive(prompt);
    }

    private String resolveRemotePassword(CommandLine line, String prompt) {
      // If -tpw/--target_host_pw is present with a value: use it.
      // In all other cases, always prompt interactively.
      if (line.hasOption(CliOptions.TARGET_PW_OPT)) {
        final String value = line.getOptionValue(CliOptions.TARGET_PW_OPT);
        return StringUtils.isNotBlank(value)
            ? value
            : CliAuthHelper.readPasswordInteractive(prompt);
      }
      return CliAuthHelper.readPasswordInteractive(prompt);
    }

    private boolean parseSqlDialect(String dialect) throws ParseException {
      if (dialect == null) {
        throw new ParseException("sql_dialect (-sql_dialect) is required.");
      }
      dialect = dialect.trim().toLowerCase();
      if (Constants.SQL_DIALECT_VALUE_TREE.equals(dialect)) {
        return true;
      }
      if (Constants.SQL_DIALECT_VALUE_TABLE.equals(dialect)) {
        return false;
      }
      throw new ParseException("sql_dialect must be 'tree' or 'table'.");
    }

    private String requireNonBlank(String val, String errMsg) {
      if (StringUtils.isBlank(val)) {
        throw new IllegalArgumentException(errMsg);
      }
      return val.trim();
    }
  }

  // ==============================================================================================
  // 2. Parser: CLI parsing and help rendering
  // ==============================================================================================
  private static class CliParser {
    private static final Options OPTIONS = buildOptions();
    private static final String CLI_SCRIPT_NAME = "tsfile-backup.sh";

    public static BackupConfig parse(String[] args) throws ParseException {
      CommandLine line = new DefaultParser().parse(OPTIONS, args, false);
      if (line.hasOption(CliOptions.HELP_LONG)) {
        printHelp();
        return null;
      }
      return new BackupConfig(line);
    }

    public static void printHelp() {
      HelpFormatter help = new HelpFormatter();
      help.setWidth(Constants.MAX_HELP_CONSOLE_WIDTH);
      help.printHelp(CLI_SCRIPT_NAME, OPTIONS, true);
    }

    private static Options buildOptions() {
      Options o = new Options();
      o.addOption(
          Option.builder(CliOptions.HELP_OPT)
              .longOpt(CliOptions.HELP_LONG)
              .desc(CliOptions.HELP_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.HOST_OPT)
              .longOpt(CliOptions.HOST_LONG)
              .hasArg()
              .argName(CliOptions.HOST_ARG)
              .desc(CliOptions.HOST_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.PORT_OPT)
              .longOpt(CliOptions.PORT_LONG)
              .hasArg()
              .argName(CliOptions.PORT_ARG)
              .desc(CliOptions.PORT_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.USER_OPT)
              .longOpt(CliOptions.USER_LONG)
              .hasArg()
              .argName(CliOptions.USER_ARG)
              .desc(CliOptions.USER_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.PW_OPT)
              .longOpt(CliOptions.PW_LONG)
              .hasArg()
              .optionalArg(true)
              .argName(CliOptions.PW_ARG)
              .desc(CliOptions.PW_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.SQL_OPT)
              .longOpt(CliOptions.SQL_LONG)
              .required()
              .hasArg()
              .argName(CliOptions.SQL_ARG)
              .desc(CliOptions.SQL_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.PATH_OPT)
              .longOpt(CliOptions.PATH_LONG)
              .hasArg()
              .argName(CliOptions.PATH_ARG)
              .desc(CliOptions.PATH_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.DB_OPT)
              .longOpt(CliOptions.DB_LONG)
              .hasArg()
              .argName(CliOptions.DB_ARG)
              .desc(CliOptions.DB_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TABLE_OPT)
              .longOpt(CliOptions.TABLE_LONG)
              .hasArg()
              .argName(CliOptions.TABLE_ARG)
              .desc(CliOptions.TABLE_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.START_TIME_OPT)
              .longOpt(CliOptions.START_TIME_LONG)
              .hasArg()
              .argName(CliOptions.START_TIME_ARG)
              .desc(CliOptions.START_TIME_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.END_TIME_OPT)
              .longOpt(CliOptions.END_TIME_LONG)
              .hasArg()
              .argName(CliOptions.END_TIME_ARG)
              .desc(CliOptions.END_TIME_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TARGET_DIR_OPT)
              .longOpt(CliOptions.TARGET_DIR_LONG)
              .required()
              .hasArg()
              .argName(CliOptions.TARGET_DIR_ARG)
              .desc(CliOptions.TARGET_DIR_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TARGET_HOST_OPT)
              .longOpt(CliOptions.TARGET_HOST_LONG)
              .hasArg()
              .argName(CliOptions.TARGET_HOST_ARG)
              .desc(CliOptions.TARGET_HOST_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TARGET_USER_OPT)
              .longOpt(CliOptions.TARGET_USER_LONG)
              .hasArg()
              .argName(CliOptions.TARGET_USER_ARG)
              .desc(CliOptions.TARGET_USER_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TARGET_PW_OPT)
              .longOpt(CliOptions.TARGET_PW_LONG)
              .hasArg()
              .optionalArg(true)
              .argName(CliOptions.TARGET_PW_ARG)
              .desc(CliOptions.TARGET_PW_DESC)
              .build());
      o.addOption(
          Option.builder(CliOptions.TARGET_PORT_OPT)
              .longOpt(CliOptions.TARGET_PORT_LONG)
              .hasArg()
              .argName(CliOptions.TARGET_PORT_ARG)
              .desc(CliOptions.TARGET_PORT_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.RATE_LIMIT_LONG)
              .hasArg()
              .argName(CliOptions.RATE_LIMIT_ARG)
              .desc(CliOptions.RATE_LIMIT_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.OBJECT_BATCH_SIZE_LONG)
              .hasArg()
              .argName(CliOptions.OBJECT_BATCH_SIZE_ARG)
              .desc(CliOptions.OBJECT_BATCH_SIZE_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.OBJECT_PARALLELISM_LONG)
              .hasArg()
              .argName(CliOptions.OBJECT_PARALLELISM_ARG)
              .desc(CliOptions.OBJECT_PARALLELISM_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.OBJECT_WAITING_QUEUE_SIZE_LONG)
              .hasArg()
              .argName(CliOptions.OBJECT_WAITING_QUEUE_SIZE_ARG)
              .desc(CliOptions.OBJECT_WAITING_QUEUE_SIZE_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.OBJECT_THREAD_KEEP_ALIVE_SECONDS_LONG)
              .hasArg()
              .argName(CliOptions.OBJECT_THREAD_KEEP_ALIVE_SECONDS_ARG)
              .desc(CliOptions.OBJECT_THREAD_KEEP_ALIVE_SECONDS_DESC)
              .build());
      o.addOption(
          Option.builder()
              .longOpt(CliOptions.PLUGIN_JAR_LONG)
              .hasArg()
              .argName(CliOptions.PLUGIN_JAR_ARG)
              .desc(CliOptions.PLUGIN_JAR_DESC)
              .build());
      return o;
    }
  }

  // ==============================================================================================
  // 3. Executor: session lifecycle and RPC interaction
  // ==============================================================================================
  private static class IoTDBPipeExecutor {
    private static final String SQL_SHOW_PLUGINS = "SHOW PIPEPLUGINS";
    private static final String PIPE_PREFIX = "tsfile_backup_";

    public static void execute(BackupConfig config)
        throws IoTDBConnectionException, StatementExecutionException {
      String dialect =
          config.isTreeModel ? Constants.SQL_DIALECT_VALUE_TREE : Constants.SQL_DIALECT_VALUE_TABLE;
      try (Session session =
          new Session.Builder()
              .host(config.host)
              .port(config.port)
              .username(config.user)
              .password(config.password)
              .sqlDialect(dialect)
              .build()) {
        session.open(false);
        registerPluginIfNeeded(session, config.pluginJar);
        submitPipeTask(session, config);
      }
    }

    private static void registerPluginIfNeeded(Session session, File pluginJar)
        throws IoTDBConnectionException, StatementExecutionException {
      if (isPluginRegistered(session)) {
        OUT.println("[INFO] Pipe plugin " + SINK_PLUGIN_NAME + " already exists.");
        return;
      }
      OUT.println("[INFO] Pipe plugin " + SINK_PLUGIN_NAME + " not found, creating...");
      String uri = pluginJar.toPath().toAbsolutePath().normalize().toUri().toASCIIString();
      String createPluginSql = PipeSqlBuilder.buildCreatePluginSql(uri);
      session.executeNonQueryStatement(createPluginSql);
    }

    private static void submitPipeTask(Session session, BackupConfig config)
        throws IoTDBConnectionException, StatementExecutionException {
      String pipeName = PIPE_PREFIX + System.currentTimeMillis();
      String createPipeSql = PipeSqlBuilder.buildCreatePipeSql(pipeName, config);
      session.executeNonQueryStatement(createPipeSql);
      OUT.println("[INFO] Authentication successful. Pipe task submitted.");
      OUT.println("[INFO] Pipe name: " + pipeName);
    }

    private static boolean isPluginRegistered(Session session)
        throws IoTDBConnectionException, StatementExecutionException {
      try (SessionDataSet ds = session.executeQueryStatement(SQL_SHOW_PLUGINS)) {
        SessionDataSet.DataIterator it = ds.iterator();
        while (it.next()) {
          String n = readPluginNameCell(it);
          if (SINK_PLUGIN_NAME.equals(n)) {
            return true;
          }
        }
      }
      return false;
    }

    private static String readPluginNameCell(SessionDataSet.DataIterator it) {
      try {
        if (!it.isNull(ColumnHeaderConstant.PLUGIN_NAME)) {
          return it.getString(ColumnHeaderConstant.PLUGIN_NAME);
        }
      } catch (Exception ignored) {
      }
      try {
        if (!it.isNull(ColumnHeaderConstant.PLUGIN_NAME_TABLE_MODEL)) {
          return it.getString(ColumnHeaderConstant.PLUGIN_NAME_TABLE_MODEL);
        }
      } catch (Exception ignored) {
      }
      return null;
    }
  }

  // ==============================================================================================
  // 4. SQL Builder: isolated SQL generation
  // ==============================================================================================
  private static class PipeSqlBuilder {

    public static String buildCreatePluginSql(String uri) {
      return String.format(
          "CREATE PIPEPLUGIN IF NOT EXISTS %s AS '%s' USING URI '%s'",
          SINK_PLUGIN_NAME, SINK_PLUGIN_CLASS, uri);
    }

    public static String buildCreatePipeSql(String pipeName, BackupConfig config) {
      List<String> source = new ArrayList<>();
      // Snapshot + history TsFile extraction, realtime disabled (heartbeat only): no tablet stream.
      source.add(formatKv(PipeKeys.SOURCE_MODE, PipeKeys.SOURCE_MODE_SNAPSHOT));
      source.add(formatKv(PipeKeys.SOURCE_HISTORY_ENABLE, "true"));
      source.add(formatKv(PipeKeys.SOURCE_REALTIME_ENABLE, "false"));
      source.add(formatKv(PipeKeys.SOURCE_INCLUSION, PipeKeys.SOURCE_INCLUSION_INSERT));
      source.add(formatKv(PipeKeys.SOURCE_MODS_ENABLE, "true"));
      if (StringUtils.isNotBlank(config.startTime)) {
        source.add(formatKv(PipeKeys.SOURCE_START_TIME, config.startTime.trim()));
      }
      if (StringUtils.isNotBlank(config.endTime)) {
        source.add(formatKv(PipeKeys.SOURCE_END_TIME, config.endTime.trim()));
      }

      if (config.isTreeModel) {
        source.add(formatKv(PipeKeys.SOURCE_CAPTURE_TREE, "true"));
        source.add(formatKv(PipeKeys.SOURCE_CAPTURE_TABLE, "false"));
        source.add(formatKv(PipeKeys.SOURCE_PATTERN, config.treePath));
      } else {
        source.add(formatKv(PipeKeys.SOURCE_CAPTURE_TABLE, "true"));
        source.add(formatKv(PipeKeys.SOURCE_CAPTURE_TREE, "false"));
        source.add(formatKv(PipeKeys.SOURCE_DB_NAME, config.dbRegex));
        source.add(formatKv(PipeKeys.SOURCE_TABLE_NAME, config.tableRegex));
      }

      List<String> sink = new ArrayList<>();
      sink.add(formatKv(PipeKeys.SINK, SINK_PLUGIN_NAME));
      sink.add(formatKv(PipeKeys.SINK_SCP_HOST, config.scpHost));
      sink.add(formatKv(PipeKeys.SINK_SCP_PORT, String.valueOf(config.sshPort)));
      sink.add(formatKv(PipeKeys.SINK_SCP_USER, config.scpUser));
      sink.add(formatKv(PipeKeys.SINK_SCP_PASSWORD, config.scpPassword));
      sink.add(formatKv(PipeKeys.SINK_SCP_REMOTE_PATH, config.targetDir));

      if (config.rateLimitBytesPerSecond != null
          && config.rateLimitBytesPerSecond > 0
          && !config.rateLimitBytesPerSecond.isNaN()
          && !config.rateLimitBytesPerSecond.isInfinite()) {
        sink.add(
            formatKv(PipeKeys.SINK_RATE_LIMIT, String.valueOf(config.rateLimitBytesPerSecond)));
      }
      if (config.objectBatchSizeBytes != null) {
        sink.add(
            formatKv(
                PipeKeys.SINK_SCP_OBJECT_BATCH_SIZE_BYTES,
                String.valueOf(config.objectBatchSizeBytes)));
      }
      if (config.objectParallelism != null) {
        sink.add(
            formatKv(
                PipeKeys.SINK_SCP_OBJECT_PARALLELISM, String.valueOf(config.objectParallelism)));
      }
      if (config.objectWaitingQueueSize != null) {
        sink.add(
            formatKv(
                PipeKeys.SINK_SCP_OBJECT_WAITING_QUEUE_SIZE,
                String.valueOf(config.objectWaitingQueueSize)));
      }
      if (config.objectThreadKeepAliveSeconds != null) {
        sink.add(
            formatKv(
                PipeKeys.SINK_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS,
                String.valueOf(config.objectThreadKeepAliveSeconds)));
      }

      return String.format(
          "CREATE PIPE IF NOT EXISTS %s WITH SOURCE (%s) WITH SINK (%s)",
          pipeName, String.join(", ", source), String.join(", ", sink));
    }

    /** Formats one SQL key-value pair. */
    private static String formatKv(String key, String value) {
      return String.format("'%s'='%s'", key, value);
    }
  }

  // ==============================================================================================
  // 5. Utility layer: terminal auth and network helpers
  // ==============================================================================================
  private static class CliAuthHelper {
    public static String readPasswordInteractive(String prompt) {
      CliContext ctx = new CliContext(System.in, System.out, System.err, ExitType.SYSTEM_EXIT);
      try {
        ctx.setLineReader(
            JlineUtils.getLineReader(
                ctx,
                Constants.USERNAME_DEFAULT_VALUE,
                Constants.HOST_DEFAULT_VALUE,
                Constants.PORT_DEFAULT_VALUE));
        String line = ctx.getLineReader().readLine(prompt, '\0');
        return line == null ? "" : line;
      } catch (IOException e) {
        throw new RuntimeException("Failed to read password from console", e);
      }
    }
  }

  static File resolvePluginJar(String cliOverride, String iotdbHome) {
    String configuredJarPath = normalizePathSetting(cliOverride);
    if (StringUtils.isNotBlank(configuredJarPath)) {
      File configuredPluginJar = new File(configuredJarPath);
      if (configuredPluginJar.isFile()) {
        return configuredPluginJar;
      }
      throw new IllegalArgumentException("Specified plugin JAR not found: " + configuredJarPath);
    }

    File defaultPluginDir = resolveDefaultPluginDir(iotdbHome);
    File defaultPluginJar = findPluginJarInDirectory(defaultPluginDir);
    if (defaultPluginJar != null) {
      return defaultPluginJar;
    }

    throw new IllegalArgumentException(buildMissingPluginJarMessage(defaultPluginDir));
  }

  private static File resolveDefaultPluginDir(String iotdbHome) {
    iotdbHome = normalizePathSetting(iotdbHome);
    if (StringUtils.isBlank(iotdbHome)) {
      return null;
    }
    return new File(iotdbHome, DEFAULT_PLUGIN_DIR);
  }

  private static File findPluginJarInDirectory(File pluginDir) {
    if (pluginDir == null || !pluginDir.isDirectory()) {
      return null;
    }

    File[] pluginJars =
        pluginDir.listFiles(
            (dir, name) ->
                name.startsWith(DEFAULT_PLUGIN_JAR_PREFIX)
                    && name.endsWith(DEFAULT_PLUGIN_JAR_SUFFIX));
    if (pluginJars == null || pluginJars.length == 0) {
      return null;
    }

    Arrays.sort(pluginJars, Comparator.comparing(File::getName));
    return pluginJars[0];
  }

  private static String buildMissingPluginJarMessage(File defaultPluginDir) {
    String expectedJarName = DEFAULT_PLUGIN_JAR_PREFIX + "*" + DEFAULT_PLUGIN_JAR_SUFFIX;
    if (defaultPluginDir == null) {
      return "Pipe plugin JAR does not exist: --plugin_jar is not configured, and the default directory "
          + DEFAULT_PLUGIN_DIR
          + " cannot be resolved because "
          + IOTDB_HOME_PROPERTY
          + " is not set. Expected jar pattern: "
          + expectedJarName
          + ".";
    }
    return "Pipe plugin JAR does not exist: --plugin_jar is not configured, and the default directory "
        + defaultPluginDir.getAbsolutePath()
        + " does not contain "
        + expectedJarName
        + ".";
  }

  private static String normalizePathSetting(String path) {
    if (StringUtils.isBlank(path)) {
      return "";
    }

    String normalized = path.trim();
    if ((normalized.startsWith("\"") && normalized.endsWith("\""))
        || (normalized.startsWith("'") && normalized.endsWith("'"))) {
      normalized = normalized.substring(1, normalized.length() - 1).trim();
    }
    return normalized;
  }

  private static String getLocalIpv4() {
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        if (!nic.isUp() || nic.isLoopback()) {
          continue;
        }
        Enumeration<InetAddress> addrs = nic.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress a = addrs.nextElement();
          if (a instanceof Inet4Address && !a.isLoopbackAddress()) {
            return a.getHostAddress();
          }
        }
      }
    } catch (Exception ignored) {
    }
    return DefaultValues.FALLBACK_IP;
  }
}
