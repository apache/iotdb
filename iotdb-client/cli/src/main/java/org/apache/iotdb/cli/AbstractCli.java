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

import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.tool.data.ImportData;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.jdbc.Config.SQL_DIALECT;

public abstract class AbstractCli {

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  static final String PW_ARGS = "pw";
  private static final String PW_NAME = "password";

  static final String USERNAME_ARGS = "u";
  static final String USERNAME_NAME = "username";

  private static final String EXECUTE_ARGS = "e";

  static final String USE_SSL_ARGS = "usessl";
  static final String TRUST_STORE_ARGS = "ts";

  static final String TRUST_STORE_PWD_ARGS = "tpw";

  private static final String EXECUTE_NAME = "execute";

  private static final String USE_SSL = "use_ssl";
  private static final String TRUST_STORE = "trust_store";

  private static final String TRUST_STORE_PWD = "trust_store_pwd";
  private static final String NULL = "null";

  static final int CODE_OK = 0;
  static final int CODE_ERROR = 1;

  static final String ISO8601_ARGS = "disableISO8601";
  static final List<String> AGGREGRATE_TIME_LIST = new ArrayList<>();
  static final String RPC_COMPRESS_ARGS = "c";
  private static final String RPC_COMPRESS_NAME = "rpcCompressed";
  static final String TIMEOUT_ARGS = "timeout";
  private static final String TIMEOUT_NAME = "queryTimeout";
  static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";
  static final String SHOW_TIMESTAMP_DISPLAY = "show time_display_type";
  static final String SET_TIME_ZONE = "set time_zone";
  static final String SHOW_TIMEZONE = "show time_zone";
  static final String SET_FETCH_SIZE = "set fetch_size";
  static final String SHOW_FETCH_SIZE = "show fetch_size";
  private static final String HELP = "help";
  static final String IOTDB_CLI_PREFIX = "IoTDB";
  static final String SCRIPT_HINT = "./start-cli.sh(start-cli.bat if Windows)";
  static final String QUIT_COMMAND = "quit";
  static final String EXIT_COMMAND = "exit";
  private static final String SHOW_METADATA_COMMAND = "show timeseries";
  static final int MAX_HELP_CONSOLE_WIDTH = 88;
  static final String TIMESTAMP_STR = "Time";
  private static final String IMPORT_CMD = "import";
  static int maxPrintRowCount = 1000;
  private static int fetchSize = 1000;
  static int queryTimeout = 0;
  static String timestampPrecision = "ms";
  static String timeFormat = RpcUtils.DEFAULT_TIME_FORMAT;
  private static boolean continuePrint = false;

  private static int lineCount = 0;
  private static final String SUCCESS_MESSAGE = "The statement is executed successfully.";

  private static boolean isReachEnd = false;

  static String host = "127.0.0.1";
  static String port = "6667";
  static String username;
  // TODO: Make non-static
  static String password;
  // TODO: Make non-static
  static String useSsl;
  // TODO: Make non-static
  static String trustStore;
  // TODO: Make non-static
  static String trustStorePwd;

  static String execute;
  static boolean hasExecuteSQL = false;

  static Set<String> keywordSet = new HashSet<>();

  static ServerProperties properties = null;

  private static boolean cursorBeforeFirst = true;

  static int lastProcessStatus = CODE_OK;

  static String sqlDialect = "tree";

  static void init() {
    keywordSet.add("-" + HOST_ARGS);
    keywordSet.add("-" + HELP_ARGS);
    keywordSet.add("-" + PORT_ARGS);
    keywordSet.add("-" + PW_ARGS);
    keywordSet.add("-" + USERNAME_ARGS);
    keywordSet.add("-" + USE_SSL_ARGS);
    keywordSet.add("-" + TRUST_STORE_ARGS);
    keywordSet.add("-" + TRUST_STORE_PWD_ARGS);
    keywordSet.add("-" + EXECUTE_ARGS);
    keywordSet.add("-" + ISO8601_ARGS);
    keywordSet.add("-" + RPC_COMPRESS_ARGS);
  }

  static Options createOptions() {
    Options options = new Options();
    Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
    help.setRequired(false);
    options.addOption(help);

    Option timeFormat = new Option(ISO8601_ARGS, false, "Display timestamp in number(optional)");
    timeFormat.setRequired(false);
    options.addOption(timeFormat);

    Option host =
        Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (optional, default 127.0.0.1)")
            .build();
    options.addOption(host);

    Option port =
        Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (optional, default 6667)")
            .build();
    options.addOption(port);

    Option username =
        Option.builder(USERNAME_ARGS)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc("User name (required)")
            .required()
            .build();
    options.addOption(username);

    Option password =
        Option.builder(PW_ARGS).argName(PW_NAME).hasArg().desc("password (optional)").build();
    options.addOption(password);

    Option useSSL =
        Option.builder(USE_SSL_ARGS)
            .argName(USE_SSL)
            .hasArg()
            .desc("use_ssl statement (optional)")
            .build();
    options.addOption(useSSL);

    Option trustStore =
        Option.builder(TRUST_STORE_ARGS)
            .argName(TRUST_STORE)
            .hasArg()
            .desc("trust_store statement (optional)")
            .build();
    options.addOption(trustStore);

    Option trustStorePwd =
        Option.builder(TRUST_STORE_PWD_ARGS)
            .argName(TRUST_STORE_PWD)
            .hasArg()
            .desc("trust_store_pwd statement (optional)")
            .build();
    options.addOption(trustStorePwd);

    Option execute =
        Option.builder(EXECUTE_ARGS)
            .argName(EXECUTE_NAME)
            .hasArg()
            .desc("execute statement (optional)")
            .build();
    options.addOption(execute);

    Option isRpcCompressed =
        Option.builder(RPC_COMPRESS_ARGS)
            .argName(RPC_COMPRESS_NAME)
            .desc("Rpc Compression enabled or not")
            .build();
    options.addOption(isRpcCompressed);

    Option queryTimeout =
        Option.builder(TIMEOUT_ARGS)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc(
                "The timeout in second. "
                    + "Using the configuration of server if it's not set (optional)")
            .build();
    options.addOption(queryTimeout);

    Option sqlDialect =
        Option.builder(SQL_DIALECT)
            .argName(SQL_DIALECT)
            .hasArg()
            .desc("currently support tree and table, using tree if it's not set (optional)")
            .build();
    options.addOption(sqlDialect);
    return options;
  }

  static String checkRequiredArg(
      CliContext ctx,
      String arg,
      String name,
      CommandLine commandLine,
      boolean isRequired,
      String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (isRequired) {
        String msg =
            String.format(
                "%s: Required values for option '%s' not provided", IOTDB_CLI_PREFIX, name);
        ctx.getPrinter().println(msg);
        ctx.getPrinter().println("Use -help for more information");
        throw new ArgsErrorException(msg);
      } else if (defaultValue == null) {
        String msg =
            String.format("%s: Required values for option '%s' is null.", IOTDB_CLI_PREFIX, name);
        throw new ArgsErrorException(msg);
      } else {
        return defaultValue;
      }
    }
    return str;
  }

  private static void setFetchSize(String fetchSizeString) {
    long tmp = Long.parseLong(fetchSizeString.trim());
    if (tmp > Integer.MAX_VALUE || tmp < 0) {
      fetchSize = Integer.MAX_VALUE;
    } else {
      fetchSize = Integer.parseInt(fetchSizeString.trim());
    }
  }

  private static int setFetchSize(CliContext ctx, String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      ctx.getPrinter()
          .println(
              String.format("Fetch size format error, please input like %s=10000", SET_FETCH_SIZE));
      return CODE_ERROR;
    }
    try {
      setFetchSize(cmd.split("=")[1]);
    } catch (Exception e) {
      ctx.getPrinter().println(String.format("Fetch size format error, %s", e.getMessage()));
      return CODE_ERROR;
    }
    ctx.getPrinter().println("Fetch size has set to " + values[1].trim());
    return CODE_OK;
  }

  static void setQueryTimeout(String timeoutString) {
    long timeout = Long.parseLong(timeoutString.trim());
    if (timeout > Integer.MAX_VALUE || timeout < 0) {
      queryTimeout = 0;
    } else {
      queryTimeout = Integer.parseInt(timeoutString.trim());
    }
  }

  static void setSqlDialect(String sqlDialect) {
    AbstractCli.sqlDialect = sqlDialect;
  }

  static String[] removePasswordArgs(String[] args) {
    int index = -1;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + PW_ARGS)) {
        index = i;
        break;
      }
    }
    if (index >= 0
        && ((index + 1 >= args.length)
            || (index + 1 < args.length && keywordSet.contains(args[index + 1])))) {
      return ArrayUtils.remove(args, index);
    }
    return args;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  static String[] processExecuteArgs(String[] args) {
    int index = -1;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + EXECUTE_ARGS)) {
        index = i;
        break;
      }
    }
    if (index >= 0
        && ((index + 1 >= args.length)
            || (index + 1 < args.length && keywordSet.contains(args[index + 1])))) {
      return ArrayUtils.remove(args, index);
    } else if (index == -1) {
      return args;
    } else {
      StringBuilder executeCommand = new StringBuilder();
      for (int j = index + 1; j < args.length; j++) {
        executeCommand.append(args[j]).append(" ");
      }
      // remove last space
      executeCommand.deleteCharAt(executeCommand.length() - 1);
      // some bashes may not remove quotes of parameters automatically, remove them in that case
      if (executeCommand.charAt(0) == '\'' || executeCommand.charAt(0) == '\"') {
        executeCommand.deleteCharAt(0);
        if (executeCommand.charAt(executeCommand.length() - 1) == '\''
            || executeCommand.charAt(executeCommand.length() - 1) == '\"') {
          executeCommand.deleteCharAt(executeCommand.length() - 1);
        }
      }

      execute = executeCommand.toString();
      hasExecuteSQL = true;
      // When execute sql in CLI with -e mode, we should print all results by setting continuePrint
      // is true.
      continuePrint = true;
      args = Arrays.copyOfRange(args, 0, index);
      return args;
    }
  }

  static void displayLogo(CliContext ctx, String logo, String version, String buildInfo) {
    ctx.getPrinter()
        .println(
            (logo != null
                    ? logo
                    : (" _____       _________  ______   ______    \n"
                        + "|_   _|     |  _   _  ||_   _ `.|_   _ \\   \n"
                        + "  | |   .--.|_/ | | \\_|  | | `. \\ | |_) |  \n"
                        + "  | | / .'`\\ \\  | |      | |  | | |  __'.  \n"
                        + " _| |_| \\__. | _| |_    _| |_.' /_| |__) | \n"
                        + "|_____|'.__.' |_____|  |______.'|_______/  "))
                + "version "
                + version
                + " (Build: "
                + (buildInfo != null ? buildInfo : "UNKNOWN")
                + ")"
                + "\n"
                + "                                           \n");
  }

  static void echoStarting(CliContext ctx) {
    ctx.getPrinter().println("---------------------");
    ctx.getPrinter().println("Starting IoTDB Cli");
    ctx.getPrinter().println("---------------------");
  }

  static OperationResult handleInputCmd(CliContext ctx, String cmd, IoTDBConnection connection) {
    lastProcessStatus = CODE_OK;
    String specialCmd = cmd.toLowerCase().trim();

    if (QUIT_COMMAND.equals(specialCmd) || EXIT_COMMAND.equals(specialCmd)) {
      return OperationResult.STOP_OPER;
    }
    if (HELP.equals(specialCmd)) {
      showHelp(ctx);
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SET_TIMESTAMP_DISPLAY)) {
      lastProcessStatus = setTimestampDisplay(ctx, specialCmd, cmd);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SET_TIME_ZONE)) {
      lastProcessStatus = setTimeZone(ctx, specialCmd, cmd, connection);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SET_FETCH_SIZE)) {
      lastProcessStatus = setFetchSize(ctx, specialCmd, cmd);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SHOW_TIMEZONE)) {
      lastProcessStatus = showTimeZone(ctx, connection);
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SHOW_TIMESTAMP_DISPLAY)) {
      ctx.getPrinter().println("Current time format: " + timeFormat);
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SHOW_FETCH_SIZE)) {
      ctx.getPrinter().println("Current fetch size: " + fetchSize);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(IMPORT_CMD)) {
      lastProcessStatus = importCmd(ctx, specialCmd, cmd, connection);
      return OperationResult.CONTINUE_OPER;
    }

    lastProcessStatus = executeQuery(ctx, connection, cmd);
    return OperationResult.NO_OPER;
  }

  private static void showHelp(CliContext ctx) {
    ctx.getPrinter().println("    <your-sql>\t\t\t execute your sql statment");
    ctx.getPrinter()
        .println(
            String.format(
                "    %s\t\t show how many timeseries are in iotdb", SHOW_METADATA_COMMAND));
    ctx.getPrinter()
        .println(
            String.format(
                "    %s=xxx\t eg. long, default, ISO8601, yyyy-MM-dd HH:mm:ss.",
                SET_TIMESTAMP_DISPLAY));
    ctx.getPrinter()
        .println(String.format("    %s\t show time display type", SHOW_TIMESTAMP_DISPLAY));
    ctx.getPrinter()
        .println(String.format("    %s=xxx\t\t eg. +08:00, Asia/Shanghai.", SET_TIME_ZONE));
    ctx.getPrinter().println(String.format("    %s\t\t show cli time zone", SHOW_TIMEZONE));
    ctx.getPrinter()
        .println(
            String.format(
                "    %s=xxx\t\t set fetch size when querying data from server.", SET_FETCH_SIZE));
    ctx.getPrinter().println(String.format("    %s\t\t show fetch size", SHOW_FETCH_SIZE));
  }

  private static int setTimestampDisplay(CliContext ctx, String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      ctx.getPrinter()
          .println(
              String.format(
                  "Time display format error, please input like %s=ISO8601",
                  SET_TIMESTAMP_DISPLAY));
      return CODE_ERROR;
    }
    try {
      timeFormat = RpcUtils.setTimeFormat(cmd.split("=")[1]);
    } catch (Exception e) {
      ctx.getPrinter().println(String.format("time display format error, %s", e.getMessage()));
      return CODE_ERROR;
    }
    ctx.getPrinter().println("Time display type has set to " + cmd.split("=")[1].trim());
    return CODE_OK;
  }

  /**
   * If cli has not specified a zoneId, it will be set to cli's system timezone by default otherwise
   * for insert and query accuracy cli should set timezone the same for all sessions.
   *
   * @param specialCmd
   * @param cmd
   * @param connection
   * @return execute result code
   */
  private static int setTimeZone(
      CliContext ctx, String specialCmd, String cmd, IoTDBConnection connection) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      ctx.getPrinter()
          .println(
              String.format("Time zone format error, please input like %s=+08:00", SET_TIME_ZONE));
      return CODE_ERROR;
    }
    try {
      connection.setTimeZone(cmd.split("=")[1].trim());
    } catch (Exception e) {
      ctx.getPrinter().println(String.format("Time zone format error: %s", e.getMessage()));
      return CODE_ERROR;
    }
    ctx.getPrinter().println("Time zone has set to " + values[1].trim());
    return CODE_OK;
  }

  private static int showTimeZone(CliContext ctx, IoTDBConnection connection) {
    try {
      ctx.getPrinter().println("Current time zone: " + connection.getTimeZone());
    } catch (Exception e) {
      ctx.getPrinter().println("Cannot get time zone from server side because: " + e.getMessage());
      return CODE_ERROR;
    }
    return CODE_OK;
  }

  private static int importCmd(
      CliContext ctx, String specialCmd, String cmd, IoTDBConnection connection) {
    String[] values = specialCmd.split(" ");
    if (values.length != 2) {
      ctx.getPrinter()
          .println(
              "Please input like: import /User/myfile. "
                  + "Noted that your file path cannot contain any space character)");
      return CODE_ERROR;
    }
    ctx.getPrinter().println(cmd.split(" ")[1]);
    try {
      return ImportData.importFromTargetPath(
          host,
          Integer.parseInt(port),
          username,
          password,
          cmd.split(" ")[1],
          connection.getTimeZone());
    } catch (IoTDBConnectionException e) {
      ctx.getPrinter().printException(e);
      return CODE_ERROR;
    }
  }

  @SuppressWarnings({"squid:S3776"}) // Suppress high Cognitive Complexity warning
  private static int executeQuery(CliContext ctx, IoTDBConnection connection, String cmd) {
    int executeStatus = CODE_OK;
    long startTime = System.currentTimeMillis();
    try (Statement statement = connection.createStatement()) {
      ZoneId zoneId = ZoneId.of(connection.getTimeZone());
      statement.setFetchSize(fetchSize);
      boolean hasResultSet = statement.execute(cmd.trim());
      if (hasResultSet) {
        // print the result
        try (ResultSet resultSet = statement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          int columnLength = resultSetMetaData.getColumnCount();
          List<Integer> maxSizeList = new ArrayList<>(columnLength);
          List<List<String>> lists =
              cacheResult(ctx, resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
          output(ctx, lists, maxSizeList);
          long costTime = System.currentTimeMillis() - startTime;
          ctx.getPrinter().println(String.format("It costs %.3fs", costTime / 1000.0));
          while (!isReachEnd) {
            if (continuePrint) {
              maxSizeList = new ArrayList<>(columnLength);
              lists =
                  cacheResult(ctx, resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
              output(ctx, lists, maxSizeList);
              continue;
            }
            ctx.getPrinter()
                .println("This display 1000 rows. Press ENTER to show more, input 'q' to quit.");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            try {
              if ("".equals(br.readLine())) {
                maxSizeList = new ArrayList<>(columnLength);
                lists =
                    cacheResult(
                        ctx, resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
                output(ctx, lists, maxSizeList);
              } else {
                break;
              }
            } catch (IOException e) {
              ctx.getPrinter().printException(e);
              executeStatus = CODE_ERROR;
            }
          }
          // output tracing activity
          if (((IoTDBJDBCResultSet) resultSet).isSetTracingInfo()) {
            maxSizeList = new ArrayList<>(2);
            lists = cacheTracingInfo(resultSet, maxSizeList);
            outputTracingInfo(ctx, lists, maxSizeList);
          }
        }
      } else {
        ctx.getPrinter().println("Msg: " + SUCCESS_MESSAGE);
      }
    } catch (Exception e) {
      ctx.getPrinter().println("Msg: " + e);
      executeStatus = CODE_ERROR;
    } finally {
      resetArgs();
    }
    return executeStatus;
  }

  /**
   * Cache all results.
   *
   * @param resultSet jdbc resultSet
   * @param maxSizeList the longest result of every column
   * @param columnCount the number of column
   * @param resultSetMetaData jdbc resultSetMetaData
   * @param zoneId your time zone
   * @return {@literal List<List<String>> result}
   * @throws SQLException throw exception
   */
  @SuppressWarnings({"squid:S6541", "squid:S3776"}) // Suppress high Cognitive Complexity warning
  // Methods should not perform too many tasks (aka Brain method)
  private static List<List<String>> cacheResult(
      CliContext ctx,
      ResultSet resultSet,
      List<Integer> maxSizeList,
      int columnCount,
      ResultSetMetaData resultSetMetaData,
      ZoneId zoneId)
      throws SQLException {

    int j = 0;
    if (cursorBeforeFirst) {
      isReachEnd = !resultSet.next();
      cursorBeforeFirst = false;
    }

    List<List<String>> lists = new ArrayList<>(columnCount);
    int endTimeIndex = -1;
    if (resultSet instanceof IoTDBJDBCResultSet) {
      for (int i = 1; i <= columnCount; i++) {
        List<String> list = new ArrayList<>(maxPrintRowCount + 1);
        String columnLabel = resultSetMetaData.getColumnLabel(i);
        if (columnLabel.equalsIgnoreCase("__endTime")) {
          endTimeIndex = i;
        }
        list.add(columnLabel);
        lists.add(list);
        int count = ctx.getPrinter().computeHANCount(columnLabel);
        maxSizeList.add(columnLabel.length() + count);
      }

      IoTDBJDBCResultSet ioTDBJDBCResultSet = (IoTDBJDBCResultSet) resultSet;
      boolean printTimestamp = !ioTDBJDBCResultSet.isIgnoreTimeStamp();
      while (j < maxPrintRowCount && !isReachEnd) {
        for (int i = 1; i <= columnCount; i++) {
          String tmp;
          if (printTimestamp && i == 1) {
            tmp =
                RpcUtils.formatDatetime(
                    timeFormat, timestampPrecision, resultSet.getLong(TIMESTAMP_STR), zoneId);
          } else if (endTimeIndex == i) {
            tmp =
                RpcUtils.formatDatetime(
                    timeFormat, timestampPrecision, resultSet.getLong(i), zoneId);
          } else {
            tmp = getStringByColumnIndex(ioTDBJDBCResultSet, i, zoneId);
          }
          if (tmp == null) {
            tmp = NULL;
          }
          lists.get(i - 1).add(tmp);
          int count = ctx.getPrinter().computeHANCount(tmp);
          int realLength = tmp.length() + count;
          if (maxSizeList.get(i - 1) < realLength) {
            maxSizeList.set(i - 1, realLength);
          }
        }
        j++;
        isReachEnd = !resultSet.next();
      }
      return lists;
    }

    for (int i = 1; i <= columnCount; i += 2) {
      List<String> timeList = new ArrayList<>(maxPrintRowCount + 1);
      timeList.add(resultSetMetaData.getColumnLabel(i).substring(0, TIMESTAMP_STR.length()));
      lists.add(timeList);
      List<String> valueList = new ArrayList<>(maxPrintRowCount + 1);
      valueList.add(resultSetMetaData.getColumnLabel(i + 1));
      lists.add(valueList);
      maxSizeList.add(TIMESTAMP_STR.length());
      maxSizeList.add(resultSetMetaData.getColumnLabel(i + 1).length());
    }

    while (j < maxPrintRowCount && !isReachEnd) {
      for (int i = 1; i <= columnCount; i++) {
        String tmp = resultSet.getString(i);
        if (tmp == null) {
          tmp = NULL;
        }
        if (i % 2 != 0 && !tmp.equals(NULL)) {
          tmp =
              RpcUtils.formatDatetime(timeFormat, timestampPrecision, Long.parseLong(tmp), zoneId);
        }
        lists.get(i - 1).add(tmp);
        if (maxSizeList.get(i - 1) < tmp.length()) {
          maxSizeList.set(i - 1, tmp.length());
        }
      }
      j++;
      isReachEnd = !resultSet.next();
    }
    return lists;
  }

  private static String getStringByColumnIndex(
      IoTDBJDBCResultSet resultSet, int columnIndex, ZoneId zoneId) throws SQLException {
    TSDataType type = resultSet.getColumnTypeByIndex(columnIndex);
    switch (type) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TEXT:
      case STRING:
        return resultSet.getString(columnIndex);
      case BLOB:
        byte[] v = resultSet.getBytes(columnIndex);
        if (v == null) {
          return null;
        } else {
          return BytesUtils.parseBlobByteArrayToString(v);
        }
      case DATE:
        int intValue = resultSet.getInt(columnIndex);
        if (resultSet.wasNull()) {
          return null;
        } else {
          return DateUtils.formatDate(intValue);
        }
      case TIMESTAMP:
        long longValue = resultSet.getLong(columnIndex);
        if (resultSet.wasNull()) {
          return null;
        } else {
          return RpcUtils.formatDatetime(timeFormat, timestampPrecision, longValue, zoneId);
        }
      default:
        return null;
    }
  }

  private static List<List<String>> cacheTracingInfo(ResultSet resultSet, List<Integer> maxSizeList)
      throws Exception {
    List<List<String>> lists = new ArrayList<>(2);
    lists.add(0, new ArrayList<>());
    lists.add(1, new ArrayList<>());

    String activityStr = "Activity";
    String elapsedTimeStr = "Elapsed Time";
    lists.get(0).add(activityStr);
    lists.get(1).add(elapsedTimeStr);
    maxSizeList.add(0, activityStr.length());
    maxSizeList.add(1, elapsedTimeStr.length());

    List<String> activityList = ((IoTDBJDBCResultSet) resultSet).getActivityList();
    List<Long> elapsedTimeList = ((IoTDBJDBCResultSet) resultSet).getElapsedTimeList();
    String[] statisticsInfoList = {
      "seriesPathNum", "seqFileNum", "unSeqFileNum", "seqChunkInfo", "unSeqChunkInfo", "pageNumInfo"
    };

    for (int i = 0; i < activityList.size(); i++) {

      if (i == activityList.size() - 1) {
        // cache Statistics
        for (String infoName : statisticsInfoList) {
          String info = ((IoTDBJDBCResultSet) resultSet).getStatisticsInfoByName(infoName);
          lists.get(0).add(info);
          lists.get(1).add("");
          if (info.length() > maxSizeList.get(0)) {
            maxSizeList.set(0, info.length());
          }
        }
      }

      String activity = activityList.get(i);
      String elapsedTime = elapsedTimeList.get(i).toString();
      if (activity.length() > maxSizeList.get(0)) {
        maxSizeList.set(0, activity.length());
      }
      if (elapsedTime.length() > maxSizeList.get(1)) {
        maxSizeList.set(1, elapsedTime.length());
      }
      lists.get(0).add(activity);
      lists.get(1).add(elapsedTime);
    }

    return lists;
  }

  private static void output(CliContext ctx, List<List<String>> lists, List<Integer> maxSizeList) {
    ctx.getPrinter().printBlockLine(maxSizeList);
    ctx.getPrinter().printRow(lists, 0, maxSizeList);
    ctx.getPrinter().printBlockLine(maxSizeList);
    if (!lists.isEmpty()) {
      for (int i = 1; i < lists.get(0).size(); i++) {
        ctx.getPrinter().printRow(lists, i, maxSizeList);
      }
    }
    ctx.getPrinter().printBlockLine(maxSizeList);
    if (isReachEnd) {
      lineCount += lists.isEmpty() ? 0 : lists.get(0).size() - 1;
      ctx.getPrinter().printCount(lineCount);
    } else {
      lineCount += maxPrintRowCount;
    }
  }

  private static void outputTracingInfo(
      CliContext ctx, List<List<String>> lists, List<Integer> maxSizeList) {
    ctx.getPrinter().println();
    ctx.getPrinter().println("Tracing Activities:");
    ctx.getPrinter().printBlockLine(maxSizeList);
    ctx.getPrinter().printRow(lists, 0, maxSizeList);
    ctx.getPrinter().printBlockLine(maxSizeList);
    for (int i = 1; i < lists.get(0).size(); i++) {
      ctx.getPrinter().printRow(lists, i, maxSizeList);
    }
    ctx.getPrinter().printBlockLine(maxSizeList);
  }

  private static void resetArgs() {
    lineCount = 0;
    cursorBeforeFirst = true;
    isReachEnd = false;
  }

  enum OperationResult {
    STOP_OPER,
    CONTINUE_OPER,
    NO_OPER
  }

  static boolean processCommand(CliContext ctx, String s, IoTDBConnection connection) {
    if (s == null) {
      return true;
    }
    String[] cmds = s.trim().split(";");
    for (String cmd : cmds) {
      if (cmd != null && !"".equals(cmd.trim())) {
        OperationResult result = handleInputCmd(ctx, cmd, connection);
        switch (result) {
          case STOP_OPER:
            return false;
          case CONTINUE_OPER:
            continue;
          default:
            break;
        }
      }
    }
    return true;
  }
}
