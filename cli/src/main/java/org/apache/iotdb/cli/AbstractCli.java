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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.AbstractIoTDBJDBCResultSet;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.tool.ImportCsv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;

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

import static org.apache.iotdb.cli.utils.IoTPrinter.computeHANCount;
import static org.apache.iotdb.cli.utils.IoTPrinter.printBlockLine;
import static org.apache.iotdb.cli.utils.IoTPrinter.printCount;
import static org.apache.iotdb.cli.utils.IoTPrinter.printRow;
import static org.apache.iotdb.cli.utils.IoTPrinter.println;

public abstract class AbstractCli {

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  static final String PASSWORD_ARGS = "pw";
  private static final String PASSWORD_NAME = "password";

  static final String USERNAME_ARGS = "u";
  static final String USERNAME_NAME = "username";

  private static final String EXECUTE_ARGS = "e";
  private static final String EXECUTE_NAME = "execute";
  private static final String NULL = "null";

  static final String ISO8601_ARGS = "disableISO8601";
  static final List<String> AGGREGRATE_TIME_LIST = new ArrayList<>();
  static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
  private static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
  static final String RPC_COMPRESS_ARGS = "c";
  private static final String RPC_COMPRESS_NAME = "rpcCompressed";
  static final String SET_MAX_DISPLAY_NUM = "set max_display_num";
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
  static String timestampPrecision = "ms";
  static String timeFormat = RpcUtils.DEFAULT_TIME_FORMAT;
  private static boolean continuePrint = false;

  private static int lineCount = 0;
  private static final String SUCCESS_MESSAGE = "The statement is executed successfully.";

  private static boolean isReachEnd = false;

  static String host = "127.0.0.1";
  static String port = "6667";
  static String username;
  static String password;
  static String execute;
  static boolean hasExecuteSQL = false;

  static Set<String> keywordSet = new HashSet<>();

  static ServerProperties properties = null;

  private static boolean cursorBeforeFirst = true;

  static void init() {
    keywordSet.add("-" + HOST_ARGS);
    keywordSet.add("-" + HELP_ARGS);
    keywordSet.add("-" + PORT_ARGS);
    keywordSet.add("-" + PASSWORD_ARGS);
    keywordSet.add("-" + USERNAME_ARGS);
    keywordSet.add("-" + EXECUTE_ARGS);
    keywordSet.add("-" + ISO8601_ARGS);
    keywordSet.add("-" + MAX_PRINT_ROW_COUNT_ARGS);
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
        Option.builder(PASSWORD_ARGS)
            .argName(PASSWORD_NAME)
            .hasArg()
            .desc("password (optional)")
            .build();
    options.addOption(password);

    Option execute =
        Option.builder(EXECUTE_ARGS)
            .argName(EXECUTE_NAME)
            .hasArg()
            .desc("execute statement (optional)")
            .build();
    options.addOption(execute);

    Option maxPrintCount =
        Option.builder(MAX_PRINT_ROW_COUNT_ARGS)
            .argName(MAX_PRINT_ROW_COUNT_NAME)
            .hasArg()
            .desc("Maximum number of rows displayed (optional)")
            .build();
    options.addOption(maxPrintCount);

    Option isRpcCompressed =
        Option.builder(RPC_COMPRESS_ARGS)
            .argName(RPC_COMPRESS_NAME)
            .desc("Rpc Compression enabled or not")
            .build();
    options.addOption(isRpcCompressed);
    return options;
  }

  static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, boolean isRequired, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (isRequired) {
        String msg =
            String.format(
                "%s: Required values for option '%s' not provided", IOTDB_CLI_PREFIX, name);
        println(msg);
        println("Use -help for more information");
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

  static void setMaxDisplayNumber(String maxDisplayNum) {
    long tmp = Long.parseLong(maxDisplayNum.trim());
    if (tmp > Integer.MAX_VALUE) {
      throw new NumberFormatException();
    } else if (tmp <= 0) {
      continuePrint = true;
    } else {
      maxPrintRowCount = Integer.parseInt(maxDisplayNum.trim());
    }
  }

  static String[] removePasswordArgs(String[] args) {
    int index = -1;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + PASSWORD_ARGS)) {
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

  static void displayLogo(String version) {
    println(
        " _____       _________  ______   ______    \n"
            + "|_   _|     |  _   _  ||_   _ `.|_   _ \\   \n"
            + "  | |   .--.|_/ | | \\_|  | | `. \\ | |_) |  \n"
            + "  | | / .'`\\ \\  | |      | |  | | |  __'.  \n"
            + " _| |_| \\__. | _| |_    _| |_.' /_| |__) | \n"
            + "|_____|'.__.' |_____|  |______.'|_______/  version "
            + version
            + "\n"
            + "                                           \n");
  }

  static void echoStarting() {
    println("---------------------");
    println("Starting IoTDB Cli");
    println("---------------------");
  }

  static OperationResult handleInputCmd(String cmd, IoTDBConnection connection) {
    String specialCmd = cmd.toLowerCase().trim();

    if (QUIT_COMMAND.equals(specialCmd) || EXIT_COMMAND.equals(specialCmd)) {
      return OperationResult.STOP_OPER;
    }
    if (HELP.equals(specialCmd)) {
      showHelp();
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SET_TIMESTAMP_DISPLAY)) {
      setTimestampDisplay(specialCmd, cmd);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SET_TIME_ZONE)) {
      setTimeZone(specialCmd, cmd, connection);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SET_FETCH_SIZE)) {
      setFetchSize(specialCmd, cmd);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SET_MAX_DISPLAY_NUM)) {
      setMaxDisplayNum(specialCmd, cmd);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(SHOW_TIMEZONE)) {
      showTimeZone(connection);
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SHOW_TIMESTAMP_DISPLAY)) {
      println("Current time format: " + timeFormat);
      return OperationResult.CONTINUE_OPER;
    }
    if (specialCmd.startsWith(SHOW_FETCH_SIZE)) {
      println("Current fetch size: " + fetchSize);
      return OperationResult.CONTINUE_OPER;
    }

    if (specialCmd.startsWith(IMPORT_CMD)) {
      importCmd(specialCmd, cmd, connection);
      return OperationResult.CONTINUE_OPER;
    }

    executeQuery(connection, cmd);
    return OperationResult.NO_OPER;
  }

  private static void showHelp() {
    println("    <your-sql>\t\t\t execute your sql statment");
    println(
        String.format("    %s\t\t show how many timeseries are in iotdb", SHOW_METADATA_COMMAND));
    println(
        String.format(
            "    %s=xxx\t eg. long, default, ISO8601, yyyy-MM-dd HH:mm:ss.",
            SET_TIMESTAMP_DISPLAY));
    println(String.format("    %s\t show time display type", SHOW_TIMESTAMP_DISPLAY));
    println(String.format("    %s=xxx\t\t eg. +08:00, Asia/Shanghai.", SET_TIME_ZONE));
    println(String.format("    %s\t\t show cli time zone", SHOW_TIMEZONE));
    println(
        String.format(
            "    %s=xxx\t\t set fetch size when querying data from server.", SET_FETCH_SIZE));
    println(String.format("    %s\t\t show fetch size", SHOW_FETCH_SIZE));
    println(
        String.format(
            "    %s=xxx\t eg. set max lines for cli to ouput, -1 equals to unlimited.",
            SET_MAX_DISPLAY_NUM));
  }

  private static void setTimestampDisplay(String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(
          String.format(
              "Time display format error, please input like %s=ISO8601", SET_TIMESTAMP_DISPLAY));
      return;
    }
    try {
      timeFormat = RpcUtils.setTimeFormat(cmd.split("=")[1]);
    } catch (Exception e) {
      println(String.format("time display format error, %s", e.getMessage()));
      return;
    }
    println("Time display type has set to " + cmd.split("=")[1].trim());
  }

  /**
   * if cli has not specified a zondId, it will be set to cli's system timezone by default otherwise
   * for insert and query accuracy cli should set timezone the same for all sessions
   *
   * @param specialCmd
   * @param cmd
   * @param connection
   */
  private static void setTimeZone(String specialCmd, String cmd, IoTDBConnection connection) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(String.format("Time zone format error, please input like %s=+08:00", SET_TIME_ZONE));
      return;
    }
    try {
      connection.setTimeZone(cmd.split("=")[1].trim());
    } catch (Exception e) {
      println(String.format("Time zone format error: %s", e.getMessage()));
      return;
    }
    println("Time zone has set to " + values[1].trim());
  }

  private static void setFetchSize(String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(String.format("Fetch size format error, please input like %s=10000", SET_FETCH_SIZE));
      return;
    }
    try {
      setFetchSize(cmd.split("=")[1]);
    } catch (Exception e) {
      println(String.format("Fetch size format error, %s", e.getMessage()));
      return;
    }
    println("Fetch size has set to " + values[1].trim());
  }

  private static void setMaxDisplayNum(String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(
          String.format(
              "Max display number format error, please input like %s = 10000",
              SET_MAX_DISPLAY_NUM));
      return;
    }
    try {
      setMaxDisplayNumber(cmd.split("=")[1]);
    } catch (Exception e) {
      println(String.format("Max display number format error, %s", e.getMessage()));
      return;
    }
    println("Max display number has set to " + values[1].trim());
  }

  private static void showTimeZone(IoTDBConnection connection) {
    try {
      println("Current time zone: " + connection.getTimeZone());
    } catch (Exception e) {
      println("Cannot get time zone from server side because: " + e.getMessage());
    }
  }

  private static void importCmd(String specialCmd, String cmd, IoTDBConnection connection) {
    String[] values = specialCmd.split(" ");
    if (values.length != 2) {
      println(
          "Please input like: import /User/myfile. "
              + "Noted that your file path cannot contain any space character)");
      return;
    }
    println(cmd.split(" ")[1]);
    try {
      ImportCsv.importFromTargetPath(
          host,
          Integer.valueOf(port),
          username,
          password,
          cmd.split(" ")[1],
          connection.getTimeZone());
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  private static void executeQuery(IoTDBConnection connection, String cmd) {
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
              cacheResult(resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
          output(lists, maxSizeList);
          long costTime = System.currentTimeMillis() - startTime;
          println(String.format("It costs %.3fs", costTime / 1000.0));
          while (!isReachEnd) {
            if (continuePrint) {
              maxSizeList = new ArrayList<>(columnLength);
              lists = cacheResult(resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
              output(lists, maxSizeList);
              continue;
            }
            println(
                String.format(
                    "Reach the max_display_num = %s. Press ENTER to show more, input 'q' to quit.",
                    maxPrintRowCount));
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            try {
              if (br.readLine().equals("")) {
                maxSizeList = new ArrayList<>(columnLength);
                lists =
                    cacheResult(resultSet, maxSizeList, columnLength, resultSetMetaData, zoneId);
                output(lists, maxSizeList);
              } else {
                break;
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          // output tracing activity
          if (((AbstractIoTDBJDBCResultSet) resultSet).isSetTracingInfo()) {
            maxSizeList = new ArrayList<>(2);
            lists = cacheTracingInfo(resultSet, maxSizeList);
            outputTracingInfo(lists, maxSizeList);
          }
        }
      } else {
        println("Msg: " + SUCCESS_MESSAGE);
      }
    } catch (Exception e) {
      println("Msg: " + e.getMessage());
    } finally {
      resetArgs();
    }
  }

  /**
   * cache all results
   *
   * @param resultSet jdbc resultSet
   * @param maxSizeList the longest result of every column
   * @param columnCount the number of column
   * @param resultSetMetaData jdbc resultSetMetaData
   * @param zoneId your time zone
   * @return List<List<String>> result
   * @throws SQLException throw exception
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static List<List<String>> cacheResult(
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
    if (resultSet instanceof IoTDBJDBCResultSet) {
      for (int i = 1; i <= columnCount; i++) {
        List<String> list = new ArrayList<>(maxPrintRowCount + 1);
        String columnLabel = resultSetMetaData.getColumnLabel(i);
        list.add(columnLabel);
        lists.add(list);
        int count = computeHANCount(columnLabel);
        maxSizeList.add(columnLabel.length() + count);
      }

      boolean printTimestamp = !((IoTDBJDBCResultSet) resultSet).isIgnoreTimeStamp();
      while (j < maxPrintRowCount && !isReachEnd) {
        for (int i = 1; i <= columnCount; i++) {
          String tmp;
          if (printTimestamp && i == 1) {
            tmp =
                RpcUtils.formatDatetime(
                    timeFormat, timestampPrecision, resultSet.getLong(TIMESTAMP_STR), zoneId);
          } else {
            tmp = resultSet.getString(i);
          }
          if (tmp == null) {
            tmp = NULL;
          }
          lists.get(i - 1).add(tmp);
          int count = computeHANCount(tmp);
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

  private static List<List<String>> cacheTracingInfo(ResultSet resultSet, List<Integer> maxSizeList)
      throws Exception {
    List<List<String>> lists = new ArrayList<>(2);
    lists.add(0, new ArrayList<>());
    lists.add(1, new ArrayList<>());

    String ACTIVITY_STR = "Activity";
    String ELAPSED_TIME_STR = "Elapsed Time";
    lists.get(0).add(ACTIVITY_STR);
    lists.get(1).add(ELAPSED_TIME_STR);
    maxSizeList.add(0, ACTIVITY_STR.length());
    maxSizeList.add(1, ELAPSED_TIME_STR.length());

    List<String> activityList = ((AbstractIoTDBJDBCResultSet) resultSet).getActivityList();
    List<Long> elapsedTimeList = ((AbstractIoTDBJDBCResultSet) resultSet).getElapsedTimeList();
    String[] statisticsInfoList = {
      "seriesPathNum", "seqFileNum", "unSeqFileNum", "seqChunkInfo", "unSeqChunkInfo", "pageNumInfo"
    };

    for (int i = 0; i < activityList.size(); i++) {

      if (i == activityList.size() - 1) {
        // cache Statistics
        for (String infoName : statisticsInfoList) {
          String info = ((AbstractIoTDBJDBCResultSet) resultSet).getStatisticsInfoByName(infoName);
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

  private static void output(List<List<String>> lists, List<Integer> maxSizeList) {
    printBlockLine(maxSizeList);
    printRow(lists, 0, maxSizeList);
    printBlockLine(maxSizeList);
    for (int i = 1; i < lists.get(0).size(); i++) {
      printRow(lists, i, maxSizeList);
    }
    printBlockLine(maxSizeList);
    if (isReachEnd) {
      lineCount += lists.get(0).size() - 1;
      printCount(lineCount);
    } else {
      lineCount += maxPrintRowCount;
    }
  }

  private static void outputTracingInfo(List<List<String>> lists, List<Integer> maxSizeList) {
    println();
    println("Tracing Activties:");
    printBlockLine(maxSizeList);
    printRow(lists, 0, maxSizeList);
    printBlockLine(maxSizeList);
    for (int i = 1; i < lists.get(0).size(); i++) {
      printRow(lists, i, maxSizeList);
    }
    printBlockLine(maxSizeList);
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

  static boolean processCommand(String s, IoTDBConnection connection) {
    if (s == null) {
      return true;
    }
    String[] cmds = s.trim().split(";");
    for (String cmd : cmds) {
      if (cmd != null && !"".equals(cmd.trim())) {
        OperationResult result = handleInputCmd(cmd, connection);
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
