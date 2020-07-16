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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.tool.ImportCsv;
import org.apache.thrift.TException;

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
  static final int ISO_DATETIME_LEN = 35;
  private static final String IMPORT_CMD = "import";
  private static final String DEFAULT_TIME_FORMAT = "default";
  private static String timeFormat = DEFAULT_TIME_FORMAT;
  static int maxPrintRowCount = 1000;
  private static int fetchSize = 1000;
  static int maxTimeLength = ISO_DATETIME_LEN;
  static int maxValueLength = 15;
  static String TIMESTAMP_PRECISION = "ms";
  private static int lineCount = 0;
  private static final String SUCCESS_MESSAGE = "The statement is executed successfully.";

  private static boolean isReachEnd = false;

  static String formatTime = "%" + maxTimeLength + "s|";
  static String host = "127.0.0.1";
  static String port = "6667";
  static String username;
  static String password;
  static String execute;
  static boolean hasExecuteSQL = false;

  static Set<String> keywordSet = new HashSet<>();

  static ServerProperties properties = null;

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
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


  private static String getTimestampPrecision() {
    return TIMESTAMP_PRECISION;
  }

  private static void printCount(int cnt) {
    if (cnt == 0) {
      println("Empty set.");
    } else {
      println("Total line number = " + cnt);
    }
  }

  static Options createOptions() {
    Options options = new Options();
    Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
    help.setRequired(false);
    options.addOption(help);

    Option timeFormat = new Option(ISO8601_ARGS, false, "Display timestamp in number(optional)");
    timeFormat.setRequired(false);
    options.addOption(timeFormat);

    Option host = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg()
        .desc("Host Name (optional, default 127.0.0.1)").build();
    options.addOption(host);

    Option port = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg()
        .desc("Port (optional, default 6667)")
        .build();
    options.addOption(port);

    Option username = Option.builder(USERNAME_ARGS).argName(USERNAME_NAME).hasArg()
        .desc("User name (required)")
        .required().build();
    options.addOption(username);

    Option password = Option.builder(PASSWORD_ARGS).argName(PASSWORD_NAME).hasArg()
        .desc("password (optional)")
        .build();
    options.addOption(password);

    Option execute = Option.builder(EXECUTE_ARGS).argName(EXECUTE_NAME).hasArg()
        .desc("execute statement (optional)")
        .build();
    options.addOption(execute);

    Option maxPrintCount = Option.builder(MAX_PRINT_ROW_COUNT_ARGS)
        .argName(MAX_PRINT_ROW_COUNT_NAME).hasArg()
        .desc("Maximum number of rows displayed (optional)").build();
    options.addOption(maxPrintCount);

    Option isRpcCompressed = Option.builder(RPC_COMPRESS_ARGS)
        .argName(RPC_COMPRESS_NAME)
        .desc("Rpc Compression enabled or not").build();
    options.addOption(isRpcCompressed);
    return options;
  }

  public static String parseLongToDateWithPrecision(DateTimeFormatter formatter,
      long timestamp, ZoneId zoneid, String timestampPrecision) {
    if (timestampPrecision.equals("ms")) {
      long integerofDate = timestamp / 1000;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000));
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 3) {
        for (int i = 0; i < 3 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
    } else if (timestampPrecision.equals("us")) {
      long integerofDate = timestamp / 1000_000;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000_000));
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 6) {
        for (int i = 0; i < 6 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
    } else {
      long integerofDate = timestamp / 1000_000_000L;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000_000_000L));
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 9) {
        for (int i = 0; i < 9 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
    }
  }

  private static String formatDatetime(long timestamp, ZoneId zoneId) {
    ZonedDateTime dateTime;
    switch (timeFormat) {
      case "long":
      case "number":
        return Long.toString(timestamp);
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        return parseLongToDateWithPrecision(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME, timestamp, zoneId, getTimestampPrecision());
      default:
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
        return dateTime.format(DateTimeFormatter.ofPattern(timeFormat));
    }
  }

  static String checkRequiredArg(String arg, String name, CommandLine commandLine,
      boolean isRequired,
      String defaultValue) throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (isRequired) {
        String msg = String
            .format("%s: Required values for option '%s' not provided", IOTDB_CLI_PREFIX, name);
        println(msg);
        println("Use -help for more information");
        throw new ArgsErrorException(msg);
      } else if (defaultValue == null) {
        String msg = String
            .format("%s: Required values for option '%s' is null.", IOTDB_CLI_PREFIX, name);
        throw new ArgsErrorException(msg);
      } else {
        return defaultValue;
      }
    }
    return str;
  }

  static void setTimeFormat(String newTimeFormat) {
    switch (newTimeFormat.trim().toLowerCase()) {
      case "long":
      case "number":
        maxTimeLength = maxValueLength;
        timeFormat = newTimeFormat.trim().toLowerCase();
        break;
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        maxTimeLength = ISO_DATETIME_LEN;
        timeFormat = newTimeFormat.trim().toLowerCase();
        break;
      default:
        // use java default SimpleDateFormat to check whether input time format is legal
        // if illegal, it will throw an exception
        new SimpleDateFormat(newTimeFormat.trim());
        maxTimeLength = Math.max(TIMESTAMP_STR.length(), newTimeFormat.length());
        timeFormat = newTimeFormat;
        break;
    }
    formatTime = "%" + maxTimeLength + "s|";
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
    if (tmp > Integer.MAX_VALUE || tmp < 0) {
      maxPrintRowCount = Integer.MAX_VALUE;
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
    if (index >= 0 && ((index + 1 >= args.length) || (index + 1 < args.length && keywordSet
        .contains(args[index + 1])))) {
      return ArrayUtils.remove(args, index);
    }
    return args;
  }

  static String[] processExecuteArgs(String[] args) {
    int index = -1;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + EXECUTE_ARGS)) {
        index = i;
        break;
      }
    }
    if (index >= 0 && ((index + 1 >= args.length) || (index + 1 < args.length && keywordSet
        .contains(args[index + 1])))) {
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
      args = Arrays.copyOfRange(args, 0, index);
      return args;
    }
  }

  static void displayLogo(String version) {
    println(" _____       _________  ______   ______    \n"
        + "|_   _|     |  _   _  ||_   _ `.|_   _ \\   \n"
        + "  | |   .--.|_/ | | \\_|  | | `. \\ | |_) |  \n"
        + "  | | / .'`\\ \\  | |      | |  | | |  __'.  \n"
        + " _| |_| \\__. | _| |_    _| |_.' /_| |__) | \n"
        + "|_____|'.__.' |_____|  |______.'|_______/  version " + version + "\n"
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
      setMaxDisplaNum(specialCmd, cmd);
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
    println(String.format("    %s\t\t show how many timeseries are in iotdb",
        SHOW_METADATA_COMMAND));
    println(String.format("    %s=xxx\t eg. long, default, ISO8601, yyyy-MM-dd HH:mm:ss.",
        SET_TIMESTAMP_DISPLAY));
    println(String.format("    %s\t show time display type", SHOW_TIMESTAMP_DISPLAY));
    println(String.format("    %s=xxx\t\t eg. +08:00, Asia/Shanghai.", SET_TIME_ZONE));
    println(String.format("    %s\t\t show cli time zone", SHOW_TIMEZONE));
    println(
        String.format("    %s=xxx\t\t set fetch size when querying data from server.",
            SET_FETCH_SIZE));
    println(String.format("    %s\t\t show fetch size", SHOW_FETCH_SIZE));
    println(
        String.format("    %s=xxx\t eg. set max lines for cli to ouput, -1 equals to unlimited.",
            SET_MAX_DISPLAY_NUM));
  }

  private static void setTimestampDisplay(String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(String.format("Time display format error, please input like %s=ISO8601",
          SET_TIMESTAMP_DISPLAY));
      return;
    }
    try {
      setTimeFormat(cmd.split("=")[1]);
    } catch (Exception e) {
      println(String.format("time display format error, %s", e.getMessage()));
      return;
    }
    println("Time display type has set to " + cmd.split("=")[1].trim());
  }

  private static void setTimeZone(String specialCmd, String cmd, IoTDBConnection connection) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(
          String.format("Time zone format error, please input like %s=+08:00", SET_TIME_ZONE));
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
      println(String
          .format("Fetch size format error, please input like %s=10000", SET_FETCH_SIZE));
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

  private static void setMaxDisplaNum(String specialCmd, String cmd) {
    String[] values = specialCmd.split("=");
    if (values.length != 2) {
      println(String.format("Max display number format error, please input like %s = 10000",
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
      println("Please input like: import /User/myfile. "
          + "Noted that your file path cannot contain any space character)");
      return;
    }
    try {
      println(cmd.split(" ")[1]);
      ImportCsv.importCsvFromFile(host, port, username, password, cmd.split(" ")[1],
          connection.getTimeZone());
    } catch (SQLException e) {
      println(String.format("Failed to import from %s because %s",
          cmd.split(" ")[1], e.getMessage()));
    } catch (TException e) {
      println("Cannot connect to server");
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
          List<List<String>> lists = cacheResult(resultSet, maxSizeList, columnLength,
              resultSetMetaData, zoneId);
          output(lists, maxSizeList);
          long costTime = System.currentTimeMillis() - startTime;
          println(String.format("It costs %.3fs", costTime / 1000.0));
          while (!isReachEnd) {
            println(String.format(
                "Reach the max_display_num = %s. Press ENTER to show more, input 'q' to quit.",
                maxPrintRowCount));
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            try {
              if (br.readLine().equals("")) {
                maxSizeList = new ArrayList<>(columnLength);
                lists = cacheResult(resultSet, maxSizeList, columnLength,
                    resultSetMetaData, zoneId);
                output(lists, maxSizeList);
              } else {
                break;
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
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
   * @param resultSet         jdbc resultSet
   * @param maxSizeList       the longest result of every column
   * @param columnCount       the number of column
   * @param resultSetMetaData jdbc resultSetMetaData
   * @param zoneId            your time zone
   * @return List<List<String>> result
   * @throws SQLException throw exception
   */
  private static List<List<String>> cacheResult(ResultSet resultSet, List<Integer> maxSizeList,
      int columnCount, ResultSetMetaData resultSetMetaData, ZoneId zoneId) throws SQLException {
    List<List<String>> lists = new ArrayList<>(columnCount);
    if (resultSet instanceof IoTDBJDBCResultSet) {
      for (int i = 1; i <= columnCount; i++) {
        List<String> list = new ArrayList<>(maxPrintRowCount + 1);
        list.add(resultSetMetaData.getColumnLabel(i));
        lists.add(list);
        maxSizeList.add(resultSetMetaData.getColumnLabel(i).length());
      }
    } else {
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
    }
    int j = 0;
    if (cursorBeforeFirst) {
      isReachEnd = !resultSet.next();
      cursorBeforeFirst = false;
    }
    if (resultSet instanceof IoTDBJDBCResultSet) {
      boolean printTimestamp = !((IoTDBJDBCResultSet) resultSet).isIgnoreTimeStamp();
      while (j < maxPrintRowCount && !isReachEnd) {
        for (int i = 1; i <= columnCount; i++) {
          String tmp;
          if (printTimestamp && i == 1) {
            tmp = formatDatetime(resultSet.getLong(TIMESTAMP_STR), zoneId);
          } else {
            tmp = resultSet.getString(i);
          }
          if (tmp == null) {
            tmp = NULL;
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
    } else {
      while (j < maxPrintRowCount && !isReachEnd) {
        for (int i = 1; i <= columnCount; i++) {
          String tmp;
          tmp = resultSet.getString(i);
          if (tmp == null) {
            tmp = NULL;
          }
          if (i % 2 != 0 && !tmp.equals(NULL)) {
            tmp = formatDatetime(Long.parseLong(tmp), zoneId);
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

  private static void resetArgs() {
    lineCount = 0;
    cursorBeforeFirst = true;
    isReachEnd = false;
  }

  private static void printBlockLine(List<Integer> maxSizeList) {
    StringBuilder blockLine = new StringBuilder();
    for (Integer integer : maxSizeList) {
      blockLine.append("+").append(StringUtils.repeat("-", integer));
    }
    blockLine.append("+");
    println(blockLine.toString());
  }

  private static void printRow(List<List<String>> lists, int i, List<Integer> maxSizeList) {
    printf("|");
    for (int j = 0; j < maxSizeList.size(); j++) {
      printf("%" + maxSizeList.get(j) + "s|", lists.get(j).get(i));
    }
    println();
  }

  enum OperationResult {
    STOP_OPER, CONTINUE_OPER, NO_OPER
  }

  private static void printf(String format, Object... args) {
    SCREEN_PRINTER.printf(format, args);
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  private static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
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
