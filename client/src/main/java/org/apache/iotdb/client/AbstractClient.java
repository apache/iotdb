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
package org.apache.iotdb.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBQueryResultSet;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.tool.ImportCsv;
import org.apache.thrift.TException;

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
import java.util.*;

public abstract class AbstractClient {

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

  static final String ISO8601_ARGS = "disableISO8601";
  static final List<String> AGGREGRATE_TIME_LIST = new ArrayList<>();
  static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
  private static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
  static final String SET_MAX_DISPLAY_NUM = "set max_display_num";
  static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";
  static final String SHOW_TIMESTAMP_DISPLAY = "show time_display_type";
  static final String SET_TIME_ZONE = "set time_zone";
  static final String SHOW_TIMEZONE = "show time_zone";
  static final String SET_FETCH_SIZE = "set fetch_size";
  static final String SHOW_FETCH_SIZE = "show fetch_size";
  private static final String HELP = "help";
  static final String IOTDB_CLI_PREFIX = "IoTDB";
  static final String SCRIPT_HINT = "./start-client.sh(start-client.bat if Windows)";
  static final String QUIT_COMMAND = "quit";
  static final String EXIT_COMMAND = "exit";
  private static final String SHOW_METADATA_COMMAND = "show timeseries";
  static final int MAX_HELP_CONSOLE_WIDTH = 88;
  static final String TIMESTAMP_STR = "Time";
  static final int ISO_DATETIME_LEN = 35;
  private static final String IMPORT_CMD = "import";
  private static final String NEED_NOT_TO_PRINT_TIMESTAMP = "AGGREGATION";
  private static final String DEFAULT_TIME_FORMAT = "default";
  private static String timeFormat = DEFAULT_TIME_FORMAT;
  static int maxPrintRowCount = 100000;
  private static int fetchSize = 10000;
  static int maxTimeLength = ISO_DATETIME_LEN;
  static int maxValueLength = 15;
  private static int deviceColumnLength = 20; // for GROUP_BY_DEVICE sql
  private static int measurementColumnLength = 10; // for GROUP_BY_DEVICE sql
  // for GROUP_BY_DEVICE sql; this name should be the same as that used in server
  private static String GROUPBY_DEVICE_COLUMN_NAME = "Device";
  private static boolean isQuit = false;
  static String TIMESTAMP_PRECISION = "ms";

  private static final int START_PRINT_INDEX = 2;
  private static final int NO_ALIGN_PRINT_INTERVAL = 2;

  /**
   * control the width of columns for 'show timeseries path' and 'show storage group'.
   * <p>
   * for 'show timeseries path':
   * <table>
   * <tr>
   * <th>Timeseries (width:75)</th>
   * <th>Storage Group (width:45)</th>
   * <th>DataType width:8)</th>
   * <th>Encoding (width:8)</th>
   * </tr>
   * <tr>
   * <td>root.vehicle.d1.s1</td>
   * <td>root.vehicle</td>
   * <td>INT32</td>
   * <td>PLAIN</td>
   * </tr>
   * <tr>
   * <td>...</td>
   * <td>...</td>
   * <td>...</td>
   * <td>...</td>
   * </tr>
   * </table>
   * </p>
   * <p>
   * for "show storage group path":
   * <table>
   * <tr>
   * <th>STORAGE_GROUP (width:75)</th>
   * </tr>
   * <tr>
   * <td>root.vehicle</td>
   * </tr>
   * <tr>
   * <td>...</td>
   * </tr>
   * </table>
   * </p>
   */
  private static int[] maxValueLengthForShow = new int[]{75, 45, 8, 8};
  static String formatTime = "%" + maxTimeLength + "s|";
  private static String formatValue = "%" + maxValueLength + "s|";
  private static final int DIVIDING_LINE_LENGTH = 40;
  static String host = "127.0.0.1";
  static String port = "6667";
  static String username;
  static String password;
  static String execute;
  static boolean hasExecuteSQL = false;

  private static boolean printToConsole = true;

  static Set<String> keywordSet = new HashSet<>();

  static ServerProperties properties = null;

  private static boolean printHeader = false;
  private static int displayCnt = 0;

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  /**
   * showException is currently fixed to false because the display of exceptions is not elaborate.
   * We can make it an option in future versions.
   */
  private static boolean showException = false;

  static void init() {
    keywordSet.add("-" + HOST_ARGS);
    keywordSet.add("-" + HELP_ARGS);
    keywordSet.add("-" + PORT_ARGS);
    keywordSet.add("-" + PASSWORD_ARGS);
    keywordSet.add("-" + USERNAME_ARGS);
    keywordSet.add("-" + EXECUTE_ARGS);
    keywordSet.add("-" + ISO8601_ARGS);
    keywordSet.add("-" + MAX_PRINT_ROW_COUNT_ARGS);
  }

  /**
   * Client result output.
   *
   * @param res result set
   * @param printToConsole print to console
   * @param zoneId time-zone ID
   * @throws SQLException SQLException
   */
  private static void output(ResultSet res, boolean printToConsole, ZoneId zoneId)
      throws SQLException {
    int cnt = 0;
    boolean printTimestamp = true;
    boolean align = true;
    displayCnt = 0;
    printHeader = false;
    ResultSetMetaData resultSetMetaData = res.getMetaData();

    int colCount = resultSetMetaData.getColumnCount();

    if (res instanceof IoTDBQueryResultSet) {
      printTimestamp = !((IoTDBQueryResultSet) res).isIgnoreTimeStamp();
    }
    else {
      align = false;
    }

    // Output values
    while (cnt < maxPrintRowCount && res.next()) {
      printRow(printTimestamp, align, colCount, resultSetMetaData, res, zoneId);
      cnt++;
      if (!printToConsole && cnt % 10000 == 0) {
        println(cnt);
      }
    }

    if (printToConsole) {
      if (!printHeader) {
        printBlockLine(printTimestamp, align, colCount, resultSetMetaData);
        printName(printTimestamp, align, colCount, resultSetMetaData);
        printBlockLine(printTimestamp, align, colCount, resultSetMetaData);
      } else {
        printBlockLine(printTimestamp, align, colCount, resultSetMetaData);
      }

    }

    if(!res.next()){
        printCount(cnt);
    } else {
        println(String.format("Reach maxPrintRowCount = %s lines", maxPrintRowCount));
    }
  }

  private static String getTimestampPrecision() {
    return TIMESTAMP_PRECISION;
  }

  private static void printCount(int cnt) {
      println("Total line number = " + cnt);
  }

  private static void printRow(boolean printTimestamp, boolean align, int colCount,
      ResultSetMetaData resultSetMetaData, ResultSet res, ZoneId zoneId)
      throws SQLException {
    // Output Labels
    if (!printToConsole) {
      return;
    }
    printHeader(printTimestamp, align, colCount, resultSetMetaData);
    printRowData(printTimestamp, align, res, zoneId, resultSetMetaData, colCount);
  }

  private static void printHeader(boolean printTimestamp, boolean align, int colCount,
      ResultSetMetaData resultSetMetaData) throws SQLException {
    if (!printHeader) {
      printBlockLine(printTimestamp, align, colCount, resultSetMetaData);
      printName(printTimestamp, align, colCount, resultSetMetaData);
      printBlockLine(printTimestamp, align, colCount, resultSetMetaData);
      printHeader = true;
    }
  }

  private static void printShow(int colCount, ResultSet res) throws SQLException {
    print("|");
    for (int i = 1; i <= colCount; i++) {
      formatValue = "%" + maxValueLengthForShow[i - 1] + "s|";
      printf(formatValue, res.getString(i));
    }
    println();
  }

  private static void printRowData(boolean printTimestamp, boolean align, ResultSet res, ZoneId zoneId,
      ResultSetMetaData resultSetMetaData, int colCount)
      throws SQLException {
    if (displayCnt < maxPrintRowCount) { // NOTE displayCnt only works on queried data results
      print("|");
      if (align) {
        if (printTimestamp) {
          printf(formatTime, formatDatetime(res.getLong(TIMESTAMP_STR), zoneId));
          for (int i = 2; i <= colCount; i++) {
            printColumnData(resultSetMetaData, true, res, i, zoneId);
          }
        } else {
          for (int i = 1; i <= colCount; i++) {
            printColumnData(resultSetMetaData, true, res, i, zoneId);
          }
        }
      }
      else {
        for (int i = START_PRINT_INDEX; i <= colCount / NO_ALIGN_PRINT_INTERVAL + 1; i++) {
          if (printTimestamp) {
            // timeLabel used for indicating the time column.
            String timeLabel = TIMESTAMP_STR + resultSetMetaData.getColumnLabel(NO_ALIGN_PRINT_INTERVAL * i - START_PRINT_INDEX);
            try {
              printf(formatTime, formatDatetime(res.getLong(timeLabel), zoneId));
            } catch (Exception e) {
              printf(formatTime, "null");
              handleException(e);
            }
          }
          printColumnData(resultSetMetaData, false, res, i, zoneId);
        }
      }
      println();
      displayCnt++;
    }
  }

  private static void printColumnData(ResultSetMetaData resultSetMetaData, boolean align,
      ResultSet res, int i, ZoneId zoneId) throws SQLException {
    boolean flag = false;
    for (String timeStr : AGGREGRATE_TIME_LIST) {
      if (resultSetMetaData.getColumnLabel(i).toUpperCase().contains(timeStr.toUpperCase())) {
        flag = true;
        break;
      }
    }
    if (flag) {
      try {
        printf(formatValue, formatDatetime(res.getLong(i), zoneId));
      } catch (Exception e) {
        printf(formatValue, "null");
        handleException(e);
      }
    } 
    else if (align) {
      if (i == 2 && resultSetMetaData.getColumnName(2).equals(GROUPBY_DEVICE_COLUMN_NAME)) {
        printf("%" + deviceColumnLength + "s|", res.getString(i));
      } else {
        printf(formatValue, res.getString(i));
      }
    }
    // for disable align clause
    else {
      if (res.getString(i * NO_ALIGN_PRINT_INTERVAL - START_PRINT_INDEX) == null) {
        //blank space
        printf(formatValue, "");
      }
      else {
        printf(formatValue, res.getString(i * NO_ALIGN_PRINT_INTERVAL - START_PRINT_INDEX));
      }
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
    return options;
  }

  public static String parseLongToDateWithPrecision(DateTimeFormatter formatter,
      long timestamp, ZoneId zoneid, String timestampPrecision) {
    if (timestampPrecision.equals("ms")) {
      long integerofDate = timestamp / 1000;
      String digits = Long.toString(timestamp % 1000);
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 3) {
        for (int i = 0; i < 3 - length; i++) {
          digits = "0" + digits;
        }
      }
      return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
    } else if (timestampPrecision.equals("us")) {
      long integerofDate = timestamp / 1000_000;
      String digits = Long.toString(timestamp % 1000_000);
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 6) {
        for (int i = 0; i < 6 - length; i++) {
          digits = "0" + digits;
        }
      }
      return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
    } else {
      long integerofDate = timestamp / 1000_000_000L;
      String digits = Long.toString(timestamp % 1000_000_000L);
      ZonedDateTime dateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 9) {
        for (int i = 0; i < 9 - length; i++) {
          digits = "0" + digits;
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

  private static void printBlockLine(boolean printTimestamp, boolean align, int colCount,
      ResultSetMetaData resultSetMetaData) throws SQLException {
    StringBuilder blockLine = new StringBuilder();
    if (align) {
      if (printTimestamp) {
        blockLine.append("+").append(StringUtils.repeat('-', maxTimeLength)).append("+");
        if (resultSetMetaData.getColumnName(2).equals(GROUPBY_DEVICE_COLUMN_NAME)) {
          maxValueLength = measurementColumnLength;
        } else {
          int tmp = Integer.MIN_VALUE;
          for (int i = 1; i <= colCount; i++) {
            int len = resultSetMetaData.getColumnLabel(i).length();
            tmp = Math.max(tmp, len);
          }
          maxValueLength = tmp;
        }
        for (int i = 2; i <= colCount; i++) {
          if (i == 2 && resultSetMetaData.getColumnName(2).equals(GROUPBY_DEVICE_COLUMN_NAME)) {
            blockLine.append(StringUtils.repeat('-', deviceColumnLength)).append("+");
          } else {
            blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
          }
        }
      } else {
        blockLine.append("+");
        for (int i = 1; i <= colCount; i++) {
          blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
        }
      }
    }
    // for disable align clause
    else {
      int tmp = Integer.MIN_VALUE;
      for (int i = 1; i <= colCount; i++) {
        int len = resultSetMetaData.getColumnLabel(i).length();
        tmp = Math.max(tmp, len);
      }
      maxValueLength = tmp;
      blockLine.append("+");
      for (int i = 2; i <= colCount / 2 + 1; i++) {
        if (printTimestamp) {
          blockLine.append(StringUtils.repeat('-', maxTimeLength)).append("+");
        }
        blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
      }
    }
    println(blockLine);
  }

  private static void printName(boolean printTimestamp, boolean align, int colCount,
      ResultSetMetaData resultSetMetaData) throws SQLException {
    print("|");
    formatValue = "%" + maxValueLength + "s|";
    if (align) {
      if (printTimestamp) {
        printf(formatTime, TIMESTAMP_STR);
        for (int i = 2; i <= colCount; i++) {
          if (i == 2 && resultSetMetaData.getColumnName(2).equals(GROUPBY_DEVICE_COLUMN_NAME)) {
            printf("%" + deviceColumnLength + "s|", resultSetMetaData.getColumnLabel(i));
          } else {
            printf(formatValue, resultSetMetaData.getColumnLabel(i));
          }
        }
      } else {
        for (int i = 1; i <= colCount; i++) {
          printf(formatValue, resultSetMetaData.getColumnLabel(i));
        }
      }
    }
    // for disable align
    else {
      for (int i = 2; i <= colCount; i += 2) {
        if (printTimestamp) {
          printf(formatTime, TIMESTAMP_STR);
        }
        printf(formatValue, resultSetMetaData.getColumnLabel(i));
      }
    }
    println();
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
      executeCommand.deleteCharAt(executeCommand.length() - 1);
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
    println("Starting IoTDB Client");
    println("---------------------");
  }

  static OperationResult handleInputCmd(String cmd, IoTDBConnection connection) {
    String specialCmd = cmd.toLowerCase().trim();

    if (QUIT_COMMAND.equals(specialCmd) || EXIT_COMMAND.equals(specialCmd)) {
      isQuit = true;
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
      handleException(e);
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
      handleException(e);
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
      handleException(e);
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
      handleException(e);
      return;
    }
    println("Max display number has set to " + values[1].trim());
  }

  private static void showTimeZone(IoTDBConnection connection) {
    try {
      println("Current time zone: " + connection.getTimeZone());
    } catch (Exception e) {
      println("Cannot get time zone from server side because: " + e.getMessage());
      handleException(e);
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
      handleException(e);
    } catch (TException e) {
      println("Cannot connect to server");
      handleException(e);
    }
  }

  private static void executeQuery(IoTDBConnection connection, String cmd) {
    long startTime = System.currentTimeMillis();
    try (Statement statement = connection.createStatement();) {
      ZoneId zoneId = ZoneId.of(connection.getTimeZone());
      statement.setFetchSize(fetchSize);
      boolean hasResultSet = statement.execute(cmd.trim());
      if (hasResultSet) {
        ResultSet resultSet = statement.getResultSet();
        output(resultSet, printToConsole, zoneId);
        if (resultSet != null) {
          resultSet.close();
        }
      }
    } catch (Exception e) {
      println("Msg: " + e.getMessage());
      handleException(e);
    }
    long costTime = System.currentTimeMillis() - startTime;
    println(String.format("It costs %.3fs", costTime / 1000.0));
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

  private static void println(Object obj) {
    SCREEN_PRINTER.println(obj);
  }

  static void handleException(Exception e) {
    if (showException) {
      e.printStackTrace(SCREEN_PRINTER);
    }
  }
}
