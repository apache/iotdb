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

package org.apache.iotdb.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cli.AbstractCli;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.thrift.TException;

/**
 * Export CSV file.
 *
 * @version 1.0.0 20170719
 */
public class ExportCsv extends AbstractCsvTool {

  private static final String TARGET_DIR_ARGS = "td";
  private static final String TARGET_DIR_NAME = "targetDirectory";

  private static final String TARGET_FILE_ARGS = "f";
  private static final String TARGET_FILE_NAME = "targetFile";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sqlfile";

  private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;

  private static String targetDirectory;

  private static final int EXPORT_PER_LINE_COUNT = 10000;

  private static String TIMESTAMP_PRECISION = "ms";

  private static List<Integer> typeList = new ArrayList<>();

  /**
   * main function of export csv tool.
   */
  public static void main(String[] args) throws IOException, SQLException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }

    ConsoleReader reader = new ConsoleReader();
    reader.setExpandEvents(false);

    try {
      parseBasicParams(commandLine, reader);
      parseSpecialParams(commandLine);
      if (!checkTimeFormat()) {
        return;
      }
      Class.forName(Config.JDBC_DRIVER_NAME);

      String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
      String sql;

      connection = (IoTDBConnection) DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password);
      setTimeZone();

      if (sqlFile == null) {
        sql = reader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
        String[] values = sql.trim().split(";");
        for (int i = 0; i < values.length; i++) {
          dumpResult(values[i], i);
        }
      } else {
        dumpFromSqlFile(sqlFile);
      }
    } catch (ClassNotFoundException e) {
      System.out.println("Failed to export data because cannot find IoTDB JDBC Driver, "
          + "please check whether you have imported driver or not: " + e.getMessage());
    } catch (TException e) {
      System.out.println("Encounter an error when connecting to server, because " + e.getMessage());
    } catch (SQLException e) {
      System.out.println("Encounter an error when exporting data, error is: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("Failed to operate on file, because " + e.getMessage());
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
    } finally {
      reader.close();
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static void parseSpecialParams(CommandLine commandLine)
      throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
    if (targetFile == null) {
      targetFile = DUMP_FILE_NAME_DEFAULT;
    }
    timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
    if (timeFormat == null) {
      timeFormat = "default";
    }
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    if (!targetDirectory.endsWith(File.separator)) {
      targetDirectory += File.separator;
    }
  }

  /**
   * commandline option create.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = new Options();

    Option opHost = Option.builder(HOST_ARGS).longOpt(HOST_NAME).required().argName(HOST_NAME)
        .hasArg()
        .desc("Host Name (required)").build();
    options.addOption(opHost);

    Option opPort = Option.builder(PORT_ARGS).longOpt(PORT_NAME).required().argName(PORT_NAME)
        .hasArg()
        .desc("Port (required)").build();
    options.addOption(opPort);

    Option opUsername = Option.builder(USERNAME_ARGS).longOpt(USERNAME_NAME).required()
        .argName(USERNAME_NAME)
        .hasArg().desc("Username (required)").build();
    options.addOption(opUsername);

    Option opPassword = Option.builder(PASSWORD_ARGS).longOpt(PASSWORD_NAME).optionalArg(true)
        .argName(PASSWORD_NAME).hasArg().desc("Password (optional)").build();
    options.addOption(opPassword);

    Option opTargetFile = Option.builder(TARGET_DIR_ARGS).required().argName(TARGET_DIR_NAME)
        .hasArg()
        .desc("Target File Directory (required)").build();
    options.addOption(opTargetFile);

    Option targetFileName = Option.builder(TARGET_FILE_ARGS).argName(TARGET_FILE_NAME).hasArg()
        .desc("Export file name (optional)").build();
    options.addOption(targetFileName);

    Option opSqlFile = Option.builder(SQL_FILE_ARGS).argName(SQL_FILE_NAME).hasArg()
        .desc("SQL File Path (optional)").build();
    options.addOption(opSqlFile);

    Option opTimeFormat = Option.builder(TIME_FORMAT_ARGS).argName(TIME_FORMAT_NAME).hasArg()
        .desc("Output time Format in csv file. "
            + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
            + "user-defined pattern like yyyy-MM-dd\\ HH:mm:ss, default ISO8601 (optional)")
        .build();
    options.addOption(opTimeFormat);

    Option opTimeZone = Option.builder(TIME_ZONE_ARGS).argName(TIME_ZONE_NAME).hasArg()
        .desc("Time Zone eg. +08:00 or -01:00 (optional)").build();
    options.addOption(opTimeZone);

    Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg(false)
        .desc("Display help information")
        .build();
    options.addOption(opHelp);

    return options;
  }

  private static void dumpFromSqlFile(String filePath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String sql;
      int index = 0;
      while ((sql = reader.readLine()) != null) {
        try {
          dumpResult(sql, index);
        } catch (SQLException e) {
          System.out
              .println("Cannot dump data for statement " + sql + ", because : " + e.getMessage());
        }
        index++;
      }
    }
  }

  /**
   * Dump files from database to CSV file.
   *
   * @param sql export the result of executing the sql
   * @param index use to create dump file name
   * @throws SQLException if SQL is not valid
   */
  private static void dumpResult(String sql, int index)
      throws SQLException {

    final String path = targetDirectory + targetFile + index + ".csv";
    File tf = new File(path);
    try {
      if (!tf.exists() && !tf.createNewFile()) {
        System.out.println("Could not create target file for sql statement: " + sql);
        return;
      }
    } catch (IOException e) {
      System.out.println("Cannot create dump file " + path + "because: " + e.getMessage());
      return;
    }
    System.out.println("Start to export data from sql statement: " + sql);
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tf))) {
      ResultSetMetaData metadata = rs.getMetaData();
      long startTime = System.currentTimeMillis();

      int count = metadata.getColumnCount();
      // write data in csv file
      writeMetadata(bw, count, metadata);

      int line = writeResultSet(rs, bw, count);
      System.out
          .println(String.format("Statement [%s] has dumped to file %s successfully! It costs "
                  + "%dms to export %d lines.", sql, path, System.currentTimeMillis() - startTime,
              line));
    } catch (IOException e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static void writeMetadata(BufferedWriter bw, int count, ResultSetMetaData metadata)
      throws SQLException, IOException {
    for (int i = 1; i <= count; i++) {
      if (i < count) {
        bw.write(metadata.getColumnLabel(i) + ",");
      } else {
        bw.write(metadata.getColumnLabel(i) + "\n");
      }
      typeList.add(metadata.getColumnType(i));
    }
  }

  private static int writeResultSet(ResultSet rs, BufferedWriter bw, int count)
      throws SQLException, IOException {
    int line = 0;
    long timestamp = System.currentTimeMillis();
    while (rs.next()) {
      if (rs.getString(1) == null ||
          "null".equalsIgnoreCase(rs.getString(1))) {
        bw.write(",");
      } else {
        writeTime(rs, bw);
        writeValue(rs, count, bw);
      }
      line++;
      if (line % EXPORT_PER_LINE_COUNT == 0) {
        long tmp = System.currentTimeMillis();
        System.out.println(
            String.format("%d lines have been exported, it takes %dms", line, (tmp - timestamp)));
        timestamp = tmp;
      }
    }
    return line;
  }

  private static void writeTime(ResultSet rs, BufferedWriter bw) throws SQLException, IOException {
    ZonedDateTime dateTime;
    switch (timeFormat) {
      case "default":
        long timestamp = rs.getLong(1);
        String str = AbstractCli
            .parseLongToDateWithPrecision(DateTimeFormatter.ISO_OFFSET_DATE_TIME, timestamp, zoneId,
                TIMESTAMP_PRECISION);
        bw.write(str + ",");
        break;
      case "timestamp":
      case "long":
      case "nubmer":
        bw.write(rs.getLong(1) + ",");
        break;
      default:
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong(1)),
            zoneId);
        bw.write(dateTime.format(DateTimeFormatter.ofPattern(timeFormat)) + ",");
        break;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static void writeValue(ResultSet rs, int count, BufferedWriter bw)
      throws SQLException, IOException {
    for (int j = 2; j <= count; j++) {
      if (j < count) {
        if ("null".equals(rs.getString(j))) {
          bw.write(",");
        } else {
          if(typeList.get(j-1) == Types.VARCHAR) {
            bw.write("\'" + rs.getString(j) + "\'"+ ",");
          } else {
            bw.write(rs.getString(j) + ",");
          }
        }
      } else {
        if ("null".equals(rs.getString(j))) {
          bw.write("\n");
        } else {
          if(typeList.get(j-1) == Types.VARCHAR) {
            bw.write("\'" + rs.getString(j) + "\'"+ "\n");
          } else {
            bw.write(rs.getString(j) + "\n");
          }
        }
      }
    }
  }
}
