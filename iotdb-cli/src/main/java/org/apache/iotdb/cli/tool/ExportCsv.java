/**
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
package org.apache.iotdb.cli.tool;

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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Export CSV file.
 *
 * @author aru cheng
 * @version 1.0.0 20170719
 */
public class ExportCsv extends AbstractCsvTool {

  private static final String TARGET_FILE_ARGS = "td";
  private static final String TARGET_FILE_NAME = "targetDirectory";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sqlfile";

  private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";

  private static final String DUMP_FILE_NAME = "dump";

  private static String targetDirectory;

  private static final Logger LOGGER = LoggerFactory.getLogger(ExportCsv.class);

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
      LOGGER.error("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      LOGGER.error(e.getMessage());
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
      LOGGER.error(
          "Failed to dump data because cannot find TsFile JDBC Driver, "
              + "please check whether you have imported driver or not", e);
    } catch (SQLException e) {
      LOGGER.error("Encounter an error when dumping data, error is ", e);
    } catch (IOException e) {
      LOGGER.error("Failed to operate on file, because ", e);
    } catch (TException e) {
      LOGGER.error("Encounter an error when connecting to server, because ",
              e);
    } catch (ArgsErrorException e) {
      LOGGER.error("Invalid args.", e);
    } finally {
      reader.close();
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static void parseSpecialParams(CommandLine commandLine)
      throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_FILE_ARGS, TARGET_FILE_NAME, commandLine);
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

    Option opTargetFile = Option.builder(TARGET_FILE_ARGS).required().argName(TARGET_FILE_NAME)
        .hasArg()
        .desc("Target File Directory (required)").build();
    options.addOption(opTargetFile);

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
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))){
      String sql;
      int index = 0;
      while ((sql = reader.readLine()) != null) {
        try {
          dumpResult(sql, index);
        } catch (SQLException e) {
          LOGGER.error("Cannot dump data for statement {}, because ", sql, e);
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

    final String path = targetDirectory + DUMP_FILE_NAME + index + ".csv";
    File tf = new File(path);
    try {
      if (!tf.exists() && !tf.createNewFile()) {
          LOGGER.error("Could not create target file for sql statement: {}", sql);
          return;
      }
    } catch (IOException e) {
      LOGGER.error("Cannot create dump file {}", path,  e);
      return;
    }

    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tf))) {
      ResultSetMetaData metadata = rs.getMetaData();
      long startTime = System.currentTimeMillis();

      int count = metadata.getColumnCount();
      // write data in csv file
      writeMetadata(bw, count, metadata);

      writeResultSet(rs, bw, count);
      LOGGER.info("Statement [{}] has dumped to file {} successfully! It costs {}ms.",
          sql, path, System.currentTimeMillis() - startTime);
    } catch (IOException e) {
      LOGGER.error("Cannot dump result because", e);
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
    }
  }

  private static void writeResultSet(ResultSet rs, BufferedWriter bw, int count)
      throws SQLException, IOException {
    while (rs.next()) {
      if (rs.getString(1) == null ||
          "null".equalsIgnoreCase(rs.getString(1))) {
        bw.write(",");
      } else {
        writeTime(rs, bw);
        writeValue(rs, count, bw);
      }
    }
  }

  private static void writeTime(ResultSet rs, BufferedWriter bw) throws SQLException, IOException {
    ZonedDateTime dateTime;
    switch (timeFormat) {
      case "default":
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong(1)),
            zoneId);
        bw.write(dateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + ",");
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

  private static void writeValue(ResultSet rs, int count, BufferedWriter bw)
      throws SQLException, IOException {
    for (int j = 2; j <= count; j++) {
      if (j < count) {
        if ("null".equals(rs.getString(j))) {
          bw.write(",");
        } else {
          bw.write(rs.getString(j) + ",");
        }
      } else {
        if ("null".equals(rs.getString(j))) {
          bw.write("\n");
        } else {
          bw.write(rs.getString(j) + "\n");
        }
      }
    }
  }
}
