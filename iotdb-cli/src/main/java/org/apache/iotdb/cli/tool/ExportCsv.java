/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  /**
   * main function of export csv tool.
   */
  public static void main(String[] args) throws IOException, SQLException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("[ERROR] Too few params input, please check the following hint.");
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
      parseSpecialParams(commandLine, reader);
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
        return;
      } else {
        dumpFromSqlFile(sqlFile);
      }
    } catch (ClassNotFoundException e) {
      System.out.println(
          "[ERROR] Failed to dump data because cannot find TsFile JDBC Driver, "
              + "please check whether you have imported driver or not");
    } catch (SQLException e) {
      System.out.println(
          String
              .format("[ERROR] Encounter an error when dumping data, error is %s", e.getMessage()));
    } catch (IOException e) {
      System.out
          .println(String.format("[ERROR] Failed to operate on file, because %s", e.getMessage()));
    } catch (TException e) {
      System.out.println(
          String.format("[ERROR] Encounter an error when connecting to server, because %s",
              e.getMessage()));
    } catch (ArgsErrorException e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        reader.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static void parseSpecialParams(CommandLine commandLine, ConsoleReader reader)
      throws IOException, ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_FILE_ARGS, TARGET_FILE_NAME, commandLine);
    timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
    if (timeFormat == null) {
      timeFormat = DEFAULT_TIME_FORMAT;
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

  private static void dumpFromSqlFile(String filePath) throws ClassNotFoundException, IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    String sql = null;
    int index = 0;
    while ((sql = reader.readLine()) != null) {
      try {
        dumpResult(sql, index);
      } catch (SQLException e) {
        System.out.println(
            String.format("[ERROR] Cannot dump data for statment %s, because %s", sql,
                e.getMessage()));
      }
      index++;
    }
    reader.close();
  }

  /**
   * Dump files from database to CSV file.
   *
   * @param sql export the result of executing the sql
   * @param index use to create dump file name
   * @throws ClassNotFoundException if cannot find driver
   * @throws SQLException if SQL is not valid
   */
  private static void dumpResult(String sql, int index)
      throws ClassNotFoundException, SQLException {
    BufferedWriter writer = null;
    final String path = targetDirectory + DUMP_FILE_NAME + index + ".csv";
    try {
      File tf = new File(path);
      if (!tf.exists()) {
        if (!tf.createNewFile()) {
          System.out.println("[ERROR] Could not create target file for sql statement: " + sql);
          return;
        }
      }
      writer = new BufferedWriter(new FileWriter(tf));
    } catch (IOException e) {
      System.out.println(e.getMessage());
      return;
    }

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery(sql);
    ResultSetMetaData metadata = rs.getMetaData();
    long startTime = System.currentTimeMillis();
    try {
      int count = metadata.getColumnCount();
      // write data in csv file
      for (int i = 1; i <= count; i++) {
        if (i < count) {
          writer.write(metadata.getColumnLabel(i) + ",");
        } else {
          writer.write(metadata.getColumnLabel(i) + "\n");
        }
      }
      while (rs.next()) {
        if (rs.getString(1) == null || rs.getString(1).toLowerCase().equals("null")) {
          writer.write(",");
        } else {
          ZonedDateTime dateTime;
          switch (timeFormat) {
            case DEFAULT_TIME_FORMAT:
            case "default":
              dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong(1)), zoneId);
              writer.write(dateTime.toString() + ",");
              break;
            case "timestamp":
            case "long":
            case "nubmer":
              writer.write(rs.getLong(1) + ",");
              break;
            default:
              dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong(1)), zoneId);
              writer.write(dateTime.format(DateTimeFormatter.ofPattern(timeFormat)) + ",");
              break;
          }

          for (int j = 2; j <= count; j++) {
            if (j < count) {
              if (rs.getString(j).equals("null")) {
                writer.write(",");
              } else {
                writer.write(rs.getString(j) + ",");
              }
            } else {
              if (rs.getString(j).equals("null")) {
                writer.write("\n");
              } else {
                writer.write(rs.getString(j) + "\n");
              }
            }
          }
        }
      }
      System.out
          .println(String
              .format("[INFO] Statement [%s] has dumped to file %s successfully! It costs %d ms.",
                  sql, path, (System.currentTimeMillis() - startTime)));
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } finally {
      try {
        writer.flush();
        writer.close();
      } catch (IOException e) {
        System.out.println(e.getMessage());
      }
      if (statement != null) {
        statement.close();
      }
    }
  }

}