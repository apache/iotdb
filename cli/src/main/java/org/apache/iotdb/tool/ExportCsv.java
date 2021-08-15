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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

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

  /** main function of export csv tool. */
  public static void main(String[] args) throws IOException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

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

      String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
      String sql;
      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);
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
    } catch (IOException e) {
      System.out.println("Failed to operate on file, because " + e.getMessage());
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Connect failed because " + e.getMessage());
    } finally {
      reader.close();
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          System.out.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
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
    Options options = createNewOptions();

    Option opTargetFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .argName(TARGET_DIR_NAME)
            .hasArg()
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opTargetFile);

    Option targetFileName =
        Option.builder(TARGET_FILE_ARGS)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc("Export file name (optional)")
            .build();
    options.addOption(targetFileName);

    Option opSqlFile =
        Option.builder(SQL_FILE_ARGS)
            .argName(SQL_FILE_NAME)
            .hasArg()
            .desc("SQL File Path (optional)")
            .build();
    options.addOption(opSqlFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(
                "Output time Format in csv file. "
                    + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
                    + "user-defined pattern like yyyy-MM-dd\\ HH:mm:ss, default ISO8601 (optional)")
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
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
        dumpResult(sql, index);
        index++;
      }
    }
  }

  /**
   * Dump files from database to CSV file.
   *
   * @param sql export the result of executing the sql
   * @param index use to create dump file name
   */
  private static void dumpResult(String sql, int index) {

    final String path = targetDirectory + targetFile + index + ".csv";
    File tf = new File(path);
    try {
      if (!tf.exists() && !tf.createNewFile()) {
        System.out.println("Could not create target file for sql statement: " + sql);
        return;
      }
    } catch (IOException e) {
      System.out.println("Cannot create dump file " + path + " " + "because: " + e.getMessage());
      return;
    }
    System.out.println("Start to export data from sql statement: " + sql);
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tf))) {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
      long startTime = System.currentTimeMillis();
      // write data in csv file
      writeMetadata(bw, sessionDataSet.getColumnNames());

      int line = writeResultSet(sessionDataSet, bw);
      System.out.printf(
          "Statement [%s] has dumped to file %s successfully! It costs "
              + "%dms to export %d lines.%n",
          sql, path, System.currentTimeMillis() - startTime, line);
    } catch (IOException | StatementExecutionException | IoTDBConnectionException e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static void writeMetadata(BufferedWriter bw, List<String> columnNames)
      throws IOException {
    if (!columnNames.get(0).equals("Time")) {
      bw.write("Time" + ",");
    }
    for (int i = 0; i < columnNames.size() - 1; i++) {
      bw.write(columnNames.get(i) + ",");
    }
    bw.write(columnNames.get(columnNames.size() - 1) + "\n");
  }

  private static int writeResultSet(SessionDataSet rs, BufferedWriter bw)
      throws IOException, StatementExecutionException, IoTDBConnectionException {
    int line = 0;
    long timestamp = System.currentTimeMillis();
    while (rs.hasNext()) {
      RowRecord rowRecord = rs.next();
      List<Field> fields = rowRecord.getFields();
      writeTime(rowRecord.getTimestamp(), bw);
      writeValue(fields, bw);
      line++;
      if (line % EXPORT_PER_LINE_COUNT == 0) {
        long tmp = System.currentTimeMillis();
        System.out.printf("%d lines have been exported, it takes %dms%n", line, (tmp - timestamp));
        timestamp = tmp;
      }
    }
    return line;
  }

  private static void writeTime(Long time, BufferedWriter bw) throws IOException {
    ZonedDateTime dateTime;
    String timestampPrecision = "ms";
    switch (timeFormat) {
      case "default":
        String str =
            RpcUtils.parseLongToDateWithPrecision(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME, time, zoneId, timestampPrecision);
        bw.write(str + ",");
        break;
      case "timestamp":
      case "long":
      case "number":
        bw.write(time + ",");
        break;
      default:
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), zoneId);
        bw.write(dateTime.format(DateTimeFormatter.ofPattern(timeFormat)) + ",");
        break;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static void writeValue(List<Field> fields, BufferedWriter bw) throws IOException {
    for (int j = 0; j < fields.size() - 1; j++) {
      String value = fields.get(j).getStringValue();
      if ("null".equalsIgnoreCase(value)) {
        bw.write(",");
      } else {
        if (fields.get(j).getDataType() == TSDataType.TEXT) {
          int location = value.indexOf("\"");
          if (location > -1) {
            if (location == 0 || value.charAt(location - 1) != '\\') {
              bw.write("\"" + value.replace("\"", "\\\"") + "\",");
            } else {
              bw.write("\"" + value + "\",");
            }
          } else if (value.contains(",")) {
            bw.write("\"" + value + "\",");
          } else {
            bw.write(value + ",");
          }
        } else {
          bw.write(value + ",");
        }
      }
    }
    String lastValue = fields.get(fields.size() - 1).getStringValue();
    if ("null".equalsIgnoreCase(lastValue)) {
      bw.write("\n");
    } else {
      if (fields.get(fields.size() - 1).getDataType() == TSDataType.TEXT) {
        int location = lastValue.indexOf("\"");
        if (location > -1) {
          if (location == 0 || lastValue.charAt(location - 1) != '\\') {
            bw.write("\"" + lastValue.replace("\"", "\\\"") + "\"\n");
          } else {
            bw.write("\"" + lastValue + "\"\n");
          }
        } else if (lastValue.contains(",")) {
          bw.write("\"" + lastValue + "\"\n");
        } else {
          bw.write(lastValue + "\n");
        }
      } else {
        bw.write(lastValue + "\n");
      }
    }
  }
}
