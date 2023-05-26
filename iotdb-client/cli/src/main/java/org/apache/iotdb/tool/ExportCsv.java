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

import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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

  private static final String DATA_TYPE_ARGS = "datatype";
  private static final String DATA_TYPE_NAME = "datatype";

  private static final String QUERY_COMMAND_ARGS = "q";
  private static final String QUERY_COMMAND_NAME = "queryCommand";

  private static final String LINES_PER_FILE_ARGS = "linesPerFile";
  private static final String LINES_PER_FILE_ARGS_NAME = "Lines Per File";

  private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;

  private static String targetDirectory;

  private static Boolean needDataTypePrinted;

  private static String queryCommand;

  private static String timestampPrecision;

  private static int linesPerFile = 10000;

  private static final int EXPORT_PER_LINE_COUNT = 10000;

  private static long timeout = -1;

  /** main function of export csv tool. */
  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    int exitCode = CODE_OK;
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
      if (!checkTimeFormat()) {
        System.exit(CODE_ERROR);
      }

      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);
      timestampPrecision = session.getTimestampPrecision();
      setTimeZone();

      if (queryCommand == null) {
        String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
        String sql;

        if (sqlFile == null) {
          LineReader lineReader = JlineUtils.getLineReader(username, host, port);
          sql = lineReader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
          System.out.println(sql);
          String[] values = sql.trim().split(";");
          for (int i = 0; i < values.length; i++) {
            dumpResult(values[i], i);
          }
        } else {
          dumpFromSqlFile(sqlFile);
        }
      } else {
        dumpResult(queryCommand, 0);
      }

    } catch (IOException e) {
      System.out.println("Failed to operate on file, because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (TException e) {
      System.out.println(
          "Can not get the timestamp precision from server because " + e.getMessage());
      exitCode = CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          exitCode = CODE_ERROR;
          System.out.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
    System.exit(exitCode);
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
    needDataTypePrinted = Boolean.valueOf(commandLine.getOptionValue(DATA_TYPE_ARGS));
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);
    String timeoutString = commandLine.getOptionValue(TIMEOUT_ARGS);
    if (timeoutString != null) {
      timeout = Long.parseLong(timeoutString);
    }
    if (needDataTypePrinted == null) {
      needDataTypePrinted = true;
    }
    if (targetFile == null) {
      targetFile = DUMP_FILE_NAME_DEFAULT;
    }
    timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
    if (timeFormat == null) {
      timeFormat = "default";
    }
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
      targetDirectory += File.separator;
    }
    if (commandLine.getOptionValue(LINES_PER_FILE_ARGS) != null) {
      linesPerFile = Integer.parseInt(commandLine.getOptionValue(LINES_PER_FILE_ARGS));
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

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(
                "Will the data type of timeseries be printed in the head line of the CSV file?"
                    + '\n'
                    + "You can choose true) or false) . (optional)")
            .build();
    options.addOption(opDataType);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .argName(QUERY_COMMAND_NAME)
            .hasArg()
            .desc("The query command that you want to execute. (optional)")
            .build();
    options.addOption(opQuery);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .argName(LINES_PER_FILE_ARGS_NAME)
            .hasArg()
            .desc("Lines per dump file.")
            .build();
    options.addOption(opLinesPerFile);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opTimeout =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .hasArg()
            .desc("Timeout for session query")
            .build();
    options.addOption(opTimeout);
    return options;
  }

  /**
   * This method will be called, if the query commands are written in a sql file.
   *
   * @param filePath
   * @throws IOException
   */
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
   * @param index used to create dump file name
   */
  private static void dumpResult(String sql, int index) {
    final String path = targetDirectory + targetFile + index;
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql, timeout);
      List<Object> headers = new ArrayList<>();
      List<String> names = sessionDataSet.getColumnNames();
      List<String> types = sessionDataSet.getColumnTypes();
      if (needDataTypePrinted) {
        for (int i = 0; i < names.size(); i++) {
          if (!"Time".equals(names.get(i)) && !"Device".equals(names.get(i))) {
            headers.add(String.format("%s(%s)", names.get(i), types.get(i)));
          } else {
            headers.add(names.get(i));
          }
        }
      } else {
        headers.addAll(names);
      }
      writeCsvFile(sessionDataSet, path, headers, linesPerFile);
      sessionDataSet.closeOperationHandle();
      System.out.println("Export completely!");
    } catch (StatementExecutionException | IoTDBConnectionException | IOException e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  public static String timeTrans(Long time) {
    switch (timeFormat) {
      case "default":
        return RpcUtils.parseLongToDateWithPrecision(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME, time, zoneId, timestampPrecision);
      case "timestamp":
      case "long":
      case "number":
        return String.valueOf(time);
      default:
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), zoneId)
            .format(DateTimeFormatter.ofPattern(timeFormat));
    }
  }

  public static void writeCsvFile(
      SessionDataSet sessionDataSet, String filePath, List<Object> headers, int linesPerFile)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    int fileIndex = 0;
    boolean hasNext = true;
    while (hasNext) {
      int i = 0;
      final String finalFilePath = filePath + "_" + fileIndex + ".csv";
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
      csvPrinterWrapper.printRecord(headers);
      while (i++ < linesPerFile) {
        if (sessionDataSet.hasNext()) {
          RowRecord rowRecord = sessionDataSet.next();
          if (rowRecord.getTimestamp() != 0) {
            csvPrinterWrapper.print(timeTrans(rowRecord.getTimestamp()));
          }
          rowRecord
              .getFields()
              .forEach(
                  field -> {
                    String fieldStringValue = field.getStringValue();
                    if (!"null".equals(field.getStringValue())) {
                      if (field.getDataType() == TSDataType.TEXT
                          && !fieldStringValue.startsWith("root.")) {
                        fieldStringValue = "\"" + fieldStringValue + "\"";
                      }
                      csvPrinterWrapper.print(fieldStringValue);
                    } else {
                      csvPrinterWrapper.print("");
                    }
                  });
          csvPrinterWrapper.println();
        } else {
          hasNext = false;
          break;
        }
      }
      fileIndex++;
      csvPrinterWrapper.flush();
      csvPrinterWrapper.close();
    }
  }
}
