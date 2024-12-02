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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.tsfile.ExportTsFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

/**
 * Export CSV file.
 *
 * @version 1.0.0 20170719
 */
public class ExportData extends AbstractDataTool {

  private static final String TARGET_DIR_ARGS = "t";
  private static final String TARGET_DIR_NAME = "target";
  private static final String TARGET_DIR_ARGS_NAME = "target_directory";

  private static final String TARGET_FILE_ARGS = "pfn";
  private static final String TARGET_FILE_NAME = "prefix_file_name";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sourceSqlFile";

  private static final String DATA_TYPE_ARGS = "dt";
  private static final String DATA_TYPE_NAME = "datatype";

  private static final String QUERY_COMMAND_ARGS = "q";
  private static final String QUERY_COMMAND_NAME = "query";
  private static final String QUERY_COMMAND_ARGS_NAME = "query_command";

  private static final String EXPORT_SQL_TYPE_NAME = "sql";

  private static final String ALIGNED_ARGS = "aligned";
  private static final String ALIGNED_NAME = "export_aligned";
  private static final String ALIGNED_ARGS_NAME = "export aligned insert sql";
  private static final String LINES_PER_FILE_ARGS = "lpf";
  private static final String LINES_PER_FILE_NAME = "lines_per_file";

  private static final String TSFILEDB_CLI_PREFIX = "Export Data";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;
  private static Session session;
  private static String targetDirectory;

  private static Boolean needDataTypePrinted;

  private static String queryCommand;

  private static String timestampPrecision;

  private static int linesPerFile = 10000;

  private static long timeout = -1;

  private static Boolean aligned = false;
  private static String fileType = null;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static final String TSFILEDB_CLI_HEAD =
      "Please obtain help information for the corresponding data type based on different parameters, for example:\n"
          + "./export_data.sh -help tsfile\n"
          + "./export_data.sh -help sql\n"
          + "./export_data.sh -help csv";

  @SuppressWarnings({
    "squid:S3776",
    "squid:S2093"
  }) // Suppress high Cognitive Complexity warning, ignore try-with-resources
  /* main function of export csv tool. */
  public static void main(String[] args) {
    Options helpOptions = createHelpOptions();
    Options tsFileOptions = createTsFileOptions();
    Options csvOptions = createCsvOptions();
    Options sqlOptions = createSqlOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      printHelpOptions(
          TSFILEDB_CLI_HEAD, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(helpOptions, args, true);
    } catch (ParseException e) {
      printHelpOptions(
          TSFILEDB_CLI_HEAD, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    final List<String> argList = Arrays.asList(args);
    int helpIndex = argList.indexOf(MINUS + HELP_ARGS);
    int ftIndex = argList.indexOf(MINUS + FILE_TYPE_ARGS);
    if (ftIndex < 0) {
      ftIndex = argList.indexOf(MINUS + FILE_TYPE_NAME);
    }
    if (helpIndex >= 0) {
      fileType = argList.get(helpIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, null, null, false);
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, null, csvOptions, null, false);
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, null, null, sqlOptions, false);
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              TSFILEDB_CLI_HEAD,
              TSFILEDB_CLI_PREFIX,
              hf,
              tsFileOptions,
              csvOptions,
              sqlOptions,
              true);
        }
      } else {
        printHelpOptions(
            TSFILEDB_CLI_HEAD,
            TSFILEDB_CLI_PREFIX,
            hf,
            tsFileOptions,
            csvOptions,
            sqlOptions,
            true);
      }
      System.exit(CODE_ERROR);
    } else if (ftIndex >= 0) {
      fileType = argList.get(ftIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(tsFileOptions, args, true);
            ExportTsFile exportTsFile = new ExportTsFile(commandLine);
            exportTsFile.exportTsfile(CODE_OK);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, null, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(csvOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, null, csvOptions, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(sqlOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, null, null, sqlOptions, false);
            System.exit(CODE_ERROR);
          }
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              TSFILEDB_CLI_HEAD,
              TSFILEDB_CLI_PREFIX,
              hf,
              tsFileOptions,
              csvOptions,
              sqlOptions,
              true);
          System.exit(CODE_ERROR);
        }
      } else {
        printHelpOptions(
            TSFILEDB_CLI_HEAD,
            TSFILEDB_CLI_PREFIX,
            hf,
            tsFileOptions,
            csvOptions,
            sqlOptions,
            true);
        System.exit(CODE_ERROR);
      }
    } else {
      ioTPrinter.println(
          String.format(
              "Invalid args: Required values for option '%s' not provided", FILE_TYPE_NAME));
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
          LineReader lineReader =
              JlineUtils.getLineReader(
                  new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
                  username,
                  host,
                  port);
          sql = lineReader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
          ioTPrinter.println(sql);
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
      ioTPrinter.println("Failed to operate on file, because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      ioTPrinter.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (TException e) {
      ioTPrinter.println(
          "Can not get the timestamp precision from server because " + e.getMessage());
      exitCode = CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          exitCode = CODE_ERROR;
          ioTPrinter.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
    System.exit(exitCode);
  }

  private static Options createTsFileOptions() {
    Options options = createExportOptions();

    Option opFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .longOpt(TARGET_DIR_NAME)
            .argName(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_NAME)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc("Export file name (optional)")
            .build();
    options.addOption(opOnSuccess);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .longOpt(QUERY_COMMAND_NAME)
            .argName(QUERY_COMMAND_ARGS_NAME)
            .hasArg()
            .desc("The query command that you want to execute. (optional)")
            .build();
    options.addOption(opQuery);

    Option opTimeOut =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc("Timeout for session query (optional)")
            .build();
    options.addOption(opTimeOut);

    return options;
  }

  private static Options createCsvOptions() {
    Options options = createExportOptions();

    Option opFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .longOpt(TARGET_DIR_NAME)
            .argName(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_NAME)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc("Export file name (optional)")
            .build();
    options.addOption(opOnSuccess);

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .longOpt(DATA_TYPE_NAME)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(
                "Will the data type of timeseries be printed in the head line of the CSV file?"
                    + '\n'
                    + "You can choose true) or false) . (optional)")
            .build();
    options.addOption(opDataType);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .argName(LINES_PER_FILE_NAME)
            .hasArg()
            .desc("Lines per dump file.(optional)")
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(
                "Output time Format in csv file. "
                    + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
                    + "user-defined pattern like yyyy-MM-dd HH:mm:ss, default ISO8601.\n OutPut timestamp in sql file, No matter what time format is set(optional)")
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIMEOUT_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .longOpt(QUERY_COMMAND_NAME)
            .argName(QUERY_COMMAND_ARGS_NAME)
            .hasArg()
            .desc("The query command that you want to execute. (optional)")
            .build();
    options.addOption(opQuery);

    Option opTimeOut =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc("Timeout for session query (optional)")
            .build();
    options.addOption(opTimeOut);

    return options;
  }

  private static Options createSqlOptions() {
    Options options = createExportOptions();

    Option opFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .longOpt(TARGET_DIR_NAME)
            .argName(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_NAME)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc("Export file name (optional)")
            .build();
    options.addOption(opOnSuccess);

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .longOpt(ALIGNED_NAME)
            .argName(ALIGNED_ARGS_NAME)
            .hasArgs()
            .desc("Whether export to sql of aligned (optional)")
            .build();
    options.addOption(opAligned);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(
                "Output time Format in csv file. "
                    + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
                    + "user-defined pattern like yyyy-MM-dd HH:mm:ss, default ISO8601.\n OutPut timestamp in sql file, No matter what time format is set(optional)")
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .longOpt(QUERY_COMMAND_NAME)
            .argName(QUERY_COMMAND_ARGS_NAME)
            .hasArg()
            .desc("The query command that you want to execute. (optional)")
            .build();
    options.addOption(opQuery);

    Option opTimeOut =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc("Timeout for session query (optional)")
            .build();
    options.addOption(opTimeOut);

    return options;
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine, null);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
    needDataTypePrinted = Boolean.valueOf(commandLine.getOptionValue(DATA_TYPE_ARGS));
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);
    exportType = commandLine.getOptionValue(FILE_TYPE_ARGS);
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
    final File file = new File(targetDirectory);
    if (!file.isDirectory()) {
      ioTPrinter.println(
          String.format("Source file or directory %s does not exist", targetDirectory));
      System.exit(CODE_ERROR);
    }
    if (commandLine.getOptionValue(LINES_PER_FILE_ARGS) != null) {
      linesPerFile = Integer.parseInt(commandLine.getOptionValue(LINES_PER_FILE_ARGS));
    }
    if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
    }
  }

  protected static void setTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    if (timeZoneID != null) {
      session.setTimeZone(timeZoneID);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  /**
   * This method will be called, if the query commands are written in a sql file.
   *
   * @param filePath sql file path
   * @throws IOException exception
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
    if (EXPORT_SQL_TYPE_NAME.equalsIgnoreCase(exportType)) {
      legalCheck(sql);
    }
    final String path = targetDirectory + targetFile + index;
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql, timeout);
      List<Object> headers = new ArrayList<>();
      List<String> names = sessionDataSet.getColumnNames();
      List<String> types = sessionDataSet.getColumnTypes();
      if (EXPORT_SQL_TYPE_NAME.equalsIgnoreCase(exportType)) {
        writeSqlFile(sessionDataSet, path, names, linesPerFile);
      } else {
        if (Boolean.TRUE.equals(needDataTypePrinted)) {
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
      }
      sessionDataSet.closeOperationHandle();
      ioTPrinter.println("Export completely!");
    } catch (StatementExecutionException | IoTDBConnectionException | IOException e) {
      ioTPrinter.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static void legalCheck(String sql) {
    String aggregatePattern =
        "\\b(count|sum|avg|extreme|max_value|min_value|first_value|last_value|max_time|min_time|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|max_by|min_by)\\b\\s*\\(";
    Pattern pattern = Pattern.compile(aggregatePattern, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(sql.toUpperCase(Locale.ROOT));
    if (matcher.find()) {
      ioTPrinter.println("The sql you entered is invalid, please don't use aggregate query.");
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

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
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
                      if ((field.getDataType() == TSDataType.TEXT
                              || field.getDataType() == TSDataType.STRING)
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

  public static void writeSqlFile(
      SessionDataSet sessionDataSet, String filePath, List<String> headers, int linesPerFile)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    int fileIndex = 0;
    String deviceName = null;
    boolean writeNull = false;
    List<String> seriesList = new ArrayList<>(headers);
    if (CollectionUtils.isEmpty(headers) || headers.size() <= 1) {
      writeNull = true;
    } else {
      if (headers.contains("Device")) {
        seriesList.remove("Time");
        seriesList.remove("Device");
      } else {
        Path path = new Path(seriesList.get(1), true);
        deviceName = path.getDeviceString();
        seriesList.remove("Time");
        for (int i = 0; i < seriesList.size(); i++) {
          String series = seriesList.get(i);
          path = new Path(series, true);
          seriesList.set(i, path.getMeasurement());
        }
      }
    }
    boolean hasNext = true;
    while (hasNext) {
      int i = 0;
      final String finalFilePath = filePath + "_" + fileIndex + ".sql";
      try (FileWriter writer = new FileWriter(finalFilePath)) {
        if (writeNull) {
          break;
        }
        while (i++ < linesPerFile) {
          if (sessionDataSet.hasNext()) {
            RowRecord rowRecord = sessionDataSet.next();
            List<Field> fields = rowRecord.getFields();
            List<String> headersTemp = new ArrayList<>(seriesList);
            List<String> timeseries = new ArrayList<>();
            if (headers.contains("Device")) {
              deviceName = fields.get(0).toString();
              if (deviceName.startsWith(SYSTEM_DATABASE + ".")) {
                continue;
              }
              for (String header : headersTemp) {
                timeseries.add(deviceName + "." + header);
              }
            } else {
              if (headers.get(1).startsWith(SYSTEM_DATABASE + ".")) {
                continue;
              }
              timeseries.addAll(headers);
              timeseries.remove(0);
            }
            String sqlMiddle = null;
            if (Boolean.TRUE.equals(aligned)) {
              sqlMiddle = " ALIGNED VALUES (" + rowRecord.getTimestamp() + ",";
            } else {
              sqlMiddle = " VALUES (" + rowRecord.getTimestamp() + ",";
            }
            List<String> values = new ArrayList<>();
            if (headers.contains("Device")) {
              fields.remove(0);
            }
            for (int index = 0; index < fields.size(); index++) {
              RowRecord next =
                  session
                      .executeQueryStatement("SHOW TIMESERIES " + timeseries.get(index), timeout)
                      .next();
              if (ObjectUtils.isNotEmpty(next)) {
                List<Field> timeseriesList = next.getFields();
                String value = fields.get(index).toString();
                if (value.equals("null")) {
                  headersTemp.remove(seriesList.get(index));
                  continue;
                }
                if ("TEXT".equalsIgnoreCase(timeseriesList.get(3).getStringValue())) {
                  values.add("\"" + value + "\"");
                } else {
                  values.add(value);
                }
              } else {
                headersTemp.remove(seriesList.get(index));
                continue;
              }
            }
            if (CollectionUtils.isNotEmpty(headersTemp)) {
              writer.write(
                  "INSERT INTO "
                      + deviceName
                      + "(TIMESTAMP,"
                      + String.join(",", headersTemp)
                      + ")"
                      + sqlMiddle
                      + String.join(",", values)
                      + ");\n");
            }

          } else {
            hasNext = false;
            break;
          }
        }
        fileIndex++;
        writer.flush();
      }
    }
  }
}
