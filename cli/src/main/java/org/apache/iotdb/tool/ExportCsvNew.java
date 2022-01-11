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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.annotation.Nullable;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ExportCsvNew extends AbstractCsvTool {

  public static Log logger = LogFactory.getLog(ExportCsvNew.class);

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

  private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;

  public static final int POINT_FEEDBACK_SIZE = 10_000_000;

  private static String targetDirectory;

  private static Boolean needDataTypePrinted;

  private static String queryCommand;

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

    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
      if (!checkTimeFormat()) {
        return;
      }

      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);
      setTimeZone();

      if (queryCommand == null) {
        String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
        String sql;

        if (sqlFile == null) {
          LineReader lineReader = JlineUtils.getLineReader();
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
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Connect failed because " + e.getMessage());
    } finally {
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
    needDataTypePrinted = Boolean.valueOf(commandLine.getOptionValue(DATA_TYPE_ARGS));
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);

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
    String compressAlgorithm = commandLine.getOptionValue(COMPRESS_ARGS);
    compressMode = CompressMode.forValue(compressAlgorithm);
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

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opCompress =
        Option.builder(COMPRESS_ARGS)
            .longOpt(COMPRESS_NAME)
            .argName(COMPRESS_NAME)
            .hasArg()
            .desc("Type algorithm for compress, snappy or gzip is available. (optional)")
            .build();
    options.addOption(opCompress);

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
   * @param index use to create dump file name
   */
  private static void dumpResult(String sql, int index) {
    String path =
        new StringBuilder(targetDirectory)
            .append(targetFile)
            .append(index)
            .append(".csv")
            .toString();
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 1000);
      if (!CompressMode.PLAIN.equals(compressMode)) {
        path = new StringBuilder(path).append(compressMode.getSuffix()).toString();
        PipedInputStream input = new PipedInputStream();
        final PipedOutputStream out = new PipedOutputStream((PipedInputStream) input);
        new Thread(
                new Runnable() {
                  public void run() {
                    try {
                      writeCsvFile(null, sessionDataSet, out);
                    } catch (Exception e) {
                    }
                  }
                })
            .start();
        OutputStream out2 = new FileOutputStream(new File(path));
        if (CompressMode.GZIP.equals(compressMode)) {
          CompressUtil.gzipCompress(input, out2);
        } else if (CompressMode.SNAPPY.equals(compressMode)) {
          CompressUtil.snappyCompress(input, out2);
        }
      } else {
        writeCsvFile(null, sessionDataSet, new FileOutputStream(new File(path)));
      }
      System.out.println("Export completely!");
    } catch (Exception e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static Boolean writeCsvFile(
      @Nullable List<String> headerNames, SessionDataSet sessionDataSet, OutputStream out)
      throws Exception {

    List<String> columnNameList = sessionDataSet.getColumnNames();
    List<String> columnTypeList = sessionDataSet.getColumnTypes();

    DataIterator it = sessionDataSet.iterator();
    CSVPrinter printer = CSVFormat.DEFAULT.print(new PrintWriter(out, true));
    if (headerNames != null) {
      printer.printRecord(headerNames);
      printer.println();
    }

    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      if ("Time".equals(name)) {
        printer.print(name);
      } else {
        String type = columnTypeList.get(i);
        if (needDataTypePrinted && !"Device".equals(name)) {
          printer.print(new StringBuilder(name).append("(").append(type).append(")").toString());
        } else {
          printer.print(name);
        }
      }
    }
    printer.println();
    int length = columnNameList.size();
    Long[] finishedCount = new Long[] {0L, 0L}; // 0: finishRowCount; 1: finishPointCountTemp;
    while (it.next()) {
      for (int i = 0; i < length; i++) {
        String s = columnNameList.get(i);
        Object o = it.getObject(s);
        if (i == 0 && o != null) {
          try {
            Long ol = Long.valueOf(o.toString());
            String t = timeTrans(ol);
            printer.print(t);
          } catch (Exception e) {
            printer.print(null);
          }
        } else {
          printer.print(o);
        }
      }
      printer.println();
      ongoingFeedback(finishedCount, length);
    }
    printer.flush();
    printer.close();
    finishFeedback(finishedCount, length);
    return true;
  }

  private static void ongoingFeedback(Long[] finishedCount, long length) {
    finishedCount[0]++;
    finishedCount[1] += (length - 1);
    if (finishedCount[1] >= POINT_FEEDBACK_SIZE) {
      finishedCount[1] = finishedCount[1] % POINT_FEEDBACK_SIZE;
      logger.warn(
          "Exported "
              + (length - 1) * finishedCount[0]
              + " points in "
              + finishedCount[0]
              + " rows");
    }
  }

  private static void finishFeedback(Long[] finishedCount, long length) {
    logger.warn(
        "Export finish, total "
            + (length - 1) * finishedCount[0]
            + " points in "
            + finishedCount[0]
            + " rows");
  }

  public static String timeTrans(Long time) {
    String timestampPrecision = "ms";
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
}
