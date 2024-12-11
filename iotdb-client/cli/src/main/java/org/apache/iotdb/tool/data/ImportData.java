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

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.tsfile.ImportTsFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ImportData extends AbstractDataTool {

  private static final String FILE_ARGS = "s";
  private static final String FILE_NAME = "source";

  private static final String ON_SUCCESS_ARGS = "os";
  private static final String ON_SUCCESS_NAME = "on_success";

  private static final String SUCCESS_DIR_ARGS = "sd";
  private static final String SUCCESS_DIR_NAME = "success_dir";

  private static final String FAIL_DIR_ARGS = "fd";
  private static final String FAIL_DIR_NAME = "fail_dir";

  private static final String ON_FAIL_ARGS = "of";
  private static final String ON_FAIL_NAME = "on_fail";

  private static final String THREAD_NUM_ARGS = "tn";
  private static final String THREAD_NUM_NAME = "thread_num";

  private static final String BATCH_POINT_SIZE_ARGS = "batch";
  private static final String BATCH_POINT_SIZE_NAME = "batch_size";
  private static final String BATCH_POINT_SIZE_ARGS_NAME = "batch_size";

  private static final String ALIGNED_ARGS = "aligned";
  private static final String ALIGNED_NAME = "use_aligned";
  private static final String ALIGNED_ARGS_NAME = "use the aligned interface";

  private static final String TIMESTAMP_PRECISION_ARGS = "tp";
  private static final String TIMESTAMP_PRECISION_NAME = "timestamp_precision";
  private static final String TIMESTAMP_PRECISION_ARGS_NAME = "timestamp precision (ms/us/ns)";

  private static final String TYPE_INFER_ARGS = "ti";
  private static final String TYPE_INFER_NAME = "type_infer";

  private static final String LINES_PER_FAILED_FILE_ARGS = "lpf";
  private static final String LINES_PER_FAILED_FILE_ARGS_NAME = "lines_per_failed_file";

  private static final String TSFILEDB_CLI_PREFIX = "Import Data";
  private static final String TSFILEDB_CLI_HEAD =
      "Please obtain help information for the corresponding data type based on different parameters, for example:\n"
          + "./import_data.sh -help tsfile\n"
          + "./import_data.sh -help sql\n"
          + "./import_data.sh -help csv";

  private static String targetPath;
  private static String fileType = null;
  private static Boolean aligned = false;
  private static int threadNum = 8;
  private static SessionPool sessionPool;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static Options createTsFileOptions() {
    Options options = createImportOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc("The local directory path of the script file (folder) to be loaded. (required)")
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_NAME)
            .argName(ON_SUCCESS_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile successfully, do operation on tsfile (and its .resource and .mods files), "
                    + "optional parameters are none, mv, cp, delete. (required)")
            .build();
    options.addOption(opOnSuccess);

    Option opSuccessDir =
        Option.builder(SUCCESS_DIR_ARGS)
            .longOpt(SUCCESS_DIR_NAME)
            .argName(SUCCESS_DIR_NAME)
            .hasArg()
            .desc("The target folder when 'os' is 'mv' or 'cp'.(optional)")
            .build();
    options.addOption(opSuccessDir);

    Option opOnFail =
        Option.builder(ON_FAIL_ARGS)
            .longOpt(ON_FAIL_NAME)
            .argName(ON_FAIL_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile fail, do operation on tsfile (and its .resource and .mods files), "
                    + "optional parameters are none, mv, cp, delete. (required)")
            .build();
    options.addOption(opOnFail);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc("The target folder when 'of' is 'mv' or 'cp'.(optional)")
            .build();
    options.addOption(opFailDir);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc("The number of threads used to import tsfile, default is 8.(optional)")
            .build();
    options.addOption(opThreadNum);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc("Timestamp precision (ms/us/ns) (optional)")
            .build();

    options.addOption(opTimestampPrecision);
    return options;
  }

  private static Options createCsvOptions() {
    Options options = createImportOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .required()
            .hasArg()
            .desc("The local directory path of the script file (folder) to be loaded. (required)")
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(
                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .longOpt(LINES_PER_FAILED_FILE_ARGS_NAME)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc("Lines per failed file (optional)")
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .longOpt(ALIGNED_NAME)
            .argName(ALIGNED_ARGS_NAME)
            .hasArg()
            .desc("Whether to use the interface of aligned (optional)")
            .build();
    options.addOption(opAligned);

    Option opTypeInfer =
        Option.builder(TYPE_INFER_ARGS)
            .longOpt(TYPE_INFER_NAME)
            .argName(TYPE_INFER_NAME)
            .numberOfArgs(5)
            .hasArgs()
            .valueSeparator(',')
            .desc("Define type info by option:\"boolean=text,int=long, ... (optional)")
            .build();
    options.addOption(opTypeInfer);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc("Timestamp precision (ms/us/ns) (optional)")
            .build();

    options.addOption(opTimestampPrecision);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_ARGS_NAME)
            .hasArg()
            .desc("100000 (optional)")
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc("The number of threads used to import tsfile, default is 8. (optional)")
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  private static Options createSqlOptions() {
    Options options = createImportOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc("The local directory path of the script file (folder) to be loaded. (required)")
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(
                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc("Lines per failed file (optional)")
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .desc("100000 (optional)")
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArgs()
            .desc("The number of threads used to import tsfile, default is 8. (optional)")
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  /**
   * parse optional params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    targetPath = commandLine.getOptionValue(FILE_ARGS);
    if (commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS) != null) {
      batchPointSize = Integer.parseInt(commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS));
    }
    if (commandLine.getOptionValue(FAIL_DIR_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAIL_DIR_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      } else if (!failedFileDirectory.endsWith("/") && !failedFileDirectory.endsWith("\\")) {
        failedFileDirectory += File.separator;
      }
    }
    if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
    }
    if (commandLine.getOptionValue(THREAD_NUM_ARGS) != null) {
      threadNum = Integer.parseInt(commandLine.getOptionValue(THREAD_NUM_ARGS));
      if (threadNum <= 0) {
        ioTPrinter.println(
            String.format(
                "error: Invalid thread number '%s'. Please set a positive integer.", threadNum));
        System.exit(CODE_ERROR);
      }
    }
    if (commandLine.getOptionValue(TIMESTAMP_PRECISION_ARGS) != null) {
      timestampPrecision = commandLine.getOptionValue(TIMESTAMP_PRECISION_ARGS);
    }
    final String[] opTypeInferValues = commandLine.getOptionValues(TYPE_INFER_ARGS);
    if (opTypeInferValues != null && opTypeInferValues.length > 0) {
      for (String opTypeInferValue : opTypeInferValues) {
        if (opTypeInferValue.contains("=")) {
          final String[] typeInfoExpressionArr = opTypeInferValue.split("=");
          final String key = typeInfoExpressionArr[0];
          final String value = typeInfoExpressionArr[1];
          applyTypeInferArgs(key, value);
        }
      }
    }
    if (commandLine.getOptionValue(LINES_PER_FAILED_FILE_ARGS) != null) {
      linesPerFailedFile = Integer.parseInt(commandLine.getOptionValue(LINES_PER_FAILED_FILE_ARGS));
    }
  }

  private static void applyTypeInferArgs(String key, String value) throws ArgsErrorException {
    if (!TYPE_INFER_KEY_DICT.containsKey(key)) {
      throw new ArgsErrorException("Unknown type infer key: " + key);
    }
    if (!TYPE_INFER_VALUE_DICT.containsKey(value)) {
      throw new ArgsErrorException("Unknown type infer value: " + value);
    }
    if (key.equals(DATATYPE_NAN)
        && !(value.equals(DATATYPE_FLOAT)
            || value.equals(DATATYPE_DOUBLE)
            || value.equals(DATATYPE_TEXT))) {
      throw new ArgsErrorException("NaN can not convert to " + value);
    }
    if (key.equals(DATATYPE_BOOLEAN)
        && !(value.equals(DATATYPE_BOOLEAN) || value.equals(DATATYPE_TEXT))) {
      throw new ArgsErrorException("Boolean can not convert to " + value);
    }
    final TSDataType srcType = TYPE_INFER_VALUE_DICT.get(key);
    final TSDataType dstType = TYPE_INFER_VALUE_DICT.get(value);
    if (dstType.getType() < srcType.getType()) {
      throw new ArgsErrorException(key + " can not convert to " + value);
    }
    TYPE_INFER_KEY_DICT.put(key, TYPE_INFER_VALUE_DICT.get(value));
  }

  public static void main(String[] args) throws IoTDBConnectionException {
    Options helpOptions = createHelpOptions();
    Options tsFileOptions = createTsFileOptions();
    Options csvOptions = createCsvOptions();
    Options sqlOptions = createSqlOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      printHelpOptions(
          TSFILEDB_CLI_HEAD, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(helpOptions, args, true);
    } catch (org.apache.commons.cli.ParseException e) {
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
            commandLine = parser.parse(tsFileOptions, args);
            ImportTsFile.importData(commandLine);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, null, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(csvOptions, args);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, TSFILEDB_CLI_PREFIX, hf, null, csvOptions, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(sqlOptions, args);
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
        ioTPrinter.println(
            String.format(
                "Invalid args: Required values for option '%s' not provided", FILE_TYPE_NAME));
        System.exit(CODE_ERROR);
      }
    } else {
      ioTPrinter.println(
          String.format(
              "Invalid args: Required values for option '%s' not provided", FILE_TYPE_NAME));
      System.exit(CODE_ERROR);
    }

    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        ioTPrinter.println(TSFILEDB_CLI_HEAD);
        printHelpOptions(
            null, TSFILEDB_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
        System.exit(CODE_ERROR);
      }
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Args error: " + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e) {
      ioTPrinter.println("Encounter an error, because: " + e.getMessage());
      System.exit(CODE_ERROR);
    }
    final int resultCode = importFromTargetPathAsync();
    if (ImportDataScanTool.getTsFileQueueSize() <= 0) {
      System.exit(CODE_OK);
    }
    asyncImportDataFiles();
    System.exit(resultCode);
  }

  private static void asyncImportDataFiles() {
    List<Thread> list = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      final Thread thread = new Thread(new AsyncImportData());
      thread.start();
      list.add(thread);
    }
    list.forEach(
        thread -> {
          try {
            thread.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ioTPrinter.println("ImportData thread join interrupted: " + e.getMessage());
          }
        });
    ioTPrinter.println("Import completely!");
  }

  private static int importFromTargetPathAsync() {
    try {
      sessionPool =
          new SessionPool.Builder()
              .host(host)
              .port(Integer.parseInt(port))
              .user(username)
              .password(password)
              .maxSize(threadNum + 1)
              .enableCompression(false)
              .enableRedirection(false)
              .enableAutoFetch(false)
              .build();
      sessionPool.setEnableQueryRedirection(false);
      AsyncImportData.setAligned(aligned);
      AsyncImportData.setSessionPool(sessionPool);
      AsyncImportData.setTimeZone();
      ImportDataScanTool.setSourceFullPath(targetPath);
      final File file = new File(targetPath);
      if (!file.isFile() && !file.isDirectory()) {
        ioTPrinter.println(String.format("Source file or directory %s does not exist", targetPath));
        System.exit(CODE_ERROR);
      }
      ImportDataScanTool.traverseAndCollectFiles();
      asyncImportDataFiles();
      return CODE_OK;
    } catch (InterruptedException e) {
      ioTPrinter.println(String.format("Import tsfile fail: %s", e.getMessage()));
      Thread.currentThread().interrupt();
      return CODE_ERROR;
    } catch (Exception e) {
      ioTPrinter.println(String.format("Import tsfile fail: %s", e.getMessage()));
      return CODE_ERROR;
    }
  }

  /**
   * Specifying a CSV file or a directory including CSV files that you want to import. This method
   * can be offered to console cli to implement importing CSV file by command.
   *
   * @param host
   * @param port
   * @param username
   * @param password
   * @param targetPath a CSV file or a directory including CSV files
   * @param timeZone
   * @return the status code
   * @throws IoTDBConnectionException
   */
  @SuppressWarnings({"squid:S2093"}) // ignore try-with-resources
  public static int importFromTargetPath(
      String host, int port, String username, String password, String targetPath, String timeZone)
      throws IoTDBConnectionException {
    try {
      session = new Session(host, port, username, password, false);
      session.open(false);
      timeZoneID = timeZone;
      setTimeZone();

      File file = new File(targetPath);
      if (file.isFile()) {
        if (file.getName().endsWith(SQL_SUFFIXS)) {
          importFromSqlFile(session, file);
        } else {
          importFromSingleFile(session, file);
        }
      } else if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files == null) {
          return CODE_OK;
        }

        for (File subFile : files) {
          if (subFile.isFile()) {
            if (subFile.getName().endsWith(SQL_SUFFIXS)) {
              importFromSqlFile(session, subFile);
            } else {
              importFromSingleFile(session, subFile);
            }
          }
        }
      } else {
        ioTPrinter.println("File not found!");
        return CODE_ERROR;
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      ioTPrinter.println("Encounter an error when connecting to server, because " + e.getMessage());
      return CODE_ERROR;
    } finally {
      if (session != null) {
        session.close();
      }
    }
    return CODE_OK;
  }

  /**
   * import the CSV file and load headers and records.
   *
   * @param file the File object of the CSV file that you want to import.
   */
  private static void importFromSingleFile(Session session, File file) {
    if (file.getName().endsWith(CSV_SUFFIXS) || file.getName().endsWith(TXT_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          ioTPrinter.println("Empty file!");
          return;
        }
        if (!timeColumn.equalsIgnoreCase(filterBomHeader(headerNames.get(0)))) {
          ioTPrinter.println("The first field of header must be `Time`!");
          return;
        }
        String failedFilePath = null;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        if (!deviceColumn.equalsIgnoreCase(headerNames.get(1))) {
          writeDataAlignedByTime(session, headerNames, records, failedFilePath);
        } else {
          writeDataAlignedByDevice(session, headerNames, records, failedFilePath);
        }
      } catch (IOException | IllegalPathException e) {
        ioTPrinter.println("CSV file read exception because: " + e.getMessage());
      }
    } else {
      ioTPrinter.println("The file name must end with \"csv\" or \"txt\"!");
    }
  }

  @SuppressWarnings("java:S2259")
  private static void importFromSqlFile(Session session, File file) {
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    String failedFilePath = null;
    if (failedFileDirectory == null) {
      failedFilePath = file.getAbsolutePath() + ".failed";
    } else {
      failedFilePath = failedFileDirectory + file.getName() + ".failed";
    }
    try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
      String sql;
      while ((sql = br.readLine()) != null) {
        try {
          session.executeNonQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          failedRecords.add(Arrays.asList(sql));
        }
      }
      ioTPrinter.println(file.getName() + " Import completely!");
    } catch (IOException e) {
      ioTPrinter.println("SQL file read exception because: " + e.getMessage());
    }
    if (!failedRecords.isEmpty()) {
      FileWriter writer = null;
      try {
        writer = new FileWriter(failedFilePath);
        for (List<Object> failedRecord : failedRecords) {
          writer.write(failedRecord.get(0).toString() + "\n");
        }
      } catch (IOException e) {
        ioTPrinter.println("Cannot dump fail result because: " + e.getMessage());
      } finally {
        if (ObjectUtils.isNotEmpty(writer)) {
          try {
            writer.flush();
            writer.close();
          } catch (IOException e) {
          }
        }
      }
    }
  }
}
