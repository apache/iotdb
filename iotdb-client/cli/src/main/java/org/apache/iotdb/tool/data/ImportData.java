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
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.OptionsUtil;
import org.apache.iotdb.tool.tsfile.ImportTsFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
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
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.iotdb.tool.common.Constants.*;

public class ImportData extends AbstractDataTool {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static Session session;

  public static void main(String[] args) throws IoTDBConnectionException {
    Options helpOptions = OptionsUtil.createHelpOptions();
    Options tsFileOptions = OptionsUtil.createImportTsFileOptions();
    Options csvOptions = OptionsUtil.createImportCsvOptions();
    Options sqlOptions = OptionsUtil.createImportSqlOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      printHelpOptions(
          IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(helpOptions, args, true);
    } catch (org.apache.commons.cli.ParseException e) {
      printHelpOptions(
          IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    final List<String> argList = Arrays.asList(args);
    int helpIndex = argList.indexOf(MINUS + HELP_ARGS);
    int sql_dialect = argList.indexOf(MINUS + SQL_DIALECT_ARGS); // -sql_dialect
    if (sql_dialect >= 0
        && !SQL_DIALECT_VALUE_TREE.equalsIgnoreCase(argList.get(sql_dialect + 1))) {
      final String sqlDialectValue = argList.get(sql_dialect + 1);
      if (SQL_DIALECT_VALUE_TABLE.equalsIgnoreCase(sqlDialectValue)) {
        sqlDialectTree = false;
        tsFileOptions = OptionsUtil.createTableImportTsFileOptions();
        csvOptions = OptionsUtil.createTableImportCsvOptions();
        sqlOptions = OptionsUtil.createTableImportSqlOptions();
      } else {
        ioTPrinter.println(String.format("sql_dialect %s is not support", sqlDialectValue));
        printHelpOptions(
            IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
        System.exit(CODE_ERROR);
      }
    }
    int ftIndex = argList.indexOf(MINUS + FILE_TYPE_ARGS); // -ft
    if (ftIndex < 0) {
      ftIndex = argList.indexOf(MINUS + FILE_TYPE_NAME); // -file_type
    }
    if (helpIndex >= 0) {
      fileType = argList.get(helpIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, IMPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, IMPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, IMPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
        }
      } else {
        printHelpOptions(
            IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      }
      System.exit(CODE_ERROR);
    } else if (ftIndex >= 0) {
      fileType = argList.get(ftIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(tsFileOptions, args);
            //            ImportTsFile.importData(commandLine);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, IMPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(csvOptions, args);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, IMPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(sqlOptions, args);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, IMPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
            System.exit(CODE_ERROR);
          }
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
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
        ioTPrinter.println(IMPORT_CLI_HEAD);
        printHelpOptions(null, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
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
    int resultCode = importFromTargetPathAsync();
    System.exit(resultCode);
  }

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
    if (commandLine.getOptionValue(DB_ARGS) != null) {
      database = commandLine.getOptionValue(DB_ARGS);
    }
    if (commandLine.getOptionValue(TABLE_ARGS) != null) {
      table = commandLine.getOptionValue(TABLE_ARGS);
    }
    if (commandLine.getOptionValue(START_TIME_ARGS) != null) {
      startTime = commandLine.getOptionValue(START_TIME_ARGS);
    }
    if (commandLine.getOptionValue(END_TIME_ARGS) != null) {
      endTime = commandLine.getOptionValue(END_TIME_ARGS);
    }
    try {
      isRemoteLoad = !NodeUrlUtils.containsLocalAddress(Collections.singletonList(host));
      if (!sqlDialectTree && isRemoteLoad) {
        ioTPrinter.println(
            "host: " + host + " is remote load,only local load is supported in table model");
      }
    } catch (UnknownHostException e) {
      ioTPrinter.println(
          "Unknown host: " + host + ". Exception: " + e.getMessage() + ". Will use local load.");
    }
    final String os = commandLine.getOptionValue(ON_SUCCESS_ARGS);
    final String onSuccess = StringUtils.isNotBlank(os) ? os.trim().toLowerCase() : null;
    final String of = commandLine.getOptionValue(ON_FAIL_ARGS);
    final String onFail = StringUtils.isNotBlank(of) ? of.trim().toLowerCase() : null;
    if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)
        && (!ImportTsFile.Operation.isValidOperation(onSuccess)
            || !ImportTsFile.Operation.isValidOperation(onFail))) {
      ioTPrinter.println("Args error: os/of must be one of none, mv, cp, delete");
      System.exit(CODE_ERROR);
      boolean isSuccessDirEqualsSourceDir = false;
      if (ImportTsFile.Operation.MV.name().equalsIgnoreCase(onSuccess)
          || ImportTsFile.Operation.CP.name().equalsIgnoreCase(onSuccess)) {
        File dir = createSuccessDir(commandLine);
        isSuccessDirEqualsSourceDir = isFileStoreEquals(targetPath, dir);
      }

      boolean isFailDirEqualsSourceDir = false;
      if (ImportTsFile.Operation.MV.name().equalsIgnoreCase(onFail)
          || ImportTsFile.Operation.CP.name().equalsIgnoreCase(onFail)) {
        File dir = createFailDir(commandLine);
        isFailDirEqualsSourceDir = isFileStoreEquals(targetPath, dir);
      }

      successOperation =
          ImportTsFile.Operation.getOperation(onSuccess, isSuccessDirEqualsSourceDir);
      failOperation = ImportTsFile.Operation.getOperation(onFail, isFailDirEqualsSourceDir);
    }
  }

  public static boolean isFileStoreEquals(String pathString, File dir) {
    try {
      return Objects.equals(
          Files.getFileStore(Paths.get(pathString)), Files.getFileStore(dir.toPath()));
    } catch (IOException e) {
      ioTPrinter.println("IOException when checking file store: " + e.getMessage());
      return false;
    }
  }

  public static File createSuccessDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(SUCCESS_DIR_ARGS) != null) {
      successDir = commandLine.getOptionValue(SUCCESS_DIR_ARGS);
    }
    File file = new File(successDir);
    if (!file.isDirectory()) {
      if (!file.mkdirs()) {
        ioTPrinter.println(String.format("Failed to create %s %s", SUCCESS_DIR_NAME, successDir));
        System.exit(CODE_ERROR);
      }
    }
    return file;
  }

  public static File createFailDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(FAIL_DIR_ARGS) != null) {
      failDir = commandLine.getOptionValue(FAIL_DIR_ARGS);
    }
    File file = new File(failDir);
    if (!file.isDirectory()) {
      if (!file.mkdirs()) {
        ioTPrinter.println(String.format("Failed to create %s %s", FAIL_DIR_NAME, failDir));
        System.exit(CODE_ERROR);
      }
    }
    return file;
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
            || value.equals(DATATYPE_TEXT)
            || value.equals(DATATYPE_STRING))) {
      throw new ArgsErrorException("NaN can not convert to " + value);
    }
    if (key.equals(DATATYPE_BOOLEAN)
        && !(value.equals(DATATYPE_BOOLEAN)
            || value.equals(DATATYPE_TEXT)
            || value.equals(DATATYPE_STRING))) {
      throw new ArgsErrorException("Boolean can not convert to " + value);
    }
    if (key.equals(DATATYPE_DATE)
        && !(value.equals(DATATYPE_DATE)
            || value.equals(DATATYPE_TEXT)
            || value.equals(DATATYPE_STRING))) {
      throw new ArgsErrorException("Date can not convert to " + value);
    }
    if (key.equals(DATATYPE_TIMESTAMP)
        && !(value.equals(DATATYPE_TIMESTAMP)
            || value.equals(DATATYPE_TEXT)
            || value.equals(DATATYPE_STRING)
            || value.equals(DATATYPE_DOUBLE)
            || value.equals(DATATYPE_LONG))) {
      throw new ArgsErrorException("Timestamp can not convert to " + value);
    }
    if (key.equals(DATATYPE_BLOB) && !(value.equals(DATATYPE_BLOB))) {
      throw new ArgsErrorException("Blob can not convert to " + value);
    }
    final TSDataType srcType = TYPE_INFER_VALUE_DICT.get(key);
    final TSDataType dstType = TYPE_INFER_VALUE_DICT.get(value);
    if (dstType.getType() < srcType.getType()) {
      throw new ArgsErrorException(key + " can not convert to " + value);
    }
    TYPE_INFER_KEY_DICT.put(key, TYPE_INFER_VALUE_DICT.get(value));
  }

  private static int importFromTargetPathAsync() {
    try {
      AbstractImportData importData;
      if (sqlDialectTree) {
        importData = new ImportDataTree();
      } else {
        importData = new ImportDataTable();
      }
      importData.init();
      ImportDataScanTool.setSourceFullPath(targetPath);
      final File file = new File(targetPath);
      if (!file.isFile() && !file.isDirectory()) {
        ioTPrinter.println(String.format("Source file or directory %s does not exist", targetPath));
        System.exit(CODE_ERROR);
      }
      AbstractImportData.init(importData);
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
   * can be offered to console cli to implement importing CSV file by command. Available for Cli
   * import
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

  private static void setTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    if (timeZoneID != null) {
      session.setTimeZone(timeZoneID);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  /**
   * import the CSV file and load headers and records. Available for Cli import
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

  /**
   * import the SQL file and load headers and records. Available for Cli import
   *
   * @param session
   * @param file
   */
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
