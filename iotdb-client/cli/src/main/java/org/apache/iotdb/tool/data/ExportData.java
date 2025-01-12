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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.OptionsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.jline.reader.LineReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.tool.common.Constants.*;

/**
 * Export CSV file.
 *
 * @version 1.0.0 20170719
 */
public class ExportData extends AbstractDataTool {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  @SuppressWarnings({
    "squid:S3776",
    "squid:S2093"
  }) // Suppress high Cognitive Complexity warning, ignore try-with-resources
  /* main function of export csv tool. */
  public static void main(String[] args) {
    OptionsUtil.setIsImport(false);
    Options helpOptions = OptionsUtil.createHelpOptions();
    Options tsFileOptions = OptionsUtil.createExportTsFileOptions();
    Options csvOptions = OptionsUtil.createExportCsvOptions();
    Options sqlOptions = OptionsUtil.createExportSqlOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      printHelpOptions(
          EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(helpOptions, args, true);
    } catch (ParseException e) {
      printHelpOptions(
          EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
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
        csvOptions = OptionsUtil.createTableExportCsvOptions();
        tsFileOptions = OptionsUtil.createTableExportTsFileSqlOptions();
        sqlOptions = OptionsUtil.createTableExportTsFileSqlOptions();
      } else {
        ioTPrinter.println(String.format("sql_dialect %s is not support", sqlDialectValue));
        printHelpOptions(
            IMPORT_CLI_HEAD, IMPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
        System.exit(CODE_ERROR);
      }
    }
    int ftIndex = argList.indexOf(MINUS + FILE_TYPE_ARGS);
    if (ftIndex < 0) {
      ftIndex = argList.indexOf(MINUS + FILE_TYPE_NAME);
    }
    if (helpIndex >= 0) {
      fileType = argList.get(helpIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, EXPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, EXPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, EXPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
        }
      } else {
        printHelpOptions(
            EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
      }
      System.exit(CODE_ERROR);
    } else if (ftIndex >= 0) {
      fileType = argList.get(ftIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(tsFileOptions, args, true);
            //            ExportTsFile exportTsFile = new ExportTsFile(commandLine);
            //            exportTsFile.exportTsfile(CODE_OK);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, EXPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(csvOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, EXPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
            System.exit(CODE_ERROR);
          }
        } else if (SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(sqlOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, EXPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
            System.exit(CODE_ERROR);
          }
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
          System.exit(CODE_ERROR);
        }
      } else {
        printHelpOptions(
            EXPORT_CLI_HEAD, EXPORT_CLI_PREFIX, hf, tsFileOptions, csvOptions, sqlOptions, true);
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
      AbstractExportData exportData;
      // check timePrecision by session
      exportData = new ExportDataTree();
      exportData.init();
      if (!sqlDialectTree) {
        exportData = new ExportDataTable();
        exportData.init();
      }
      if (sqlDialectTree && queryCommand == null) {
        LineReader lineReader =
            JlineUtils.getLineReader(
                new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
                username,
                host,
                port);
        String sql = lineReader.readLine(EXPORT_CLI_PREFIX + "> please input query: ");
        ioTPrinter.println(sql);
        String[] values = sql.trim().split(";");
        for (int i = 0; i < values.length; i++) {
          exportData.exportBySql(values[i], i);
        }
      } else {
        exportData.exportBySql(queryCommand, 0);
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
    }
    System.exit(exitCode);
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
    if (!file.isDirectory() && !file.mkdirs()) {
      ioTPrinter.println(String.format("Failed to create directories %s", targetDirectory));
      System.exit(CODE_ERROR);
    }
    if (commandLine.getOptionValue(LINES_PER_FILE_ARGS) != null) {
      linesPerFile = Integer.parseInt(commandLine.getOptionValue(LINES_PER_FILE_ARGS));
    }
    if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
    }
    if (commandLine.getOptionValue(DB_ARGS) != null) {
      database = commandLine.getOptionValue(DB_ARGS).toLowerCase();
    }
    if (commandLine.getOptionValue(TABLE_ARGS) != null) {
      table = commandLine.getOptionValue(TABLE_ARGS).toLowerCase();
    } else {
      if (!sqlDialectTree && StringUtils.isBlank(queryCommand)) {
        ioTPrinter.println(queryTableParamRequired);
        System.exit(CODE_ERROR);
      }
    }
    if (commandLine.getOptionValue(START_TIME_ARGS) != null) {
      startTime = commandLine.getOptionValue(START_TIME_ARGS);
    }
    if (commandLine.getOptionValue(END_TIME_ARGS) != null) {
      endTime = commandLine.getOptionValue(END_TIME_ARGS);
    }
  }
}
