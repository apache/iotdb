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
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.OptionsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
    hf.setWidth(Constants.MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      printHelpOptions(
          Constants.EXPORT_CLI_HEAD,
          Constants.EXPORT_CLI_PREFIX,
          hf,
          tsFileOptions,
          csvOptions,
          sqlOptions,
          true);
      System.exit(Constants.CODE_ERROR);
    }
    try {
      commandLine = parser.parse(helpOptions, args, true);
    } catch (ParseException e) {
      printHelpOptions(
          Constants.EXPORT_CLI_HEAD,
          Constants.EXPORT_CLI_PREFIX,
          hf,
          tsFileOptions,
          csvOptions,
          sqlOptions,
          true);
      System.exit(Constants.CODE_ERROR);
    }
    final List<String> argList = Arrays.asList(args);
    int helpIndex = argList.indexOf(Constants.MINUS + Constants.HELP_ARGS);
    int sql_dialect = argList.indexOf(Constants.MINUS + Constants.SQL_DIALECT_ARGS); // -sql_dialect
    if (sql_dialect >= 0
        && !Constants.SQL_DIALECT_VALUE_TREE.equalsIgnoreCase(argList.get(sql_dialect + 1))) {
      final String sqlDialectValue = argList.get(sql_dialect + 1);
      if (Constants.SQL_DIALECT_VALUE_TABLE.equalsIgnoreCase(sqlDialectValue)) {
        sqlDialectTree = false;
        csvOptions = OptionsUtil.createTableExportCsvOptions();
        tsFileOptions = OptionsUtil.createTableExportTsFileOptions();
        sqlOptions = OptionsUtil.createTableExportSqlOptions();
      } else {
        ioTPrinter.println(String.format("sql_dialect %s is not support", sqlDialectValue));
        printHelpOptions(
            Constants.IMPORT_CLI_HEAD,
            Constants.IMPORT_CLI_PREFIX,
            hf,
            tsFileOptions,
            csvOptions,
            sqlOptions,
            true);
        System.exit(Constants.CODE_ERROR);
      }
    }
    int ftIndex = argList.indexOf(Constants.MINUS + Constants.FILE_TYPE_ARGS);
    if (ftIndex < 0) {
      ftIndex = argList.indexOf(Constants.MINUS + Constants.FILE_TYPE_NAME);
    }
    if (helpIndex >= 0) {
      fileType = argList.get(helpIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, Constants.EXPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
        } else if (Constants.CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, Constants.EXPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
        } else if (Constants.SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          printHelpOptions(null, Constants.EXPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              Constants.EXPORT_CLI_HEAD,
              Constants.EXPORT_CLI_PREFIX,
              hf,
              tsFileOptions,
              csvOptions,
              sqlOptions,
              true);
        }
      } else {
        printHelpOptions(
            Constants.EXPORT_CLI_HEAD,
            Constants.EXPORT_CLI_PREFIX,
            hf,
            tsFileOptions,
            csvOptions,
            sqlOptions,
            true);
      }
      System.exit(Constants.CODE_ERROR);
    } else if (ftIndex >= 0) {
      fileType = argList.get(ftIndex + 1);
      if (StringUtils.isNotBlank(fileType)) {
        if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(tsFileOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(
                null, Constants.EXPORT_CLI_PREFIX, hf, tsFileOptions, null, null, false);
            System.exit(Constants.CODE_ERROR);
          }
        } else if (Constants.CSV_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(csvOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, Constants.EXPORT_CLI_PREFIX, hf, null, csvOptions, null, false);
            System.exit(Constants.CODE_ERROR);
          }
        } else if (Constants.SQL_SUFFIXS.equalsIgnoreCase(fileType)) {
          try {
            commandLine = parser.parse(sqlOptions, args, true);
          } catch (ParseException e) {
            ioTPrinter.println("Parse error: " + e.getMessage());
            printHelpOptions(null, Constants.EXPORT_CLI_PREFIX, hf, null, null, sqlOptions, false);
            System.exit(Constants.CODE_ERROR);
          }
        } else {
          ioTPrinter.println(String.format("File type %s is not support", fileType));
          printHelpOptions(
              Constants.EXPORT_CLI_HEAD,
              Constants.EXPORT_CLI_PREFIX,
              hf,
              tsFileOptions,
              csvOptions,
              sqlOptions,
              true);
          System.exit(Constants.CODE_ERROR);
        }
      } else {
        printHelpOptions(
            Constants.EXPORT_CLI_HEAD,
            Constants.EXPORT_CLI_PREFIX,
            hf,
            tsFileOptions,
            csvOptions,
            sqlOptions,
            true);
        System.exit(Constants.CODE_ERROR);
      }
    } else {
      ioTPrinter.println(
          String.format(Constants.REQUIRED_ARGS_ERROR_MSG, Constants.FILE_TYPE_NAME));
      System.exit(Constants.CODE_ERROR);
    }
    int exitCode = Constants.CODE_OK;
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
      if (!checkTimeFormat()) {
        System.exit(Constants.CODE_ERROR);
      }
      AbstractExportData exportData;
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
        String sql = lineReader.readLine(Constants.EXPORT_CLI_PREFIX + "> please input query: ");
        ioTPrinter.println(sql);
        String[] values = sql.trim().split(";");
        for (int i = 0; i < values.length; i++) {
          exportData.exportBySql(values[i], i);
        }
      } else {
        String[] values = queryCommand.trim().split(";");
        for (int i = 0; i < values.length; i++) {
          exportData.exportBySql(values[i], i);
        }
      }
    } catch (IOException e) {
      ioTPrinter.println("Failed to operate on file, because " + e.getMessage());
      exitCode = Constants.CODE_ERROR;
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Invalid args: " + e.getMessage());
      exitCode = Constants.CODE_ERROR;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      ioTPrinter.println("Connect failed because " + e.getMessage());
      exitCode = Constants.CODE_ERROR;
    } catch (TException e) {
      ioTPrinter.println(
          "Can not get the timestamp precision from server because " + e.getMessage());
      exitCode = Constants.CODE_ERROR;
    }
    System.exit(exitCode);
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory =
        checkRequiredArg(Constants.TARGET_DIR_ARGS, Constants.TARGET_DIR_NAME, commandLine, null);
    targetFile = commandLine.getOptionValue(Constants.TARGET_FILE_ARGS);
    needDataTypePrinted = Boolean.valueOf(commandLine.getOptionValue(Constants.DATA_TYPE_ARGS));
    queryCommand = commandLine.getOptionValue(Constants.QUERY_COMMAND_ARGS);
    exportType = commandLine.getOptionValue(Constants.FILE_TYPE_ARGS);
    String timeoutString = commandLine.getOptionValue(Constants.TIMEOUT_ARGS);
    if (timeoutString != null) {
      timeout = Long.parseLong(timeoutString);
    }
    String rpcMaxFrameSizeString = commandLine.getOptionValue(Constants.RPC_MAX_FRAME_SIZE_ARGS);
    if (rpcMaxFrameSizeString != null) {
      rpcMaxFrameSize = Integer.parseInt(rpcMaxFrameSizeString);
    }
    if (needDataTypePrinted == null) {
      needDataTypePrinted = true;
    }
    if (targetFile == null) {
      targetFile = Constants.DUMP_FILE_NAME_DEFAULT;
    }
    timeFormat = commandLine.getOptionValue(Constants.TIME_FORMAT_ARGS);
    if (timeFormat == null) {
      timeFormat = "default";
    }
    timeZoneID = commandLine.getOptionValue(Constants.TIME_ZONE_ARGS);
    if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
      targetDirectory += File.separator;
    }
    final File file = new File(targetDirectory);
    if (!file.isDirectory() && !file.mkdirs()) {
      ioTPrinter.println(String.format("Failed to create directories %s", targetDirectory));
      System.exit(Constants.CODE_ERROR);
    }
    if (commandLine.getOptionValue(Constants.LINES_PER_FILE_ARGS) != null) {
      linesPerFile = Integer.parseInt(commandLine.getOptionValue(Constants.LINES_PER_FILE_ARGS));
    }
    if (commandLine.getOptionValue(Constants.ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(Constants.ALIGNED_ARGS));
    }
    if (commandLine.getOptionValue(Constants.DB_ARGS) != null) {
      database = commandLine.getOptionValue(Constants.DB_ARGS).toLowerCase();
      if (ObjectUtils.isNotEmpty(database) && "information_schema".equalsIgnoreCase(database)) {
        ioTPrinter.println(
            String.format("Does not support exporting system databases %s", database));
        System.exit(Constants.CODE_ERROR);
      }
    }
    if (commandLine.getOptionValue(Constants.TABLE_ARGS) != null) {
      table = commandLine.getOptionValue(Constants.TABLE_ARGS).toLowerCase();
    }
    if (commandLine.getOptionValue(Constants.START_TIME_ARGS) != null) {
      startTime = commandLine.getOptionValue(Constants.START_TIME_ARGS);
    }
    if (commandLine.getOptionValue(Constants.END_TIME_ARGS) != null) {
      endTime = commandLine.getOptionValue(Constants.END_TIME_ARGS);
    }
  }
}
