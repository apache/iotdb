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

package org.apache.iotdb.tool.schema;

import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.OptionsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;

/** Export Schema CSV file. */
public class ExportSchema extends AbstractSchemaTool {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  @SuppressWarnings({
    "squid:S3776",
    "squid:S2093"
  }) // Suppress high Cognitive Complexity warning, ignore try-with-resources
  /* main function of export csv tool. */
  public static void main(String[] args) {
    Options options = OptionsUtil.createExportSchemaOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(org.apache.iotdb.tool.common.Constants.MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      ioTPrinter.println(Constants.SCHEMA_CLI_CHECK_IN_HEAD);
      hf.printHelp(Constants.EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      ioTPrinter.println(e.getMessage());
      hf.printHelp(Constants.EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    if (commandLine.hasOption(Constants.HELP_ARGS)) {
      hf.printHelp(Constants.EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Args args: " + e.getMessage());
      ioTPrinter.println("Use -help for more information");
      System.exit(Constants.CODE_ERROR);
    } catch (Exception e) {
      ioTPrinter.println("Encounter an error, because " + e.getMessage());
      System.exit(Constants.CODE_ERROR);
    }
    System.exit(exportToTargetPath());
  }

  private static int exportToTargetPath() {
    AbstractExportSchema exportSchema;
    try {
      if (sqlDialectTree) {
        exportSchema = new ExportSchemaTree();
        exportSchema.init();
        if (sqlDialectTree && queryPath == null) {
          LineReader lineReader =
              JlineUtils.getLineReader(
                  new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
                  username,
                  host,
                  port);
          String pathPattern =
              lineReader.readLine(Constants.EXPORT_CLI_PREFIX + "> please input path pattern: ");
          ioTPrinter.println(pathPattern);
          String[] values = pathPattern.trim().split(";");
          for (int i = 0; i < values.length; i++) {
            if (StringUtils.isBlank(values[i])) {
              continue;
            } else {
              exportSchema.exportSchemaToCsvFile(values[i], i);
            }
          }
        } else {
          exportSchema.exportSchemaToCsvFile(queryPath, 0);
        }
      } else {
        exportSchema = new ExportSchemaTable();
        exportSchema.init();
        exportSchema.exportSchemaToSqlFile();
      }
      ioTPrinter.println(Constants.EXPORT_COMPLETELY);
      return Constants.CODE_OK;
    } catch (InterruptedException e) {
      ioTPrinter.println(String.format("Export schema fail: %s", e.getMessage()));
      Thread.currentThread().interrupt();
      return Constants.CODE_ERROR;
    } catch (Exception e) {
      ioTPrinter.println(String.format("Export schema fail: %s", e.getMessage()));
      return Constants.CODE_ERROR;
    }
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory =
        checkRequiredArg(Constants.TARGET_DIR_ARGS, Constants.TARGET_DIR_NAME, commandLine, null);
    queryPath = commandLine.getOptionValue(Constants.TARGET_PATH_ARGS);
    targetFile = commandLine.getOptionValue(Constants.TARGET_FILE_ARGS);
    targetFile =
        checkRequiredArg(
            Constants.TARGET_FILE_ARGS,
            Constants.TARGET_FILE_NAME,
            commandLine,
            Constants.DUMP_FILE_NAME_DEFAULT);
    String timeoutString = commandLine.getOptionValue(Constants.TIMEOUT_ARGS);
    if (timeoutString != null) {
      timeout = Long.parseLong(timeoutString);
    }
    if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
      targetDirectory += File.separator;
    }
    if (commandLine.getOptionValue(Constants.LINES_PER_FILE_ARGS) != null) {
      linesPerFile = Integer.parseInt(commandLine.getOptionValue(Constants.LINES_PER_FILE_ARGS));
    }
    database = commandLine.getOptionValue(Constants.DB_ARGS);
    table = commandLine.getOptionValue(Constants.TABLE_ARGS);
    String sqlDialectValue =
        checkRequiredArg(
            Constants.SQL_DIALECT_ARGS,
            Constants.SQL_DIALECT_ARGS,
            commandLine,
            Constants.SQL_DIALECT_VALUE_TREE);
    if (Constants.SQL_DIALECT_VALUE_TABLE.equalsIgnoreCase(sqlDialectValue)) {
      sqlDialectTree = false;
      if (StringUtils.isBlank(database)) {
        ioTPrinter.println(
            String.format("The database param is required when sql_dialect is table "));
        System.exit(Constants.CODE_ERROR);
      } else if (StringUtils.isNotBlank(database)
          && "information_schema".equalsIgnoreCase(database)) {
        ioTPrinter.println(
            String.format("Does not support exporting system databases %s", database));
        System.exit(Constants.CODE_ERROR);
      }
    }
  }
}
