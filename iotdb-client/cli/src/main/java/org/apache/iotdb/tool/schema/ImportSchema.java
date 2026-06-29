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

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.OptionsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.tsfile.external.commons.lang3.StringUtils;

import java.io.File;

/** Import Schema CSV file. */
public class ImportSchema extends AbstractSchemaTool {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  /**
   * parse optional params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetPath = commandLine.getOptionValue(Constants.FILE_ARGS);
    if (commandLine.getOptionValue(Constants.BATCH_POINT_SIZE_ARGS) != null) {
      batchPointSize =
          Integer.parseInt(commandLine.getOptionValue(Constants.BATCH_POINT_SIZE_ARGS));
    }
    if (commandLine.getOptionValue(Constants.FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(Constants.FAILED_FILE_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      } else if (!failedFileDirectory.endsWith("/") && !failedFileDirectory.endsWith("\\")) {
        failedFileDirectory += File.separator;
      }
    }
    if (commandLine.getOptionValue(Constants.ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(Constants.ALIGNED_ARGS));
    }
    if (commandLine.getOptionValue(Constants.LINES_PER_FAILED_FILE_ARGS) != null) {
      linesPerFailedFile =
          Integer.parseInt(commandLine.getOptionValue(Constants.LINES_PER_FAILED_FILE_ARGS));
    }
    database = checkRequiredArg(Constants.DB_ARGS, Constants.DB_NAME, commandLine, null);
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

  public static void main(String[] args) throws IoTDBConnectionException {
    Options options = OptionsUtil.createImportSchemaOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(Constants.MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      ioTPrinter.println(Constants.SCHEMA_CLI_CHECK_IN_HEAD);
      hf.printHelp(Constants.IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      ioTPrinter.println("Parse error: " + e.getMessage());
      hf.printHelp(Constants.IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    if (commandLine.hasOption(Constants.HELP_ARGS)) {
      hf.printHelp(Constants.IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(Constants.FILE_ARGS);
      if (filename == null) {
        hf.printHelp(Constants.IMPORT_SCHEMA_CLI_PREFIX, options, true);
        System.exit(Constants.CODE_ERROR);
      }
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Args error: " + e.getMessage());
      System.exit(Constants.CODE_ERROR);
    } catch (Exception e) {
      ioTPrinter.println("Encounter an error, because: " + e.getMessage());
      System.exit(Constants.CODE_ERROR);
    }
    System.exit(importFromTargetPath());
  }

  /**
   * Specifying a CSV file or a directory including CSV files that you want to import. This method
   * can be offered to console cli to implement importing CSV file by command.
   *
   * @return the status code
   * @throws IoTDBConnectionException
   */
  @SuppressWarnings({"squid:S2093"}) // ignore try-with-resources
  private static int importFromTargetPath() {
    AbstractImportSchema importSchema;
    try {
      if (sqlDialectTree) {
        importSchema = new ImportSchemaTree();
      } else {
        importSchema = new ImportSchemaTable();
      }
      importSchema.init();
      AbstractImportSchema.init(importSchema);
      return Constants.CODE_OK;
    } catch (InterruptedException e) {
      ioTPrinter.println(String.format("Import schema fail: %s", e.getMessage()));
      Thread.currentThread().interrupt();
      return Constants.CODE_ERROR;
    } catch (Exception e) {
      ioTPrinter.println(String.format("Import schema fail: %s", e.getMessage()));
      return Constants.CODE_ERROR;
    }
  }
}
