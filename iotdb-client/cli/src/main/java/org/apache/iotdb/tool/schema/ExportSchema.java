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
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

/** Export Schema CSV file. */
public class ExportSchema extends AbstractSchemaTool {

  private static final String TARGET_DIR_ARGS = "t";
  private static final String TARGET_DIR_ARGS_NAME = "target";
  private static final String TARGET_DIR_NAME = "targetDir";

  private static final String TARGET_PATH_ARGS = "path";
  private static final String TARGET_PATH_ARGS_NAME = "path_pattern";
  private static final String TARGET_PATH_NAME = "exportPathPattern";
  private static String queryPath;

  private static final String TARGET_FILE_ARGS = "pf";
  private static final String TARGET_FILE_ARGS_NAME = "path_pattern_file";
  private static final String TARGET_FILE_NAME = "exportPathPatternFile";

  private static final String LINES_PER_FILE_ARGS = "lpf";
  private static final String LINES_PER_FILE_ARGS_NAME = "lines_per_file";
  private static final String LINES_PER_FILE_NAME = "linesPerFile";
  private static int linesPerFile = 10000;

  private static final String EXPORT_SCHEMA_CLI_PREFIX = "ExportSchema";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;

  private static String targetDirectory;

  private static long timeout = 60000;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String BASE_VIEW_TYPE = "BASE";
  private static final String HEADER_VIEW_TYPE = "ViewType";
  private static final String HEADER_TIMESERIES = "Timeseries";

  @SuppressWarnings({
    "squid:S3776",
    "squid:S2093"
  }) // Suppress high Cognitive Complexity warning, ignore try-with-resources
  /* main function of export csv tool. */
  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      ioTPrinter.println("Too few params input, please check the following hint.");
      hf.printHelp(EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      ioTPrinter.println(e.getMessage());
      hf.printHelp(EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(EXPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    int exitCode = CODE_OK;
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);
      if (queryPath == null) {
        String pathFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
        String path;
        if (pathFile == null) {
          LineReader lineReader =
              JlineUtils.getLineReader(
                  new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
                  username,
                  host,
                  port);
          path = lineReader.readLine(EXPORT_SCHEMA_CLI_PREFIX + "> please input path pattern: ");
          ioTPrinter.println(path);
          String[] values = path.trim().split(";");
          for (int i = 0; i < values.length; i++) {
            if (StringUtils.isBlank(values[i])) {
              continue;
            } else {
              dumpResult(values[i], i);
            }
          }
        } else if (!pathFile.endsWith(".txt")) {
          ioTPrinter.println("The file name must end with \"txt\"!");
          hf.printHelp(EXPORT_SCHEMA_CLI_PREFIX, options, true);
          System.exit(CODE_ERROR);
        } else {
          dumpFromPathFile(pathFile);
        }
      } else {
        dumpResult(queryPath, 0);
      }
    } catch (IOException e) {
      ioTPrinter.println("Failed to operate on file, because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (IoTDBConnectionException e) {
      ioTPrinter.println("Connect failed because " + e.getMessage());
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

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_ARGS_NAME, commandLine, null);
    queryPath = commandLine.getOptionValue(TARGET_PATH_ARGS);
    String timeoutString = commandLine.getOptionValue(TIMEOUT_ARGS);
    if (timeoutString != null) {
      timeout = Long.parseLong(timeoutString);
    }
    if (targetFile == null) {
      targetFile = DUMP_FILE_NAME_DEFAULT;
    }
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
            .longOpt(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .argName(TARGET_DIR_NAME)
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opTargetFile);

    Option targetPathPattern =
        Option.builder(TARGET_PATH_ARGS)
            .longOpt(TARGET_PATH_ARGS_NAME)
            .hasArg()
            .argName(TARGET_PATH_NAME)
            .desc("Export Path Pattern (optional)")
            .build();
    options.addOption(targetPathPattern);

    Option targetFileName =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_ARGS_NAME)
            .hasArg()
            .argName(TARGET_FILE_NAME)
            .desc("Export File Name (optional)")
            .build();
    options.addOption(targetFileName);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_ARGS_NAME)
            .hasArg()
            .argName(LINES_PER_FILE_NAME)
            .desc("Lines per dump file.")
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeout =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_ARGS_NAME)
            .hasArg()
            .argName(TIMEOUT_ARGS)
            .desc(timeout + " Timeout for session query")
            .build();
    options.addOption(opTimeout);

    Option opHelp =
        Option.builder(HELP_ARGS).longOpt(HELP_ARGS).desc("Display help information").build();
    options.addOption(opHelp);
    return options;
  }

  /**
   * This method will be called, if the query commands are written in a sql file.
   *
   * @param pathFile sql file path
   * @throws IOException exception
   */
  private static void dumpFromPathFile(String pathFile) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(pathFile))) {
      String path;
      int index = 0;
      while ((path = reader.readLine()) != null) {
        dumpResult(path, index);
        index++;
      }
    }
  }

  /**
   * Dump files from database to CSV file.
   *
   * @param pattern used to be export schema
   * @param index used to create dump file name
   */
  private static void dumpResult(String pattern, int index) {
    File file = new File(targetDirectory);
    if (!file.isDirectory()) {
      file.mkdir();
    }
    final String path = targetDirectory + targetFile + index;
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("show timeseries " + pattern, timeout);
      writeCsvFile(sessionDataSet, path, sessionDataSet.getColumnNames(), linesPerFile);
      sessionDataSet.closeOperationHandle();
      ioTPrinter.println("Export completely!");
    } catch (StatementExecutionException | IoTDBConnectionException | IOException e) {
      ioTPrinter.println("Cannot dump result because: " + e.getMessage());
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void writeCsvFile(
      SessionDataSet sessionDataSet, String filePath, List<String> headers, int linesPerFile)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    int viewTypeIndex = headers.indexOf(HEADER_VIEW_TYPE);
    int timeseriesIndex = headers.indexOf(HEADER_TIMESERIES);

    int fileIndex = 0;
    boolean hasNext = true;
    while (hasNext) {
      int i = 0;
      final String finalFilePath = filePath + "_" + fileIndex + ".csv";
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
      while (i++ < linesPerFile) {
        if (sessionDataSet.hasNext()) {
          if (i == 1) {
            csvPrinterWrapper.printRecord(HEAD_COLUMNS);
          }
          RowRecord rowRecord = sessionDataSet.next();
          List<Field> fields = rowRecord.getFields();
          if (fields.get(timeseriesIndex).getStringValue().startsWith(SYSTEM_DATABASE + ".")
              || !fields.get(viewTypeIndex).getStringValue().equals(BASE_VIEW_TYPE)) {
            continue;
          }
          HEAD_COLUMNS.forEach(
              column -> {
                Field field = fields.get(headers.indexOf(column));
                String fieldStringValue = field.getStringValue();
                if (!"null".equals(field.getStringValue())) {
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
