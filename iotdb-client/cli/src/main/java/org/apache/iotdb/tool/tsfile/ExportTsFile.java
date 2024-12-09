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

package org.apache.iotdb.tool.tsfile;

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
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExportTsFile extends AbstractTsFileTool {

  private static final String TARGET_DIR_ARGS = "t";
  private static final String TARGET_DIR_NAME = "targetDirectory";
  private static final String TARGET_DIR_NAME_BACK = "target";

  private static final String TARGET_FILE_ARGS = "tfn";
  private static final String TARGET_FILE_NAME = "targetFileName";
  private static final String TARGET_FILE_ARGS_BACK = "pfn";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sourceSqlFile";
  private static final String QUERY_COMMAND_ARGS = "q";
  private static final String QUERY_COMMAND_NAME = "queryCommand";
  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static final String TSFILEDB_CLI_PREFIX = "ExportTsFile";

  private static String targetDirectory;
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;
  private static String queryCommand;
  private static String sqlFile;

  private static long timeout = -1;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  @SuppressWarnings({
    "squid:S3776",
    "squid:S2093"
  }) // Suppress high Cognitive Complexity warning, ignore try-with-resources
  /* main function of export tsFile tool. */
  public static void main(String[] args) {
    int exitCode = getCommandLine(args);
    exportTsfile(exitCode);
  }

  public static void exportTsfile(int exitCode) {
    try {
      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);

      if (queryCommand == null) {
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
            legalCheck(values[i]);
            dumpResult(values[i], i);
          }

        } else {
          dumpFromSqlFile(sqlFile);
        }
      } else {
        legalCheck(queryCommand);
        dumpResult(queryCommand, 0);
      }

    } catch (IOException e) {
      ioTPrinter.println("Failed to operate on file, because " + e.getMessage());
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

  public ExportTsFile(CommandLine commandLine) {
    try {
      parseBasicParams(commandLine);
      parseSpecialParamsBack(commandLine);
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Invalid args: " + e.getMessage());
      System.exit(CODE_ERROR);
    }
  }

  protected static int getCommandLine(String[] args) {
    createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      ioTPrinter.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      ioTPrinter.println(e.getMessage());
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
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    }
    return exitCode;
  }

  private static void legalCheck(String sql) {
    String sqlLower = sql.toLowerCase();
    if (sqlLower.contains("count(")
        || sqlLower.contains("sum(")
        || sqlLower.contains("avg(")
        || sqlLower.contains("extreme(")
        || sqlLower.contains("max_value(")
        || sqlLower.contains("min_value(")
        || sqlLower.contains("first_value(")
        || sqlLower.contains("last_value(")
        || sqlLower.contains("max_time(")
        || sqlLower.contains("min_time(")
        || sqlLower.contains("stddev(")
        || sqlLower.contains("stddev_pop(")
        || sqlLower.contains("stddev_samp(")
        || sqlLower.contains("variance(")
        || sqlLower.contains("var_pop(")
        || sqlLower.contains("var_samp(")
        || sqlLower.contains("max_by(")
        || sqlLower.contains("min_by(")) {
      ioTPrinter.println("The sql you entered is invalid, please don't use aggregate query.");
      System.exit(CODE_ERROR);
    }
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
    sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
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
  }

  private static void parseSpecialParamsBack(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME_BACK, commandLine);
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS_BACK);
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
    final File file = new File(targetDirectory);
    if (!file.isDirectory()) {
      ioTPrinter.println(
          String.format("Source file or directory %s does not exist", targetDirectory));
      System.exit(CODE_ERROR);
    }
  }

  /**
   * commandline option create.
   *
   * @return object Options
   */
  private static void createOptions() {
    createBaseOptions();

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

    Option opTimeout =
        Option.builder(TIMEOUT_ARGS)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc("Timeout for session query")
            .build();
    options.addOption(opTimeout);
  }

  /**
   * This method will be called, if the query commands are written in a sql file.
   *
   * @param filePath:file path
   * @throws IOException: exception
   */
  private static void dumpFromSqlFile(String filePath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String sql;
      int i = 0;
      while ((sql = reader.readLine()) != null) {
        legalCheck(sql);
        dumpResult(sql, i++);
      }
    }
  }

  /**
   * Dump files from database to tsFile.
   *
   * @param sql export the result of executing the sql
   */
  private static void dumpResult(String sql, int index) {
    final String path = targetDirectory + targetFile + index + ".tsfile";
    try (SessionDataSet sessionDataSet = session.executeQueryStatement(sql, timeout)) {
      long start = System.currentTimeMillis();
      writeWithTablets(sessionDataSet, path);
      long end = System.currentTimeMillis();
      ioTPrinter.println("Export completely!cost: " + (end - start) + " ms.");
    } catch (StatementExecutionException
        | IoTDBConnectionException
        | IOException
        | WriteProcessException e) {
      ioTPrinter.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static void collectSchemas(
      List<String> columnNames,
      List<String> columnTypes,
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < columnNames.size(); i++) {
      String column = columnNames.get(i);
      if (!column.startsWith("root.")) {
        continue;
      }
      TSDataType tsDataType = getTsDataType(columnTypes.get(i));
      Path path = new Path(column, true);
      String deviceId = path.getDeviceString();
      // query whether the device is aligned or not
      try (SessionDataSet deviceDataSet =
          session.executeQueryStatement("show devices " + deviceId, timeout)) {
        List<Field> deviceList = deviceDataSet.next().getFields();
        if (deviceList.size() > 1 && "true".equals(deviceList.get(1).getStringValue())) {
          alignedDevices.add(deviceId);
        }
      }

      // query timeseries metadata
      MeasurementSchema measurementSchema =
          new MeasurementSchema(path.getMeasurement(), tsDataType);
      List<Field> seriesList =
          session.executeQueryStatement("show timeseries " + column, timeout).next().getFields();
      measurementSchema.setEncoding(TSEncoding.valueOf(seriesList.get(4).getStringValue()));
      measurementSchema.setCompressionType(
          CompressionType.valueOf(seriesList.get(5).getStringValue()));

      deviceSchemaMap.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(measurementSchema);
      deviceColumnIndices.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(i);
    }
  }

  private static List<Tablet> constructTablets(
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter)
      throws WriteProcessException {
    List<Tablet> tabletList = new ArrayList<>(deviceSchemaMap.size());
    for (Map.Entry<String, List<IMeasurementSchema>> stringListEntry : deviceSchemaMap.entrySet()) {
      String deviceId = stringListEntry.getKey();
      List<IMeasurementSchema> schemaList = stringListEntry.getValue();
      Tablet tablet = new Tablet(deviceId, schemaList);
      tablet.initBitMaps();
      Path path = new Path(tablet.getDeviceId());
      if (alignedDevices.contains(tablet.getDeviceId())) {
        tsFileWriter.registerAlignedTimeseries(path, schemaList);
      } else {
        tsFileWriter.registerTimeseries(path, schemaList);
      }
      tabletList.add(tablet);
    }
    return tabletList;
  }

  private static void writeWithTablets(
      SessionDataSet sessionDataSet,
      List<Tablet> tabletList,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    while (sessionDataSet.hasNext()) {
      RowRecord rowRecord = sessionDataSet.next();
      List<Field> fields = rowRecord.getFields();

      for (Tablet tablet : tabletList) {
        String deviceId = tablet.getDeviceId();
        List<Integer> columnIndices = deviceColumnIndices.get(deviceId);
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
        List<IMeasurementSchema> schemas = tablet.getSchemas();

        for (int i = 0, columnIndicesSize = columnIndices.size(); i < columnIndicesSize; i++) {
          Integer columnIndex = columnIndices.get(i);
          IMeasurementSchema measurementSchema = schemas.get(i);
          // -1 for time not in fields
          Object value = fields.get(columnIndex - 1).getObjectValue(measurementSchema.getType());
          if (value == null) {
            tablet.bitMaps[i].mark(rowIndex);
          }
          tablet.addValue(measurementSchema.getMeasurementName(), rowIndex, value);
        }

        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writeToTsFile(alignedDevices, tsFileWriter, tablet);
          tablet.initBitMaps();
          tablet.reset();
        }
      }
    }

    for (Tablet tablet : tabletList) {
      if (tablet.getRowSize() != 0) {
        writeToTsFile(alignedDevices, tsFileWriter, tablet);
      }
    }
  }

  @SuppressWarnings({
    "squid:S3776",
    "squid:S6541"
  }) // Suppress high Cognitive Complexity warning, Suppress many task in one method warning
  public static void writeWithTablets(SessionDataSet sessionDataSet, String filePath)
      throws IOException,
          IoTDBConnectionException,
          StatementExecutionException,
          WriteProcessException {
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<String> columnTypes = sessionDataSet.getColumnTypes();
    File f = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (f.exists()) {
      Files.delete(f.toPath());
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      // device -> column indices in columnNames
      Map<String, List<Integer>> deviceColumnIndices = new HashMap<>();
      Set<String> alignedDevices = new HashSet<>();
      Map<String, List<IMeasurementSchema>> deviceSchemaMap = new LinkedHashMap<>();

      collectSchemas(
          columnNames, columnTypes, deviceSchemaMap, alignedDevices, deviceColumnIndices);

      List<Tablet> tabletList = constructTablets(deviceSchemaMap, alignedDevices, tsFileWriter);

      if (tabletList.isEmpty()) {
        ioTPrinter.println("!!!Warning:Tablet is empty,no data can be exported.");
        System.exit(CODE_ERROR);
      }

      writeWithTablets(
          sessionDataSet, tabletList, alignedDevices, tsFileWriter, deviceColumnIndices);

      tsFileWriter.flush();
    }
  }

  private static void writeToTsFile(
      Set<String> deviceFilterSet, TsFileWriter tsFileWriter, Tablet tablet)
      throws IOException, WriteProcessException {
    if (deviceFilterSet.contains(tablet.getDeviceId())) {
      tsFileWriter.writeAligned(tablet);
    } else {
      tsFileWriter.writeTree(tablet);
    }
  }

  private static TSDataType getTsDataType(String type) {
    return TSDataType.valueOf(type);
  }
}
