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
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Export tsFile.
 *
 * @version 1.0.0 20220816
 */
public class ExportTsFile extends AbstractTsFileTool {

  private static final String TARGET_DIR_ARGS = "td";
  private static final String TARGET_DIR_NAME = "targetDirectory";
  private static final String TARGET_FILE_ARGS = "f";
  private static final String TARGET_FILE_NAME = "targetFile";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sqlfile";
  private static final String QUERY_COMMAND_ARGS = "q";
  private static final String QUERY_COMMAND_NAME = "queryCommand";
  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static final String TSFILEDB_CLI_PREFIX = "ExportTsFile";
  private static String targetDirectory;
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;
  private static String queryCommand;

  /** main function of export tsFile tool. */
  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
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

      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);

      if (queryCommand == null) {
        String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
        String sql;

        if (sqlFile == null) {
          LineReader lineReader = JlineUtils.getLineReader(username, host, port);
          sql = lineReader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
          System.out.println(sql);
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
      System.out.println("Failed to operate on file, because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (IoTDBConnectionException e) {
      System.out.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          exitCode = CODE_ERROR;
          System.out.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
    System.exit(exitCode);
  }

  private static void legalCheck(String sql) {
    if (!sql.contains("select *")) {
      System.out.println(
          "The sql you entered is invalid, please enter the sql beginning with 'select *'");
      System.exit(CODE_ERROR);
    }
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);

    if (targetFile == null) {
      targetFile = DUMP_FILE_NAME_DEFAULT;
    }

    if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
      targetDirectory += File.separator;
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

    return options;
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
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 10000);
      writeTsFileFile(sessionDataSet, path);
      sessionDataSet.closeOperationHandle();
      System.out.println("Export completely!");
    } catch (StatementExecutionException
        | IoTDBConnectionException
        | IOException
        | WriteProcessException e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  public static void writeTsFileFile(SessionDataSet sessionDataSet, String filePath)
      throws IOException, IoTDBConnectionException, StatementExecutionException,
          WriteProcessException {
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<String> columnTypes = sessionDataSet.getColumnTypes();
    File f = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (f.exists()) {
      f.delete();
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      Map<String, Tablet> tabletMap = new LinkedHashMap<>();
      for (int i = 0; i < columnNames.size(); i++) {
        String column = columnNames.get(i);
        int lastIndex = column.lastIndexOf(".");
        if (lastIndex == -1) {
          continue;
        }
        TSDataType tsDataType = getTsDataType(columnTypes.get(i));
        String sensor = column.substring(lastIndex + 1);
        MeasurementSchema measurementSchema = new MeasurementSchema(sensor, tsDataType);

        String deviceId = column.substring(0, lastIndex);
        List<MeasurementSchema> schemaList;
        if (tabletMap.containsKey(deviceId)) {
          schemaList = tabletMap.get(deviceId).getSchemas();
          schemaList.add(measurementSchema);
        } else {
          schemaList = new ArrayList<>();
          schemaList.add(measurementSchema);
        }
        Tablet tablet = new Tablet(deviceId, schemaList);
        tablet.initBitMaps();
        tabletMap.put(deviceId, tablet);
      }
      ArrayList<Tablet> tabletList = new ArrayList<>(tabletMap.values());
      for (Tablet tablet : tabletList) {
        tsFileWriter.registerTimeseries(new Path(tablet.deviceId), tablet.getSchemas());
      }
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        List<Field> fields = rowRecord.getFields();
        for (int i = 0; i < fields.size(); ) {
          for (Tablet tablet : tabletList) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
            List<MeasurementSchema> schemas = tablet.getSchemas();
            for (int j = 0; j < schemas.size(); j++) {
              MeasurementSchema measurementSchema = schemas.get(j);
              Object value = fields.get(i).getObjectValue(measurementSchema.getType());
              if (value == null) {
                tablet.bitMaps[j].mark(rowIndex);
              }
              tablet.addValue(measurementSchema.getMeasurementId(), rowIndex, value);
              i++;
            }
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
              tsFileWriter.write(tablet);
              tablet.initBitMaps();
              tablet.reset();
            }
          }
        }
      }
      for (Tablet tablet : tabletList) {
        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
        }
      }
      tsFileWriter.flushAllChunkGroups();
    }
  }

  private static TSDataType getTsDataType(String type) {
    switch (type) {
      case "INT64":
        return TSDataType.INT64;
      case "INT32":
        return TSDataType.INT32;
      case "FLOAT":
        return TSDataType.FLOAT;
      case "DOUBLE":
        return TSDataType.DOUBLE;
      case "TEXT":
        return TSDataType.TEXT;
      case "BOOLEAN":
        return TSDataType.BOOLEAN;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }
}
