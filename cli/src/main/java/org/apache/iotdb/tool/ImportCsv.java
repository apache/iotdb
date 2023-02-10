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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.BOOLEAN;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.FLOAT;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class ImportCsv extends AbstractCsvTool {

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";

  private static final String FAILED_FILE_ARGS = "fd";
  private static final String FAILED_FILE_NAME = "failed file directory";

  private static final String BATCH_POINT_SIZE_ARGS = "batch";
  private static final String BATCH_POINT_SIZE_NAME = "batch point size";

  private static final String ALIGNED_ARGS = "aligned";
  private static final String ALIGNED_NAME = "use the aligned interface";

  private static final String CSV_SUFFIXS = "csv";
  private static final String TXT_SUFFIXS = "txt";

  private static final String TIMESTAMP_PRECISION_ARGS = "tp";
  private static final String TIMESTAMP_PRECISION_NAME = "timestamp precision (ms/us/ns)";

  private static final String TYPE_INFER_ARGS = "typeInfer";
  private static final String TYPE_INFER_ARGS_NAME = "type infer";

  private static final String LINES_PER_FAILED_FILE_ARGS = "linesPerFailedFile";
  private static final String LINES_PER_FAILED_FILE_ARGS_NAME = "Lines Per FailedFile";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";

  private static String targetPath;
  private static String failedFileDirectory = null;
  private static int linesPerFailedFile = 10000;
  private static Boolean aligned = false;

  private static String timeColumn = "Time";
  private static String deviceColumn = "Device";

  private static int batchPointSize = 100_000;

  private static String timestampPrecision = "ms";

  private static final Map<String, TSDataType> TYPE_INFER_KEY_DICT = new HashMap<>();

  static {
    TYPE_INFER_KEY_DICT.put("boolean", TSDataType.BOOLEAN);
    TYPE_INFER_KEY_DICT.put("int", TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put("long", TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put("float", TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put("double", TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put("NaN", TSDataType.DOUBLE);
  }

  private static final Map<String, TSDataType> TYPE_INFER_VALUE_DICT = new HashMap<>();

  static {
    TYPE_INFER_VALUE_DICT.put("boolean", TSDataType.BOOLEAN);
    TYPE_INFER_VALUE_DICT.put("int", TSDataType.INT32);
    TYPE_INFER_VALUE_DICT.put("long", TSDataType.INT64);
    TYPE_INFER_VALUE_DICT.put("float", TSDataType.FLOAT);
    TYPE_INFER_VALUE_DICT.put("double", TSDataType.DOUBLE);
    TYPE_INFER_VALUE_DICT.put("text", TSDataType.TEXT);
  }

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = createNewOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .argName(FILE_NAME)
            .hasArg()
            .desc(
                "If input a file path, load a csv file, "
                    + "otherwise load all csv file under this directory (required)")
            .build();
    options.addOption(opFile);

    Option opFailedFile =
        Option.builder(FAILED_FILE_ARGS)
            .argName(FAILED_FILE_NAME)
            .hasArg()
            .desc(
                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
            .build();
    options.addOption(opFailedFile);

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .argName(ALIGNED_NAME)
            .hasArg()
            .desc("Whether to use the interface of aligned (optional)")
            .build();
    options.addOption(opAligned);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .argName(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .desc("100000 (optional)")
            .build();
    options.addOption(opBatchPointSize);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .argName(TIMESTAMP_PRECISION_NAME)
            .hasArg()
            .desc("Timestamp precision (ms/us/ns)")
            .build();

    options.addOption(opTimestampPrecision);

    Option opTypeInfer =
        Option.builder(TYPE_INFER_ARGS)
            .argName(TYPE_INFER_ARGS_NAME)
            .numberOfArgs(5)
            .hasArgs()
            .valueSeparator(',')
            .desc("Define type info by option:\"boolean=text,int=long, ...")
            .build();
    options.addOption(opTypeInfer);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc("Lines per failedfile")
            .build();
    options.addOption(opFailedLinesPerFile);

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
    if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      }
    }
    if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
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
    if (key.equals("NaN")
        && !(value.equals("float") || value.equals("double") || value.equals("text"))) {
      throw new ArgsErrorException("NaN can not convert to " + value);
    }
    if (key.equals("boolean") && !(value.equals("boolean") || value.equals("text"))) {
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
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        System.exit(CODE_ERROR);
      }
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
      System.exit(CODE_ERROR);
    }

    System.exit(
        importFromTargetPath(
            host, Integer.parseInt(port), username, password, targetPath, timeZoneID));
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
        importFromSingleFile(file);
      } else if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files == null) {
          return CODE_OK;
        }

        for (File subFile : files) {
          if (subFile.isFile()) {
            importFromSingleFile(subFile);
          }
        }
      } else {
        System.out.println("File not found!");
        return CODE_ERROR;
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Encounter an error when connecting to server, because " + e.getMessage());
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
  private static void importFromSingleFile(File file) {
    if (file.getName().endsWith(CSV_SUFFIXS) || file.getName().endsWith(TXT_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          System.out.println("Empty file!");
          return;
        }
        if (!timeColumn.equalsIgnoreCase(headerNames.get(0))) {
          System.out.println("No headers!");
          return;
        }
        String failedFilePath = null;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        if (!deviceColumn.equalsIgnoreCase(headerNames.get(1))) {
          writeDataAlignedByTime(headerNames, records, failedFilePath);
        } else {
          writeDataAlignedByDevice(headerNames, records, failedFilePath);
        }
      } catch (IOException | IllegalPathException e) {
        System.out.println("CSV file read exception because: " + e.getMessage());
      }
    } else {
      System.out.println("The file name must end with \"csv\" or \"txt\"!");
    }
  }

  /**
   * if the data is aligned by time, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  private static void writeDataAlignedByTime(
      List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    if (headerTypeMap.isEmpty()) {
      try {
        queryType(devices, headerTypeMap, "Time");
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }

    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();

    AtomicReference<Boolean> hasStarted = new AtomicReference<>(false);
    AtomicInteger pointSize = new AtomicInteger(0);

    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    records.forEach(
        record -> {
          if (!hasStarted.get()) {
            hasStarted.set(true);
          } else if (pointSize.get() >= batchPointSize) {
            writeAndEmptyDataSet(deviceIds, times, typesList, valuesList, measurementsList, 3);
            pointSize.set(0);
          }

          boolean isFail = false;

          for (String deviceId : deviceAndMeasurementNames.keySet()) {
            ArrayList<TSDataType> types = new ArrayList<>();
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<String> measurements = new ArrayList<>();

            List<String> measurementNames = deviceAndMeasurementNames.get(deviceId);
            for (String measurement : measurementNames) {
              String header = deviceId + "." + measurement;
              String value = record.get(headerNameMap.get(header));
              if (!"".equals(value)) {
                TSDataType type;
                if (!headerTypeMap.containsKey(header)) {
                  type = typeInfer(value);
                  if (type != null) {
                    headerTypeMap.put(header, type);
                  } else {
                    System.out.printf(
                        "Line '%s', column '%s': '%s' unknown type%n",
                        record.getRecordNumber(), header, value);
                    isFail = true;
                  }
                }
                type = headerTypeMap.get(header);
                if (type != null) {
                  Object valueTrans = typeTrans(value, type);
                  if (valueTrans == null) {
                    isFail = true;
                    System.out.printf(
                        "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                        record.getRecordNumber(), header, value, type);
                  } else {
                    measurements.add(header.replace(deviceId + '.', ""));
                    types.add(type);
                    values.add(valueTrans);
                    pointSize.getAndIncrement();
                  }
                }
              }
            }
            if (!measurements.isEmpty()) {
              times.add(parseTimestamp(record.get(timeColumn)));
              deviceIds.add(deviceId);
              typesList.add(types);
              valuesList.add(values);
              measurementsList.add(measurements);
            }
          }
          if (isFail) {
            failedRecords.add(record.stream().collect(Collectors.toList()));
          }
        });
    if (!deviceIds.isEmpty()) {
      writeAndEmptyDataSet(deviceIds, times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }

    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    if (hasStarted.get()) {
      System.out.println("Import completely!");
    } else {
      System.out.println("No records!");
    }
  }

  /**
   * if the data is aligned by device, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  private static void writeDataAlignedByDevice(
      List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, null, headerTypeMap, headerNameMap);

    AtomicReference<String> deviceName = new AtomicReference<>(null);

    HashSet<String> typeQueriedDevice = new HashSet<>();

    // the data that interface need
    List<Long> times = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();

    AtomicInteger pointSize = new AtomicInteger(0);

    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    records.forEach(
        record -> {
          // only run in first record
          if (deviceName.get() == null) {
            deviceName.set(record.get(1));
          } else if (!Objects.equals(deviceName.get(), record.get(1))) {
            // if device changed
            writeAndEmptyDataSet(
                deviceName.get(), times, typesList, valuesList, measurementsList, 3);
            deviceName.set(record.get(1));
            pointSize.set(0);
          } else if (pointSize.get() >= batchPointSize) {
            // insert a batch
            writeAndEmptyDataSet(
                deviceName.get(), times, typesList, valuesList, measurementsList, 3);
            pointSize.set(0);
          }

          // the data of the record
          ArrayList<TSDataType> types = new ArrayList<>();
          ArrayList<Object> values = new ArrayList<>();
          ArrayList<String> measurements = new ArrayList<>();

          AtomicReference<Boolean> isFail = new AtomicReference<>(false);

          // read data from record
          for (Map.Entry<String, String> headerNameEntry : headerNameMap.entrySet()) {
            // headerNameWithoutType is equal to headerName if the CSV column do not have data type.
            String headerNameWithoutType = headerNameEntry.getKey();
            String headerName = headerNameEntry.getValue();
            String value = record.get(headerName);
            if (!"".equals(value)) {
              TSDataType type;
              // Get the data type directly if the CSV column have data type.
              if (!headerTypeMap.containsKey(headerNameWithoutType)) {
                boolean hasResult = false;
                // query the data type in iotdb
                if (!typeQueriedDevice.contains(deviceName.get())) {
                  try {
                    if (headerTypeMap.isEmpty()) {
                      hasResult =
                          queryType(
                              new HashSet<String>() {
                                {
                                  add(deviceName.get());
                                }
                              },
                              headerTypeMap,
                              "Device");
                    }
                    typeQueriedDevice.add(deviceName.get());
                  } catch (IoTDBConnectionException e) {
                    e.printStackTrace();
                  }
                }
                if (!hasResult) {
                  type = typeInfer(value);
                  if (type != null) {
                    headerTypeMap.put(headerNameWithoutType, type);
                  } else {
                    System.out.printf(
                        "Line '%s', column '%s': '%s' unknown type%n",
                        record.getRecordNumber(), headerNameWithoutType, value);
                    isFail.set(true);
                  }
                }
              }
              type = headerTypeMap.get(headerNameWithoutType);
              if (type != null) {
                Object valueTrans = typeTrans(value, type);
                if (valueTrans == null) {
                  isFail.set(true);
                  System.out.printf(
                      "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                      record.getRecordNumber(), headerNameWithoutType, value, type);
                } else {
                  values.add(valueTrans);
                  measurements.add(headerNameWithoutType);
                  types.add(type);
                  pointSize.getAndIncrement();
                }
              }
            }
          }
          if (isFail.get()) {
            failedRecords.add(record.stream().collect(Collectors.toList()));
          }
          if (!measurements.isEmpty()) {
            times.add(parseTimestamp(record.get(timeColumn)));
            typesList.add(types);
            valuesList.add(values);
            measurementsList.add(measurements);
          }
        });
    if (times.size() != 0) {
      writeAndEmptyDataSet(deviceName.get(), times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    System.out.println("Import completely!");
  }

  private static void writeFailedLinesFile(
      List<String> headerNames, String failedFilePath, ArrayList<List<Object>> failedRecords) {
    int fileIndex = 0;
    int from = 0;
    int failedRecordsSize = failedRecords.size();
    int restFailedRecords = failedRecordsSize;
    while (from < failedRecordsSize) {
      int step = Math.min(restFailedRecords, linesPerFailedFile);
      writeCsvFile(
          headerNames,
          failedRecords.subList(from, from + step),
          failedFilePath + "_" + fileIndex++);
      from += step;
      restFailedRecords -= step;
    }
  }

  private static void writeAndEmptyDataSet(
      String device,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (!aligned) {
        session.insertRecordsOfOneDevice(device, times, measurementsList, typesList, valuesList);
      } else {
        session.insertAlignedRecordsOfOneDevice(
            device, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        try {
          session.open();
        } catch (IoTDBConnectionException ex) {
          System.out.println("Meet error when insert csv because " + e.getMessage());
        }
        writeAndEmptyDataSet(device, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      System.out.println("Meet error when insert csv because " + e.getMessage());
    } finally {
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  private static void writeAndEmptyDataSet(
      List<String> deviceIds,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (!aligned) {
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } else {
        session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        try {
          session.open();
        } catch (IoTDBConnectionException ex) {
          System.out.println("Meet error when insert csv because " + e.getMessage());
        }
        writeAndEmptyDataSet(
            deviceIds, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      System.out.println("Meet error when insert csv because " + e.getMessage());
    } finally {
      deviceIds.clear();
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return CSVParser csv parser
   * @throws IOException when reading the csv file failed.
   */
  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .setQuote('`')
        .setEscape('\\')
        .setIgnoreEmptyLines(true)
        .build()
        .parse(new InputStreamReader(new FileInputStream(path)));
  }

  /**
   * parse deviceNames, measurementNames(aligned by time), headerType from headers
   *
   * @param headerNames
   * @param deviceAndMeasurementNames
   * @param headerTypeMap
   * @param headerNameMap
   */
  private static void parseHeaders(
      List<String> headerNames,
      @Nullable HashMap<String, List<String>> deviceAndMeasurementNames,
      HashMap<String, TSDataType> headerTypeMap,
      HashMap<String, String> headerNameMap)
      throws IllegalPathException {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    for (String headerName : headerNames) {
      if ("Time".equalsIgnoreCase(headerName)) {
        timeColumn = headerName;
        continue;
      } else if ("Device".equalsIgnoreCase(headerName)) {
        deviceColumn = headerName;
        continue;
      }
      Matcher matcher = pattern.matcher(headerName);
      String type;
      String headerNameWithoutType;
      if (matcher.find()) {
        type = matcher.group();
        headerNameWithoutType = headerName.replace("(" + type + ")", "").replaceAll("\\s+", "");
        headerNameMap.put(headerNameWithoutType, headerName);
        headerTypeMap.put(headerNameWithoutType, getType(type));
      } else {
        headerNameWithoutType = headerName;
        headerNameMap.put(headerName, headerName);
      }
      String[] split = PathUtils.splitPathToDetachedNodes(headerNameWithoutType);
      String measurementName = split[split.length - 1];
      String deviceName = StringUtils.join(Arrays.copyOfRange(split, 0, split.length - 1), '.');
      if (deviceAndMeasurementNames != null) {
        if (!deviceAndMeasurementNames.containsKey(deviceName)) {
          deviceAndMeasurementNames.put(deviceName, new ArrayList<>());
        }
        deviceAndMeasurementNames.get(deviceName).add(measurementName);
      }
    }
  }

  /**
   * query data type of timeseries from IoTDB
   *
   * @param deviceNames
   * @param headerTypeMap
   * @param alignedType
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  private static boolean queryType(
      Set<String> deviceNames, HashMap<String, TSDataType> headerTypeMap, String alignedType)
      throws IoTDBConnectionException {
    boolean hasResult = false;
    for (String deviceName : deviceNames) {
      String sql = "show timeseries " + deviceName + ".*";
      SessionDataSet sessionDataSet = null;
      try {
        sessionDataSet = session.executeQueryStatement(sql);
        int tsIndex = sessionDataSet.getColumnNames().indexOf(ColumnHeaderConstant.TIMESERIES);
        int dtIndex = sessionDataSet.getColumnNames().indexOf(ColumnHeaderConstant.DATATYPE);
        while (sessionDataSet.hasNext()) {
          hasResult = true;
          RowRecord record = sessionDataSet.next();
          List<Field> fields = record.getFields();
          String timeseries = fields.get(tsIndex).getStringValue();
          String dataType = fields.get(dtIndex).getStringValue();
          if (Objects.equals(alignedType, "Time")) {
            headerTypeMap.put(timeseries, getType(dataType));
          } else if (Objects.equals(alignedType, "Device")) {
            String[] split = PathUtils.splitPathToDetachedNodes(timeseries);
            String measurement = split[split.length - 1];
            headerTypeMap.put(measurement, getType(dataType));
          }
        }
      } catch (StatementExecutionException | IllegalPathException e) {
        System.out.println(
            "Meet error when query the type of timeseries because " + e.getMessage());
        return false;
      }
    }
    return hasResult;
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  private static TSDataType getType(String typeStr) {
    switch (typeStr) {
      case "TEXT":
        return TEXT;
      case "BOOLEAN":
        return BOOLEAN;
      case "INT32":
        return INT32;
      case "INT64":
        return INT64;
      case "FLOAT":
        return FLOAT;
      case "DOUBLE":
        return DOUBLE;
      default:
        return null;
    }
  }

  /**
   * if data type of timeseries is not defined in headers of schema, this method will be called to
   * do type inference
   *
   * @param strValue
   * @return
   */
  private static TSDataType typeInfer(String strValue) {
    if (strValue.contains("\"")) {
      return TEXT;
    }
    if (isBoolean(strValue)) {
      return TYPE_INFER_KEY_DICT.get("boolean");
    } else if (isNumber(strValue)) {
      if (!strValue.contains(TsFileConstant.PATH_SEPARATOR)) {
        if (isConvertFloatPrecisionLack(StringUtils.trim(strValue))) {
          return TYPE_INFER_KEY_DICT.get("long");
        }
        return TYPE_INFER_KEY_DICT.get("int");
      } else {
        return TYPE_INFER_KEY_DICT.get("float");
      }
    } else if ("null".equals(strValue) || "NULL".equals(strValue)) {
      return null;
      // "NaN" is returned if the NaN Literal is given in Parser
    } else if ("NaN".equals(strValue)) {
      return TYPE_INFER_KEY_DICT.get("NaN");
    } else {
      return TSDataType.TEXT;
    }
  }

  static boolean isNumber(String s) {
    if (s == null || s.equals("NaN")) {
      return false;
    }
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isBoolean(String s) {
    return s.equalsIgnoreCase(SqlConstant.BOOLEAN_TRUE)
        || s.equalsIgnoreCase(SqlConstant.BOOLEAN_FALSE);
  }

  private static boolean isConvertFloatPrecisionLack(String s) {
    return Long.parseLong(s) > (2 << 24);
  }

  /**
   * @param value
   * @param type
   * @return
   */
  private static Object typeTrans(String value, TSDataType type) {
    try {
      switch (type) {
        case TEXT:
          if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
          }
          return value;
        case BOOLEAN:
          if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            return null;
          }
          return Boolean.parseBoolean(value);
        case INT32:
          return Integer.parseInt(value);
        case INT64:
          return Long.parseLong(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DOUBLE:
          return Double.parseDouble(value);
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static long parseTimestamp(String str) {
    long timestamp;
    try {
      timestamp = Long.parseLong(str);
    } catch (NumberFormatException e) {
      timestamp = DateTimeUtils.convertDatetimeStrToLong(str, zoneId, timestampPrecision);
    }
    return timestamp;
  }
}
