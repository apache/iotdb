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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

  private static final String CSV_SUFFIXS = "csv";
  private static final String TXT_SUFFIXS = "txt";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";
  private static final String ILLEGAL_PATH_ARGUMENT = "Path parameter is null";

  private static String targetPath;
  private static String failedFileDirectory = null;

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

    return options;
  }

  /**
   * parse optional params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) {
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    targetPath = commandLine.getOptionValue(FILE_ARGS);
    if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      }
    }
  }

  public static void main(String[] args) throws IOException, IoTDBConnectionException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }

    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        return;
      }
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
    }

    importFromTargetPath(host, Integer.valueOf(port), username, password, targetPath, timeZoneID);
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
   * @throws IoTDBConnectionException
   */
  public static void importFromTargetPath(
      String host, int port, String username, String password, String targetPath, String timeZone)
      throws IoTDBConnectionException {
    try {
      session = new Session(host, Integer.valueOf(port), username, password, false);
      session.open(false);
      timeZoneID = timeZone;
      setTimeZone();

      File file = new File(targetPath);
      if (file.isFile()) {
        importFromSingleFile(file);
      } else if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files == null) {
          return;
        }

        for (File subFile : files) {
          if (subFile.isFile()) {
            importFromSingleFile(subFile);
          }
        }
      } else {
        System.out.println("File not found!");
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Encounter an error when connecting to server, because " + e.getMessage());
    } finally {
      if (session != null) {
        session.close();
      }
    }
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
        List<CSVRecord> records = csvRecords.getRecords();
        if (headerNames.isEmpty()) {
          System.out.println("Empty file!");
          return;
        }
        if (!headerNames.contains("Time")) {
          System.out.println("No headers!");
          return;
        }
        if (records.isEmpty()) {
          System.out.println("No records!");
          return;
        }
        String failedFilePath = null;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        if (!headerNames.contains("Device")) {
          writeDataAlignedByTime(headerNames, records, failedFilePath);
        } else {
          writeDataAlignedByDevice(headerNames, records, failedFilePath);
        }
      } catch (IOException e) {
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
      List<String> headerNames, List<CSVRecord> records, String failedFilePath) {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    String devicesStr = StringUtils.join(devices, ",");
    try {
      queryType(devicesStr, headerTypeMap, "Time");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }

    SimpleDateFormat timeFormatter = formatterInit(records.get(0).get("Time"));

    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    for (Map.Entry<String, List<String>> entry : deviceAndMeasurementNames.entrySet()) {
      String deviceId = entry.getKey();
      List<Long> times = new ArrayList<>();
      List<String> measurementNames = entry.getValue();
      List<List<TSDataType>> typesList = new ArrayList<>();
      List<List<Object>> valuesList = new ArrayList<>();
      List<List<String>> measurementsList = new ArrayList<>();
      records.stream()
          .forEach(
              record -> {
                ArrayList<TSDataType> types = new ArrayList<>();
                ArrayList<Object> values = new ArrayList<>();
                ArrayList<String> measurements = new ArrayList<>();
                AtomicReference<Boolean> isFail = new AtomicReference<>(false);
                measurementNames.stream()
                    .forEach(
                        measurementName -> {
                          String header = deviceId + "." + measurementName;
                          String value = record.get(header);
                          if (!value.equals("")) {
                            TSDataType type;
                            if (!headerTypeMap.containsKey(headerNameMap.get(header))) {
                              type = typeInfer(value);
                              if (type != null) {
                                headerTypeMap.put(header, type);
                              } else {
                                System.out.println(
                                    String.format(
                                        "Line '%s', column '%s': '%s' unknown type",
                                        (records.indexOf(record) + 1), header, value));
                                isFail.set(true);
                              }
                            }
                            type = headerTypeMap.get(headerNameMap.get(header));
                            if (type != null) {
                              Object valueTransed = typeTrans(value, type);
                              if (valueTransed == null) {
                                isFail.set(true);
                                System.out.println(
                                    String.format(
                                        "Line '%s', column '%s': '%s' can't convert to '%s'",
                                        (records.indexOf(record) + 1), header, value, type));
                              } else {
                                measurements.add(
                                    headerNameMap.get(header).replace(deviceId + '.', ""));
                                types.add(type);
                                values.add(valueTransed);
                              }
                            }
                          }
                        });
                if (isFail.get()) {
                  failedRecords.add(record.stream().collect(Collectors.toList()));
                }
                if (!measurements.isEmpty()) {
                  try {
                    if (timeFormatter == null) {
                      try {
                        times.add(Long.valueOf(record.get("Time")));
                      } catch (Exception e) {
                        System.out.println(
                            "Meet error when insert csv because the format of time is not supported");
                        System.exit(0);
                      }
                    } else {
                      times.add(timeFormatter.parse(record.get("Time")).getTime());
                    }
                  } catch (ParseException e) {
                    e.printStackTrace();
                  }
                  typesList.add(types);
                  valuesList.add(values);
                  measurementsList.add(measurements);
                }
              });
      try {
        session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
      } catch (StatementExecutionException | IoTDBConnectionException e) {
        System.out.println("Meet error when insert csv because " + e.getMessage());
        System.exit(0);
      }
    }
    if (!failedRecords.isEmpty()) {
      writeCsvFile(headerNames, failedRecords, failedFilePath);
    }
    System.out.println("Import completely!");
  }

  /**
   * if the data is aligned by device, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  private static void writeDataAlignedByDevice(
      List<String> headerNames, List<CSVRecord> records, String failedFilePath) {
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, null, headerTypeMap, headerNameMap);
    Set<String> devices =
        records.stream().map(record -> record.get("Device")).collect(Collectors.toSet());
    String devicesStr = StringUtils.join(devices, ",");
    try {
      queryType(devicesStr, headerTypeMap, "Device");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }

    SimpleDateFormat timeFormatter = formatterInit(records.get(0).get("Time"));
    Set<String> measurementNames = headerNameMap.keySet();
    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    devices.stream()
        .forEach(
            device -> {
              List<Long> times = new ArrayList<>();

              List<List<TSDataType>> typesList = new ArrayList<>();
              List<List<Object>> valuesList = new ArrayList<>();
              List<List<String>> measurementsList = new ArrayList<>();

              records.stream()
                  .filter(record -> record.get("Device").equals(device))
                  .forEach(
                      record -> {
                        ArrayList<TSDataType> types = new ArrayList<>();
                        ArrayList<Object> values = new ArrayList<>();
                        ArrayList<String> measurements = new ArrayList<>();

                        AtomicReference<Boolean> isFail = new AtomicReference<>(false);

                        measurementNames.stream()
                            .forEach(
                                measurement -> {
                                  String value = record.get(measurement);
                                  if (!value.equals("")) {
                                    TSDataType type;
                                    if (!headerTypeMap.containsKey(
                                        headerNameMap.get(measurement))) {
                                      type = typeInfer(value);
                                      if (type != null) {
                                        headerTypeMap.put(measurement, type);
                                      } else {
                                        System.out.println(
                                            String.format(
                                                "Line '%s', column '%s': '%s' unknown type",
                                                (records.indexOf(record) + 1), measurement, value));
                                        isFail.set(true);
                                      }
                                    }
                                    type = headerTypeMap.get(headerNameMap.get(measurement));
                                    if (type != null) {
                                      Object valueTransed = typeTrans(value, type);
                                      if (valueTransed == null) {
                                        isFail.set(true);
                                        System.out.println(
                                            String.format(
                                                "Line '%s', column '%s': '%s' can't convert to '%s'",
                                                (records.indexOf(record) + 1),
                                                measurement,
                                                value,
                                                type));
                                      } else {
                                        values.add(valueTransed);
                                        measurements.add(headerNameMap.get(measurement));
                                        types.add(type);
                                      }
                                    }
                                  }
                                });
                        if (isFail.get()) {
                          failedRecords.add(record.stream().collect(Collectors.toList()));
                        }
                        if (!measurements.isEmpty()) {
                          try {
                            if (timeFormatter == null) {
                              try {
                                times.add(Long.valueOf(record.get("Time")));
                              } catch (Exception e) {
                                System.out.println(
                                    "Meet error when insert csv because the format of time is not supported");
                                System.exit(0);
                              }
                            } else {
                              times.add(timeFormatter.parse(record.get("Time")).getTime());
                            }
                          } catch (ParseException e) {
                            e.printStackTrace();
                          }
                          typesList.add(types);
                          valuesList.add(values);
                          measurementsList.add(measurements);
                        }
                      });
              try {
                session.insertRecordsOfOneDevice(
                    device, times, measurementsList, typesList, valuesList);
              } catch (StatementExecutionException | IoTDBConnectionException e) {
                System.out.println("Meet error when insert csv because " + e.getMessage());
                System.exit(0);
              }
            });
    if (!failedRecords.isEmpty()) {
      writeCsvFile(headerNames, failedRecords, failedFilePath);
    }
    System.out.println("Import completely!");
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return
   * @throws IOException
   */
  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.EXCEL
        .withFirstRecordAsHeader()
        .withQuote('`')
        .withEscape('\\')
        .withIgnoreEmptyLines()
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
      HashMap<String, String> headerNameMap) {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    for (String headerName : headerNames) {
      if (headerName.equals("Time") || headerName.equals("Device")) continue;
      Matcher matcher = pattern.matcher(headerName);
      String type;
      if (matcher.find()) {
        type = matcher.group();
        String headerNameWithoutType =
            headerName.replace("(" + type + ")", "").replaceAll("\\s+", "");
        headerNameMap.put(headerName, headerNameWithoutType);
        headerTypeMap.put(headerNameWithoutType, getType(type));
      } else {
        headerNameMap.put(headerName, headerName);
      }
      String[] split = headerName.split("\\.");
      String measurementName = split[split.length - 1];
      String deviceName = headerName.replace("." + measurementName, "");
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
  private static void queryType(
      String deviceNames, HashMap<String, TSDataType> headerTypeMap, String alignedType)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select * from " + deviceNames + " limit 1";
    SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<String> columnTypes = sessionDataSet.getColumnTypes();
    for (int i = 1; i < columnNames.size(); i++) {
      if (alignedType == "Time") {
        headerTypeMap.put(columnNames.get(i), getType(columnTypes.get(i)));
      } else if (alignedType == "Device") {
        String[] split = columnNames.get(i).split("\\.");
        String measurement = split[split.length - 1];
        headerTypeMap.put(measurement, getType(columnTypes.get(i)));
      }
    }
  }

  /**
   * return a suit time formatter
   *
   * @param time
   * @return
   */
  private static SimpleDateFormat formatterInit(String time) {
    try {
      Long.parseLong(time);
      return null;
    } catch (Exception ignored) {
      // do nothing
    }

    for (String timeFormat : STRING_TIME_FORMAT) {
      SimpleDateFormat format = new SimpleDateFormat(timeFormat);
      try {
        format.parse(time).getTime();
        System.out.println(timeFormat);
        return format;
      } catch (java.text.ParseException ignored) {
        // do nothing
      }
    }
    return null;
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
   * @param value
   * @return
   */
  private static TSDataType typeInfer(String value) {
    if (value.contains("\"")) return TEXT;
    else if (value.equals("true") || value.equals("false")) return BOOLEAN;
    else if (!value.contains(".")) {
      try {
        Integer.valueOf(value);
        return INT32;
      } catch (Exception e) {
        try {
          Long.valueOf(value);
          return INT64;
        } catch (Exception exception) {
          return null;
        }
      }
    } else {
      if (Float.valueOf(value).toString().length() == Double.valueOf(value).toString().length())
        return FLOAT;
      else return DOUBLE;
    }
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
          return value.substring(1, value.length() - 1);
        case BOOLEAN:
          if (!value.equals("true") && !value.equals("false")) {
            return null;
          }
          return Boolean.valueOf(value);
        case INT32:
          return Integer.valueOf(value);
        case INT64:
          return Long.valueOf(value);
        case FLOAT:
          return Float.valueOf(value);
        case DOUBLE:
          return Double.valueOf(value);
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
