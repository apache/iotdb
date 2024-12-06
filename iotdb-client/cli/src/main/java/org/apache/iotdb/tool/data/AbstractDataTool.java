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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.ZoneId;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tsfile.enums.TSDataType.STRING;
import static org.apache.tsfile.enums.TSDataType.TEXT;

public abstract class AbstractDataTool {

  protected static final String FILE_TYPE_ARGS = "ft";
  protected static final String FILE_TYPE_NAME = "file_type";
  protected static final String FILE_TYPE_ARGS_NAME = "format";

  protected static final String HOST_ARGS = "h";
  protected static final String HOST_NAME = "host";
  protected static final String HOST_DEFAULT_VALUE = "127.0.0.1";

  protected static final String HELP_ARGS = "help";

  protected static final String PORT_ARGS = "p";
  protected static final String PORT_NAME = "port";
  protected static final String PORT_DEFAULT_VALUE = "6667";

  protected static final String PW_ARGS = "pw";
  protected static final String PW_NAME = "password";
  protected static final String PW_DEFAULT_VALUE = "root";

  protected static final String USERNAME_ARGS = "u";
  protected static final String USERNAME_NAME = "username";
  protected static final String USERNAME_DEFAULT_VALUE = "root";

  protected static final String TIME_FORMAT_ARGS = "tf";
  protected static final String TIME_FORMAT_NAME = "time_format";

  protected static final String TIME_ZONE_ARGS = "tz";
  protected static final String TIME_ZONE_NAME = "timezone";
  protected static final String TIMEOUT_ARGS = "timeout";
  protected static final String TIMEOUT_NAME = "query_timeout";
  protected static final int MAX_HELP_CONSOLE_WIDTH = 92;
  protected static final String[] TIME_FORMAT =
      new String[] {"default", "long", "number", "timestamp"};
  protected static final String[] STRING_TIME_FORMAT =
      new String[] {
        "yyyy-MM-dd HH:mm:ss.SSSX",
        "yyyy/MM/dd HH:mm:ss.SSSX",
        "yyyy.MM.dd HH:mm:ss.SSSX",
        "yyyy-MM-dd HH:mm:ssX",
        "yyyy/MM/dd HH:mm:ssX",
        "yyyy.MM.dd HH:mm:ssX",
        "yyyy-MM-dd HH:mm:ss.SSSz",
        "yyyy/MM/dd HH:mm:ss.SSSz",
        "yyyy.MM.dd HH:mm:ss.SSSz",
        "yyyy-MM-dd HH:mm:ssz",
        "yyyy/MM/dd HH:mm:ssz",
        "yyyy.MM.dd HH:mm:ssz",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy/MM/dd HH:mm:ss.SSS",
        "yyyy.MM.dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy/MM/dd'T'HH:mm:ss.SSSX",
        "yyyy.MM.dd'T'HH:mm:ss.SSSX",
        "yyyy-MM-dd'T'HH:mm:ssX",
        "yyyy/MM/dd'T'HH:mm:ssX",
        "yyyy.MM.dd'T'HH:mm:ssX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSz",
        "yyyy/MM/dd'T'HH:mm:ss.SSSz",
        "yyyy.MM.dd'T'HH:mm:ss.SSSz",
        "yyyy-MM-dd'T'HH:mm:ssz",
        "yyyy/MM/dd'T'HH:mm:ssz",
        "yyyy.MM.dd'T'HH:mm:ssz",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy/MM/dd'T'HH:mm:ss.SSS",
        "yyyy.MM.dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy/MM/dd'T'HH:mm:ss",
        "yyyy.MM.dd'T'HH:mm:ss"
      };
  protected static final String INSERT_CSV_MEET_ERROR_MSG = "Meet error when insert csv because ";
  protected static final String CSV_SUFFIXS = "csv";
  protected static final String TXT_SUFFIXS = "txt";
  protected static final String SQL_SUFFIXS = "sql";
  protected static final String TSFILE_SUFFIXS = "tsfile";
  protected static final String TSFILEDB_CLI_DIVIDE = "-------------------";
  protected static final String COLON = ": ";
  protected static final String MINUS = "-";
  protected static String failedFileDirectory = null;
  protected static String timeColumn = "Time";
  protected static String deviceColumn = "Device";
  protected static int linesPerFailedFile = 10000;
  protected static String timestampPrecision = "ms";
  protected static final int CODE_OK = 0;
  protected static final int CODE_ERROR = 1;

  protected static String host;
  protected static String port;
  protected static String username;
  protected static String password;
  protected static ZoneId zoneId = ZoneId.systemDefault();
  protected static String timeZoneID;
  protected static String timeFormat;
  protected static String exportType;
  protected static Boolean aligned;
  protected static Session session;
  protected static final LongAdder loadFileSuccessfulNum = new LongAdder();
  protected static final Map<String, TSDataType> TYPE_INFER_KEY_DICT = new HashMap<>();

  protected static final String DATATYPE_BOOLEAN = "boolean";
  protected static final String DATATYPE_INT = "int";
  protected static final String DATATYPE_LONG = "long";
  protected static final String DATATYPE_FLOAT = "float";
  protected static final String DATATYPE_DOUBLE = "double";
  protected static final String DATATYPE_TIMESTAMP = "timestamp";
  protected static final String DATATYPE_DATE = "date";
  protected static final String DATATYPE_BLOB = "blob";
  protected static final String DATATYPE_NAN = "NaN";
  protected static final String DATATYPE_TEXT = "text";

  protected static final String DATATYPE_NULL = "null";
  protected static int batchPointSize = 100_000;

  static {
    TYPE_INFER_KEY_DICT.put(DATATYPE_BOOLEAN, TSDataType.BOOLEAN);
    TYPE_INFER_KEY_DICT.put(DATATYPE_INT, TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put(DATATYPE_LONG, TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put(DATATYPE_FLOAT, TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put(DATATYPE_DOUBLE, TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put(DATATYPE_TIMESTAMP, TSDataType.TIMESTAMP);
    TYPE_INFER_KEY_DICT.put(DATATYPE_DATE, TSDataType.TIMESTAMP);
    TYPE_INFER_KEY_DICT.put(DATATYPE_BLOB, TSDataType.TEXT);
    TYPE_INFER_KEY_DICT.put(DATATYPE_NAN, TSDataType.DOUBLE);
  }

  protected static final Map<String, TSDataType> TYPE_INFER_VALUE_DICT = new HashMap<>();

  static {
    TYPE_INFER_VALUE_DICT.put(DATATYPE_BOOLEAN, TSDataType.BOOLEAN);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_INT, TSDataType.INT32);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_LONG, TSDataType.INT64);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_FLOAT, TSDataType.FLOAT);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_DOUBLE, TSDataType.DOUBLE);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_TIMESTAMP, TSDataType.TIMESTAMP);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_DATE, TSDataType.TIMESTAMP);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_BLOB, TSDataType.TEXT);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_TEXT, TSDataType.TEXT);
  }

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataTool.class);

  protected AbstractDataTool() {}

  protected static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (StringUtils.isNotBlank(defaultValue)) {
        return defaultValue;
      }
      String msg = String.format("Required values for option '%s' not provided", name);
      LOGGER.info(msg);
      LOGGER.info("Use -help for more information");
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void setTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    if (timeZoneID != null) {
      session.setTimeZone(timeZoneID);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  protected static void parseBasicParams(CommandLine commandLine) throws ArgsErrorException {
    host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, HOST_DEFAULT_VALUE);
    port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, PORT_DEFAULT_VALUE);
    username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, USERNAME_DEFAULT_VALUE);
    password = commandLine.getOptionValue(PW_ARGS, PW_DEFAULT_VALUE);
  }

  protected static void printHelpOptions(
      String cmdLineHead,
      String cmdLineSyntax,
      HelpFormatter hf,
      Options tsFileOptions,
      Options csvOptions,
      Options sqlOptions,
      boolean printFileType) {
    ioTPrinter.println(TSFILEDB_CLI_DIVIDE + "\n" + cmdLineSyntax + "\n" + TSFILEDB_CLI_DIVIDE);
    if (StringUtils.isNotBlank(cmdLineHead)) {
      ioTPrinter.println(cmdLineHead);
    }
    final String usageName = cmdLineSyntax.replaceAll(" ", "");
    if (ObjectUtils.isNotEmpty(tsFileOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n' + FILE_TYPE_NAME + COLON + TSFILE_SUFFIXS + '\n' + TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, tsFileOptions, true);
    }
    if (ObjectUtils.isNotEmpty(csvOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n' + FILE_TYPE_NAME + COLON + CSV_SUFFIXS + '\n' + TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, csvOptions, true);
    }
    if (ObjectUtils.isNotEmpty(sqlOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n' + FILE_TYPE_NAME + COLON + SQL_SUFFIXS + '\n' + TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, sqlOptions, true);
    }
  }

  protected static boolean checkTimeFormat() {
    for (String format : TIME_FORMAT) {
      if (timeFormat.equals(format)) {
        return true;
      }
    }
    for (String format : STRING_TIME_FORMAT) {
      if (timeFormat.equals(format)) {
        return true;
      }
    }
    LOGGER.info(
        "Input time format {} is not supported, "
            + "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS%n",
        timeFormat);
    return false;
  }

  protected static Options createImportOptions() {
    Options options = new Options();
    Option opFileType =
        Option.builder(FILE_TYPE_ARGS)
            .longOpt(FILE_TYPE_NAME)
            .argName(FILE_TYPE_ARGS_NAME)
            .required()
            .hasArg()
            .desc("Types of imported files: CSV, SQL, TSfile (required)")
            .build();
    options.addOption(opFileType);
    return createNewOptions(options);
  }

  protected static Options createExportOptions() {
    Options options = new Options();
    Option opFileType =
        Option.builder(FILE_TYPE_ARGS)
            .longOpt(FILE_TYPE_NAME)
            .argName(FILE_TYPE_ARGS_NAME)
            .required()
            .hasArg()
            .desc("Export file type ?You can choose tsfile)、csv) or sql) . (required)")
            .build();
    options.addOption(opFileType);
    return createNewOptions(options);
  }

  protected static Options createNewOptions(Options options) {
    Option opHost =
        Option.builder(HOST_ARGS)
            .longOpt(HOST_NAME)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (optional)")
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .longOpt(PORT_NAME)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (optional)")
            .build();
    options.addOption(opPort);

    Option opUsername =
        Option.builder(USERNAME_ARGS)
            .longOpt(USERNAME_NAME)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc("Username (optional)")
            .build();
    options.addOption(opUsername);

    Option opPassword =
        Option.builder(PW_ARGS)
            .longOpt(PW_NAME)
            .optionalArg(true)
            .argName(PW_NAME)
            .hasArg()
            .desc("Password (optional)")
            .build();
    options.addOption(opPassword);
    return options;
  }

  /**
   * if the data is aligned by device, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected static void writeDataAlignedByDevice(
      SessionPool sessionPool,
      List<String> headerNames,
      Stream<CSVRecord> records,
      String failedFilePath)
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
        recordObj -> {
          // only run in first record
          if (deviceName.get() == null) {
            deviceName.set(recordObj.get(1));
          } else if (!Objects.equals(deviceName.get(), recordObj.get(1))) {
            // if device changed
            writeAndEmptyDataSet(
                sessionPool, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
            deviceName.set(recordObj.get(1));
            pointSize.set(0);
          } else if (pointSize.get() >= batchPointSize) {
            // insert a batch
            writeAndEmptyDataSet(
                sessionPool, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
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
            String value = recordObj.get(headerName);
            if (!"".equals(value)) {
              TSDataType type;
              // Get the data type directly if the CSV column have data type.
              if (!headerTypeMap.containsKey(headerNameWithoutType)) {
                boolean hasResult = false;
                // query the data type in iotdb
                if (!typeQueriedDevice.contains(deviceName.get())) {
                  if (headerTypeMap.isEmpty()) {
                    Set<String> devices = new HashSet<>();
                    devices.add(deviceName.get());
                    queryType(sessionPool, devices, headerTypeMap, deviceColumn);
                  }
                  typeQueriedDevice.add(deviceName.get());
                }
                type = typeInfer(value);
                if (type != null) {
                  headerTypeMap.put(headerNameWithoutType, type);
                } else {
                  ioTPrinter.printf(
                      "Line '%s', column '%s': '%s' unknown type%n",
                      recordObj.getRecordNumber(), headerNameWithoutType, value);
                  isFail.set(true);
                }
              }
              type = headerTypeMap.get(headerNameWithoutType);
              if (type != null) {
                Object valueTrans = typeTrans(value, type);
                if (valueTrans == null) {
                  isFail.set(true);
                  ioTPrinter.printf(
                      "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                      recordObj.getRecordNumber(), headerNameWithoutType, value, type);
                } else {
                  values.add(valueTrans);
                  measurements.add(headerNameWithoutType);
                  types.add(type);
                  pointSize.getAndIncrement();
                }
              }
            }
          }
          if (Boolean.TRUE.equals(isFail.get())) {
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          }
          if (!measurements.isEmpty()) {
            times.add(parseTimestamp(recordObj.get(timeColumn)));
            typesList.add(types);
            valuesList.add(values);
            measurementsList.add(measurements);
          }
        });
    if (!times.isEmpty()) {
      writeAndEmptyDataSet(
          sessionPool, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    // ioTPrinter.println("Import completely!");
  }

  private static void writeAndEmptyDataSet(
      SessionPool sessionPool,
      String device,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (Boolean.FALSE.equals(aligned)) {
        sessionPool.insertRecordsOfOneDevice(
            device, times, measurementsList, typesList, valuesList);
      } else {
        sessionPool.insertAlignedRecordsOfOneDevice(
            device, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(
            sessionPool, device, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } finally {
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  private static void writeAndEmptyDataSet(
      Session session,
      List<String> deviceIds,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (Boolean.FALSE.equals(aligned)) {
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } else {
        session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        try {
          session.open();
        } catch (IoTDBConnectionException ex) {
          ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSet(
            session, deviceIds, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      try {
        session.close();
      } catch (IoTDBConnectionException ex) {
        // do nothing
      }
      System.exit(1);
    } finally {
      deviceIds.clear();
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  private static void writeAndEmptyDataSet(
      Session session,
      String device,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (Boolean.FALSE.equals(aligned)) {
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
          ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSet(
            session, device, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      try {
        session.close();
      } catch (IoTDBConnectionException ex) {
        // do nothing
      }
      System.exit(1);
    } finally {
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  private static void writeAndEmptyDataSet(
      SessionPool sessionPool,
      List<String> deviceIds,
      List<Long> times,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      List<List<String>> measurementsList,
      int retryTime) {
    try {
      if (Boolean.FALSE.equals(aligned)) {
        sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } else {
        sessionPool.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(
            sessionPool, deviceIds, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } finally {
      deviceIds.clear();
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
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

  /**
   * if data type of timeseries is not defined in headers of schema, this method will be called to
   * do type inference
   *
   * @param strValue
   * @return
   */
  private static TSDataType typeInfer(String strValue) {
    if (strValue.contains("\"")) {
      return strValue.length() <= 512 + 2 ? STRING : TEXT;
    }
    if (isBoolean(strValue)) {
      return TYPE_INFER_KEY_DICT.get(DATATYPE_BOOLEAN);
    } else if (isNumber(strValue)) {
      if (!strValue.contains(TsFileConstant.PATH_SEPARATOR)) {
        if (isConvertFloatPrecisionLack(StringUtils.trim(strValue))) {
          return TYPE_INFER_KEY_DICT.get(DATATYPE_LONG);
        }
        return TYPE_INFER_KEY_DICT.get(DATATYPE_INT);
      } else {
        return TYPE_INFER_KEY_DICT.get(DATATYPE_FLOAT);
      }
    } else if (DATATYPE_NULL.equals(strValue) || DATATYPE_NULL.toUpperCase().equals(strValue)) {
      return null;
      // "NaN" is returned if the NaN Literal is given in Parser
    } else if (DATATYPE_NAN.equals(strValue)) {
      return TYPE_INFER_KEY_DICT.get(DATATYPE_NAN);
    } else if (strValue.length() <= 512) {
      return STRING;
    } else {
      return TEXT;
    }
  }

  static boolean isNumber(String s) {
    if (s == null || s.equals(DATATYPE_NAN)) {
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
        case STRING:
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
        case TIMESTAMP:
        case DATE:
        case BLOB:
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
      SessionPool sessionPool,
      Set<String> deviceNames,
      HashMap<String, TSDataType> headerTypeMap,
      String alignedType) {
    for (String deviceName : deviceNames) {
      String sql = "show timeseries " + deviceName + ".*";
      try (SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement(sql)) {
        int tsIndex =
            sessionDataSetWrapper.getColumnNames().indexOf(ColumnHeaderConstant.TIMESERIES);
        int dtIndex = sessionDataSetWrapper.getColumnNames().indexOf(ColumnHeaderConstant.DATATYPE);
        while (sessionDataSetWrapper.hasNext()) {
          RowRecord rowRecord = sessionDataSetWrapper.next();
          List<Field> fields = rowRecord.getFields();
          String timeseries = fields.get(tsIndex).getStringValue();
          String dataType = fields.get(dtIndex).getStringValue();
          if (Objects.equals(alignedType, "Time")) {
            headerTypeMap.put(timeseries, getType(dataType));
          } else if (Objects.equals(alignedType, deviceColumn)) {
            String[] split = PathUtils.splitPathToDetachedNodes(timeseries);
            String measurement = split[split.length - 1];
            headerTypeMap.put(measurement, getType(dataType));
          }
        }
      } catch (StatementExecutionException | IllegalPathException | IoTDBConnectionException e) {
        ioTPrinter.println(
            "Meet error when query the type of timeseries because " + e.getMessage());
        System.exit(1);
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
      Session session,
      Set<String> deviceNames,
      HashMap<String, TSDataType> headerTypeMap,
      String alignedType) {
    for (String deviceName : deviceNames) {
      String sql = "show timeseries " + deviceName + ".*";
      SessionDataSet sessionDataSet = null;
      try {
        sessionDataSet = session.executeQueryStatement(sql);
        int tsIndex = sessionDataSet.getColumnNames().indexOf(ColumnHeaderConstant.TIMESERIES);
        int dtIndex = sessionDataSet.getColumnNames().indexOf(ColumnHeaderConstant.DATATYPE);
        while (sessionDataSet.hasNext()) {
          RowRecord rowRecord = sessionDataSet.next();
          List<Field> fields = rowRecord.getFields();
          String timeseries = fields.get(tsIndex).getStringValue();
          String dataType = fields.get(dtIndex).getStringValue();
          if (Objects.equals(alignedType, "Time")) {
            headerTypeMap.put(timeseries, getType(dataType));
          } else if (Objects.equals(alignedType, deviceColumn)) {
            String[] split = PathUtils.splitPathToDetachedNodes(timeseries);
            String measurement = split[split.length - 1];
            headerTypeMap.put(measurement, getType(dataType));
          }
        }
      } catch (StatementExecutionException | IllegalPathException | IoTDBConnectionException e) {
        ioTPrinter.println(
            "Meet error when query the type of timeseries because " + e.getMessage());
        try {
          session.close();
        } catch (IoTDBConnectionException ex) {
          // do nothing
        }
        System.exit(1);
      }
    }
  }

  /**
   * parse deviceNames, measurementNames(aligned by time), headerType from headers
   *
   * @param headerNames
   * @param deviceAndMeasurementNames
   * @param headerTypeMap
   * @param headerNameMap
   */
  @SuppressWarnings(
      "squid:S135") // ignore for loops should not contain more than a single "break" or "continue"
  // statement
  private static void parseHeaders(
      List<String> headerNames,
      @Nullable HashMap<String, List<String>> deviceAndMeasurementNames,
      HashMap<String, TSDataType> headerTypeMap,
      HashMap<String, String> headerNameMap)
      throws IllegalPathException {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    for (String headerName : headerNames) {
      if ("Time".equalsIgnoreCase(filterBomHeader(headerName))) {
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
        deviceAndMeasurementNames.putIfAbsent(deviceName, new ArrayList<>());
        deviceAndMeasurementNames.get(deviceName).add(measurementName);
      }
    }
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  private static TSDataType getType(String typeStr) {
    try {
      return TSDataType.valueOf(typeStr);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * if the data is aligned by time, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  @SuppressWarnings("squid:S3776")
  protected static void writeDataAlignedByTime(
      SessionPool sessionPool,
      List<String> headerNames,
      Stream<CSVRecord> records,
      String failedFilePath)
      throws IllegalPathException {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    if (headerTypeMap.isEmpty()) {
      queryType(sessionPool, devices, headerTypeMap, "Time");
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
        recordObj -> {
          if (Boolean.FALSE.equals(hasStarted.get())) {
            hasStarted.set(true);
          } else if (pointSize.get() >= batchPointSize) {
            writeAndEmptyDataSet(
                sessionPool, deviceIds, times, typesList, valuesList, measurementsList, 3);
            pointSize.set(0);
          }

          boolean isFail = false;

          for (Map.Entry<String, List<String>> entry : deviceAndMeasurementNames.entrySet()) {
            String deviceId = entry.getKey();
            List<String> measurementNames = entry.getValue();
            ArrayList<TSDataType> types = new ArrayList<>();
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<String> measurements = new ArrayList<>();
            for (String measurement : measurementNames) {
              String header = deviceId + "." + measurement;
              String value = recordObj.get(headerNameMap.get(header));
              if (!"".equals(value)) {
                TSDataType type;
                if (!headerTypeMap.containsKey(header)) {
                  type = typeInfer(value);
                  if (type != null) {
                    headerTypeMap.put(header, type);
                  } else {
                    ioTPrinter.printf(
                        "Line '%s', column '%s': '%s' unknown type%n",
                        recordObj.getRecordNumber(), header, value);
                    isFail = true;
                  }
                }
                type = headerTypeMap.get(header);
                if (type != null) {
                  Object valueTrans = typeTrans(value, type);
                  if (valueTrans == null) {
                    isFail = true;
                    ioTPrinter.printf(
                        "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                        recordObj.getRecordNumber(), header, value, type);
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
              times.add(parseTimestamp(recordObj.get(timeColumn)));
              deviceIds.add(deviceId);
              typesList.add(types);
              valuesList.add(values);
              measurementsList.add(measurements);
            }
          }
          if (isFail) {
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          }
        });
    if (!deviceIds.isEmpty()) {
      writeAndEmptyDataSet(
          sessionPool, deviceIds, times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }

    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    //    if (Boolean.TRUE.equals(hasStarted.get())) {
    //      ioTPrinter.println("Import completely!");
    //    } else {
    //      ioTPrinter.println("No records!");
    //    }
  }

  /**
   * if the data is aligned by device, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected static void writeDataAlignedByDevice(
      Session session, List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
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
        recordObj -> {
          // only run in first record
          if (deviceName.get() == null) {
            deviceName.set(recordObj.get(1));
          } else if (!Objects.equals(deviceName.get(), recordObj.get(1))) {
            // if device changed
            writeAndEmptyDataSet(
                session, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
            deviceName.set(recordObj.get(1));
            pointSize.set(0);
          } else if (pointSize.get() >= batchPointSize) {
            // insert a batch
            writeAndEmptyDataSet(
                session, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
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
            String value = recordObj.get(headerName);
            if (!"".equals(value)) {
              TSDataType type;
              // Get the data type directly if the CSV column have data type.
              if (!headerTypeMap.containsKey(headerNameWithoutType)) {
                boolean hasResult = false;
                // query the data type in iotdb
                if (!typeQueriedDevice.contains(deviceName.get())) {
                  if (headerTypeMap.isEmpty()) {
                    Set<String> devices = new HashSet<>();
                    devices.add(deviceName.get());
                    queryType(session, devices, headerTypeMap, deviceColumn);
                  }
                  typeQueriedDevice.add(deviceName.get());
                }
                type = typeInfer(value);
                if (type != null) {
                  headerTypeMap.put(headerNameWithoutType, type);
                } else {
                  ioTPrinter.printf(
                      "Line '%s', column '%s': '%s' unknown type%n",
                      recordObj.getRecordNumber(), headerNameWithoutType, value);
                  isFail.set(true);
                }
              }
              type = headerTypeMap.get(headerNameWithoutType);
              if (type != null) {
                Object valueTrans = typeTrans(value, type);
                if (valueTrans == null) {
                  isFail.set(true);
                  ioTPrinter.printf(
                      "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                      recordObj.getRecordNumber(), headerNameWithoutType, value, type);
                } else {
                  values.add(valueTrans);
                  measurements.add(headerNameWithoutType);
                  types.add(type);
                  pointSize.getAndIncrement();
                }
              }
            }
          }
          if (Boolean.TRUE.equals(isFail.get())) {
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          }
          if (!measurements.isEmpty()) {
            times.add(parseTimestamp(recordObj.get(timeColumn)));
            typesList.add(types);
            valuesList.add(values);
            measurementsList.add(measurements);
          }
        });
    if (!times.isEmpty()) {
      writeAndEmptyDataSet(
          session, deviceName.get(), times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    ioTPrinter.println("Import completely!");
  }

  /**
   * if the data is aligned by time, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  @SuppressWarnings("squid:S3776")
  protected static void writeDataAlignedByTime(
      Session session, List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    if (headerTypeMap.isEmpty()) {
      queryType(session, devices, headerTypeMap, "Time");
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
        recordObj -> {
          if (Boolean.FALSE.equals(hasStarted.get())) {
            hasStarted.set(true);
          } else if (pointSize.get() >= batchPointSize) {
            writeAndEmptyDataSet(
                session, deviceIds, times, typesList, valuesList, measurementsList, 3);
            pointSize.set(0);
          }

          boolean isFail = false;

          for (Map.Entry<String, List<String>> entry : deviceAndMeasurementNames.entrySet()) {
            String deviceId = entry.getKey();
            List<String> measurementNames = entry.getValue();
            ArrayList<TSDataType> types = new ArrayList<>();
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<String> measurements = new ArrayList<>();
            for (String measurement : measurementNames) {
              String header = deviceId + "." + measurement;
              String value = recordObj.get(headerNameMap.get(header));
              if (!"".equals(value)) {
                TSDataType type;
                if (!headerTypeMap.containsKey(header)) {
                  type = typeInfer(value);
                  if (type != null) {
                    headerTypeMap.put(header, type);
                  } else {
                    ioTPrinter.printf(
                        "Line '%s', column '%s': '%s' unknown type%n",
                        recordObj.getRecordNumber(), header, value);
                    isFail = true;
                  }
                }
                type = headerTypeMap.get(header);
                if (type != null) {
                  Object valueTrans = typeTrans(value, type);
                  if (valueTrans == null) {
                    isFail = true;
                    ioTPrinter.printf(
                        "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                        recordObj.getRecordNumber(), header, value, type);
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
              times.add(parseTimestamp(recordObj.get(timeColumn)));
              deviceIds.add(deviceId);
              typesList.add(types);
              valuesList.add(values);
              measurementsList.add(measurements);
            }
          }
          if (isFail) {
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          }
        });
    if (!deviceIds.isEmpty()) {
      writeAndEmptyDataSet(session, deviceIds, times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }

    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
    if (Boolean.TRUE.equals(hasStarted.get())) {
      ioTPrinter.println("Import completely!");
    } else {
      ioTPrinter.println("No records!");
    }
  }

  protected static String filterBomHeader(String s) {
    byte[] bom = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    byte[] bytes = Arrays.copyOf(s.getBytes(), 3);
    if (Arrays.equals(bom, bytes)) {
      return s.substring(1);
    }
    return s;
  }

  protected static Options createHelpOptions() {
    final Options options = new Options();
    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg()
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opFileType =
        Option.builder(FILE_TYPE_ARGS)
            .longOpt(FILE_TYPE_NAME)
            .argName(FILE_TYPE_ARGS_NAME)
            .hasArg()
            .desc("Export file type ?You can choose tsfile)、csv) or sql) . (required)")
            .build();
    options.addOption(opFileType);
    return options;
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return CSVParser csv parser
   * @throws IOException when reading the csv file failed.
   */
  protected static CSVParser readCsvFile(String path) throws IOException {
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
   * write data to CSV file.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param filePath the directory to save the file
   */
  public static Boolean writeCsvFile(
      List<String> headerNames, List<List<Object>> records, String filePath) {
    try {
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(filePath);
      if (headerNames != null) {
        csvPrinterWrapper.printRecord(headerNames);
      }
      for (List<Object> CsvRecord : records) {
        csvPrinterWrapper.printRecord(CsvRecord);
      }
      csvPrinterWrapper.flush();
      csvPrinterWrapper.close();
      return true;
    } catch (IOException e) {
      ioTPrinter.printException(e);
      return false;
    }
  }

  static class CSVPrinterWrapper {
    private final String filePath;
    private final CSVFormat csvFormat;
    private CSVPrinter csvPrinter;

    public CSVPrinterWrapper(String filePath) {
      this.filePath = filePath;
      this.csvFormat =
          CSVFormat.Builder.create(CSVFormat.DEFAULT)
              .setHeader()
              .setSkipHeaderRecord(true)
              .setEscape('\\')
              .setQuoteMode(QuoteMode.NONE)
              .build();
    }

    public void printRecord(final Iterable<?> values) throws IOException {
      if (csvPrinter == null) {
        csvPrinter = csvFormat.print(new PrintWriter(filePath));
      }
      csvPrinter.printRecord(values);
    }

    public void print(Object value) {
      if (csvPrinter == null) {
        try {
          csvPrinter = csvFormat.print(new PrintWriter(filePath));
        } catch (IOException e) {
          ioTPrinter.printException(e);
          return;
        }
      }
      try {
        csvPrinter.print(value);
      } catch (IOException e) {
        ioTPrinter.printException(e);
      }
    }

    public void println() throws IOException {
      csvPrinter.println();
    }

    public void close() throws IOException {
      if (csvPrinter != null) {
        csvPrinter.close();
      }
    }

    public void flush() throws IOException {
      if (csvPrinter != null) {
        csvPrinter.flush();
      }
    }
  }
}
