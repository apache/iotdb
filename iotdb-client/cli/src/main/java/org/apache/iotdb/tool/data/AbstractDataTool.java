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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.ImportTsFileOperation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.jline.reader.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.LocalDate;
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

  protected static String host;
  protected static String port;
  protected static String table;
  protected static String endTime;
  protected static String username;
  protected static String password;
  protected static Boolean useSsl;
  protected static String trustStore;
  protected static String trustStorePwd;
  protected static Boolean aligned;
  protected static String database;
  protected static String startTime;
  protected static int threadNum = 8;
  protected static int rpcMaxFrameSize = 536870912;
  protected static String targetPath;
  protected static long timeout = Long.MAX_VALUE;
  protected static String timeZoneID;
  protected static String timeFormat;
  protected static String exportType;
  protected static String queryCommand;
  public static String fileType = null;
  public static String failDir = "fail/";
  protected static String targetDirectory;
  public static String timeColumn = "Time";
  protected static int linesPerFile = 10000;
  public static String deviceColumn = "Device";
  protected static boolean isRemoteLoad = true;
  protected static Boolean needDataTypePrinted;
  protected static int batchPointSize = 100_000;
  protected static boolean sqlDialectTree = true;
  protected static int linesPerFailedFile = 10000;
  protected static String successDir = "success/";
  protected static String timestampPrecision = "ms";
  protected static String failedFileDirectory = null;
  protected static ImportTsFileOperation failOperation;
  protected static ZoneId zoneId = ZoneId.systemDefault();
  protected static ImportTsFileOperation successOperation;
  protected static String targetFile = Constants.DUMP_FILE_NAME_DEFAULT;
  protected static final int updateTimeInterval = 2000;
  protected static final LongAdder loadFileFailedNum = new LongAdder();
  protected static final LongAdder loadFileSuccessfulNum = new LongAdder();
  protected static final LongAdder processingLoadFailedFileSuccessfulNum = new LongAdder();
  protected static final LongAdder processingLoadSuccessfulFileSuccessfulNum = new LongAdder();

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

  protected static void parseBasicParams(CommandLine commandLine)
      throws ArgsErrorException, IOException {
    host =
        checkRequiredArg(
            Constants.HOST_ARGS, Constants.HOST_NAME, commandLine, Constants.HOST_DEFAULT_VALUE);
    port =
        checkRequiredArg(
            Constants.PORT_ARGS, Constants.PORT_NAME, commandLine, Constants.PORT_DEFAULT_VALUE);
    username =
        checkRequiredArg(
            Constants.USERNAME_ARGS,
            Constants.USERNAME_NAME,
            commandLine,
            Constants.USERNAME_DEFAULT_VALUE);
    CliContext cliCtx = new CliContext(System.in, System.out, System.err, ExitType.SYSTEM_EXIT);
    LineReader lineReader = JlineUtils.getLineReader(cliCtx, username, host, port);
    cliCtx.setLineReader(lineReader);
    String useSslStr = commandLine.getOptionValue(Constants.USE_SSL_ARGS);
    useSsl = Boolean.parseBoolean(useSslStr);
    if (useSsl) {
      String givenTS = commandLine.getOptionValue(Constants.TRUST_STORE_ARGS);
      if (givenTS != null) {
        trustStore = givenTS;
      } else {
        trustStore = cliCtx.getLineReader().readLine("please input your trust_store:", '\0');
      }
      String givenTPW = commandLine.getOptionValue(Constants.TRUST_STORE_PWD_ARGS);
      if (givenTPW != null) {
        trustStorePwd = givenTPW;
      } else {
        trustStorePwd = cliCtx.getLineReader().readLine("please input your trust_store_pwd:", '\0');
      }
    }
    boolean hasPw = commandLine.hasOption(Constants.PW_ARGS);
    if (hasPw) {
      String inputPassword = commandLine.getOptionValue(Constants.PW_ARGS);
      if (inputPassword != null) {
        password = inputPassword;
      } else {
        password = cliCtx.getLineReader().readLine("please input your password:", '\0');
      }
    } else {
      password = Constants.PW_DEFAULT_VALUE;
    }
  }

  protected static void printHelpOptions(
      String cmdLineHead,
      String cmdLineSyntax,
      HelpFormatter hf,
      Options tsFileOptions,
      Options csvOptions,
      Options sqlOptions,
      boolean printFileType) {
    ioTPrinter.println(
        Constants.TSFILEDB_CLI_DIVIDE
            + "\n"
            + cmdLineSyntax
            + "\n"
            + Constants.TSFILEDB_CLI_DIVIDE);
    if (StringUtils.isNotBlank(cmdLineHead)) {
      ioTPrinter.println(cmdLineHead);
    }
    final String usageName = cmdLineSyntax.replaceAll(" ", "");
    if (ObjectUtils.isNotEmpty(tsFileOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n'
                + Constants.FILE_TYPE_NAME
                + Constants.COLON
                + Constants.TSFILE_SUFFIXS
                + '\n'
                + Constants.TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, tsFileOptions, true);
    }
    if (ObjectUtils.isNotEmpty(csvOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n'
                + Constants.FILE_TYPE_NAME
                + Constants.COLON
                + Constants.CSV_SUFFIXS
                + '\n'
                + Constants.TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, csvOptions, true);
    }
    if (ObjectUtils.isNotEmpty(sqlOptions)) {
      if (printFileType) {
        ioTPrinter.println(
            '\n'
                + Constants.FILE_TYPE_NAME
                + Constants.COLON
                + Constants.SQL_SUFFIXS
                + '\n'
                + Constants.TSFILEDB_CLI_DIVIDE);
      }
      hf.printHelp(usageName, sqlOptions, true);
    }
  }

  protected static boolean checkTimeFormat() {
    for (String format : Constants.TIME_FORMAT) {
      if (timeFormat.equals(format)) {
        return true;
      }
    }
    for (String format : Constants.STRING_TIME_FORMAT) {
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
          ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSet(
            session, deviceIds, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
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

  protected static void writeFailedLinesFile(
      List<String> headerNames, String failedFilePath, List<List<Object>> failedRecords) {
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
  protected static TSDataType typeInfer(String strValue) {
    if (strValue.contains("\"")) {
      return strValue.length() <= 512 + 2 ? STRING : TEXT;
    }
    if (isBoolean(strValue)) {
      return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_BOOLEAN);
    } else if (isTimeStamp(strValue)) {
      return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_TIMESTAMP);
    } else if (isNumber(strValue)) {
      if (!strValue.contains(TsFileConstant.PATH_SEPARATOR)) {
        if (isConvertFloatPrecisionLack(StringUtils.trim(strValue))) {
          return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_LONG);
        }
        return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_INT);
      } else {
        return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_FLOAT);
      }
    } else if (Constants.DATATYPE_NULL.equals(strValue)
        || Constants.DATATYPE_NULL.toUpperCase().equals(strValue)) {
      return null;
      // "NaN" is returned if the NaN Literal is given in Parser
    } else if (Constants.DATATYPE_NAN.equals(strValue)) {
      return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_NAN);
    } else if (isDate(strValue)) {
      return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_DATE);
    } else if (isBlob(strValue)) {
      return Constants.TYPE_INFER_KEY_DICT.get(Constants.DATATYPE_BLOB);
    } else if (strValue.length() <= 512) {
      return STRING;
    } else {
      return TEXT;
    }
  }

  private static boolean isDate(String s) {
    return s.equalsIgnoreCase(Constants.DATATYPE_DATE);
  }

  private static boolean isTimeStamp(String s) {
    return s.equalsIgnoreCase(Constants.DATATYPE_TIMESTAMP);
  }

  static boolean isNumber(String s) {
    if (s == null || s.equals(Constants.DATATYPE_NAN)) {
      return false;
    }
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isBlob(String s) {
    return s.length() >= 3 && s.startsWith("X'") && s.endsWith("'");
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
  protected static Object typeTrans(String value, TSDataType type) {
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
          return Long.parseLong(value);
        case DATE:
          return LocalDate.parse(value);
        case BLOB:
          return new Binary(parseHexStringToByteArray(value.replaceFirst("0x", "")));
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static byte[] parseHexStringToByteArray(String hexString) {
    byte[] bytes = new byte[hexString.length() / 2];
    for (int i = 0; i < hexString.length(); i += 2) {
      int value = Integer.parseInt(hexString.substring(i, i + 2), 16);
      bytes[i / 2] = (byte) value;
    }
    return bytes;
  }

  protected static long parseTimestamp(String str) {
    long timestamp;
    try {
      timestamp = Long.parseLong(str);
    } catch (NumberFormatException e) {
      timestamp = DateTimeUtils.convertDatetimeStrToLong(str, zoneId, timestampPrecision);
    }
    return timestamp;
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  protected static TSDataType getType(String typeStr) {
    try {
      return TSDataType.valueOf(typeStr);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * return the ColumnCategory
   *
   * @param typeStr
   * @return
   */
  protected static ColumnCategory getColumnCategory(String typeStr) {
    if (StringUtils.isNotBlank(typeStr)) {
      try {
        return ColumnCategory.valueOf(typeStr);
      } catch (Exception e) {
        return null;
      }
    }
    return null;
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

    List<List<Object>> failedRecords = new ArrayList<>();

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
          ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSet(
            session, device, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
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
      SessionDataSet sessionDataSet;
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

  @SuppressWarnings(
      "squid:S135") // ignore for loops should not contain more than a single "break" or "continue"
  // statement
  protected static void parseHeaders(
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

  protected static String filterBomHeader(String s) {
    byte[] bom = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    byte[] bytes = Arrays.copyOf(s.getBytes(), 3);
    if (Arrays.equals(bom, bytes)) {
      return s.substring(1);
    }
    return s;
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
