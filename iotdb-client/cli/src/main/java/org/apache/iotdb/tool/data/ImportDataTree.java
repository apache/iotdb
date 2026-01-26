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
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportDataTree extends AbstractImportData {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static SessionPool sessionPool;

  public void init()
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException {
    SessionPool.Builder sessionPoolBuilder =
        new SessionPool.Builder()
            .host(host)
            .port(Integer.parseInt(port))
            .user(username)
            .password(password)
            .maxSize(threadNum + 1)
            .enableIoTDBRpcCompression(false)
            .enableRedirection(false)
            .enableAutoFetch(false);
    if (useSsl) {
      sessionPoolBuilder =
          sessionPoolBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    sessionPool = sessionPoolBuilder.build();
    sessionPool.setEnableQueryRedirection(false);
    if (timeZoneID != null) {
      sessionPool.setTimeZone(timeZoneID);
      zoneId = sessionPool.getZoneId();
    }
    final File file = new File(targetPath);
    if (!file.isFile() && !file.isDirectory()) {
      ioTPrinter.println(String.format("Source file or directory %s does not exist", targetPath));
      System.exit(Constants.CODE_ERROR);
    }
    if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
      ImportTsFileScanTool.setSourceFullPath(targetPath);
      ImportTsFileScanTool.traverseAndCollectFiles();
      ImportTsFileScanTool.addNoResourceOrModsToQueue();
    } else {
      ImportDataScanTool.setSourceFullPath(targetPath);
      ImportDataScanTool.traverseAndCollectFiles();
    }
  }

  @Override
  protected Runnable getAsyncImportRunnable() {
    return new ImportDataTree(); // 返回子类1的Runnable对象
  }

  @SuppressWarnings("java:S2259")
  protected void importFromSqlFile(File file) {
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    String failedFilePath;
    if (failedFileDirectory == null) {
      failedFilePath = file.getAbsolutePath() + ".failed";
    } else {
      failedFilePath = failedFileDirectory + file.getName() + ".failed";
    }
    try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
      String sql;
      while ((sql = br.readLine()) != null) {
        try {
          sessionPool.executeNonQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          failedRecords.add(Collections.singletonList(sql));
        }
      }
      processSuccessFile(null);
    } catch (IOException e) {
      ioTPrinter.println("SQL file read exception because: " + e.getMessage());
    }
    if (!failedRecords.isEmpty()) {
      try (FileWriter writer = new FileWriter(failedFilePath)) {
        for (List<Object> failedRecord : failedRecords) {
          writer.write(failedRecord.get(0).toString() + "\n");
        }
      } catch (IOException e) {
        ioTPrinter.println("Cannot dump fail result because: " + e.getMessage());
      }
    }
  }

  protected void importFromTsFile(File file) {
    final String sql = "load '" + file + "' onSuccess=none ";
    try {
      sessionPool.executeNonQueryStatement(sql);
      processSuccessFile(file.getPath());
    } catch (final Exception e) {
      processFailFile(file.getPath(), e);
    }
  }

  protected void importFromCsvFile(File file) {
    if (file.getName().endsWith(Constants.CSV_SUFFIXS)
        || file.getName().endsWith(Constants.TXT_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          ioTPrinter.println("Empty file!");
          return;
        }
        if (!timeColumn.equalsIgnoreCase(filterBomHeader(headerNames.get(0)))) {
          ioTPrinter.println("The first field of header must be `Time`!");
          return;
        }
        String failedFilePath;
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
        processSuccessFile(null);
      } catch (IOException | IllegalPathException e) {
        ioTPrinter.println("CSV file read exception because: " + e.getMessage());
      }
    } else {
      ioTPrinter.println("The file name must end with \"csv\" or \"txt\"!");
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
      List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    if (headerTypeMap.isEmpty()) {
      queryType(devices, headerTypeMap, "Time");
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
            writeAndEmptyDataSet(deviceIds, times, typesList, valuesList, measurementsList, 3);
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
                  queryType(header, headerTypeMap);
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
      writeAndEmptyDataSet(deviceIds, times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }

    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected static void writeDataAlignedByDevice(
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

    List<List<Object>> failedRecords = new ArrayList<>();

    records.forEach(
        recordObj -> {
          // only run in first record
          if (deviceName.get() == null) {
            deviceName.set(recordObj.get(1));
          } else if (!Objects.equals(deviceName.get(), recordObj.get(1))) {
            // if device changed
            writeAndEmptyDataSet(
                deviceName.get(), times, typesList, valuesList, measurementsList, 3);
            deviceName.set(recordObj.get(1));
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
            String value = recordObj.get(headerName);
            if (!"".equals(value)) {
              TSDataType type;
              if (!headerTypeMap.containsKey(headerNameWithoutType)) {
                // query the data type in iotdb
                if (!typeQueriedDevice.contains(deviceName.get())) {
                  if (headerTypeMap.isEmpty()) {
                    Set<String> devices = new HashSet<>();
                    devices.add(deviceName.get());
                    queryType(devices, headerTypeMap, deviceColumn);
                  }
                  typeQueriedDevice.add(deviceName.get());
                }
                if (!headerTypeMap.containsKey(headerNameWithoutType)) {
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
      writeAndEmptyDataSet(deviceName.get(), times, typesList, valuesList, measurementsList, 3);
      pointSize.set(0);
    }
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
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
      if (Boolean.FALSE.equals(aligned)) {
        sessionPool.insertRecordsOfOneDevice(
            device, times, measurementsList, typesList, valuesList);
      } else {
        sessionPool.insertAlignedRecordsOfOneDevice(
            device, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(device, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
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
      if (Boolean.FALSE.equals(aligned)) {
        sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } else {
        sessionPool.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
      }
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(
            deviceIds, times, typesList, valuesList, measurementsList, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } finally {
      deviceIds.clear();
      times.clear();
      typesList.clear();
      valuesList.clear();
      measurementsList.clear();
    }
  }

  private static void queryType(
      Set<String> deviceNames, HashMap<String, TSDataType> headerTypeMap, String alignedType) {
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

  private static void queryType(String series, HashMap<String, TSDataType> headerTypeMap) {
    String sql = "show timeseries " + series;
    try (SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement(sql)) {
      int tsIndex = sessionDataSetWrapper.getColumnNames().indexOf(ColumnHeaderConstant.TIMESERIES);
      int dtIndex = sessionDataSetWrapper.getColumnNames().indexOf(ColumnHeaderConstant.DATATYPE);
      while (sessionDataSetWrapper.hasNext()) {
        RowRecord rowRecord = sessionDataSetWrapper.next();
        List<Field> fields = rowRecord.getFields();
        String timeseries = fields.get(tsIndex).getStringValue();
        String dataType = fields.get(dtIndex).getStringValue();
        if (Objects.equals(series, timeseries)) {
          headerTypeMap.put(timeseries, getType(dataType));
        }
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      ioTPrinter.println("Meet error when query the type of timeseries because " + e.getMessage());
      System.exit(1);
    }
  }
}
