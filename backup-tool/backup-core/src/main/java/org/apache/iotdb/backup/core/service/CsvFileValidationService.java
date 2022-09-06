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
package org.apache.iotdb.backup.core.service;

import org.apache.iotdb.backup.core.exception.FileTransFormationException;
import org.apache.iotdb.backup.core.model.ValidationType;
import org.apache.iotdb.backup.core.parse.CsvFileTransParser;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvFileValidationService implements FileValidationService {

  @Override
  public void dataValidateWithServer(
      String path, Session session, String charset, ValidationType type) throws Exception {
    File fi = validateFilePath(path);
    long size = fi.length();
    if (size >= 200 * 1024 * 1024) {
      throw new FileTransFormationException("the max supported file size is 200MB");
    }
    if (!fi.getName().endsWith(".csv")) {
      throw new FileTransFormationException("given file is not a csv file");
    }

    CsvFileTransParser parser = new CsvFileTransParser(fi, charset);
    List<CSVRecord> allList = new ArrayList<>();
    List<CSVRecord> batchList = new ArrayList();
    try {
      while ((batchList = parser.nextBatchRecords(10000)).size() != 0) {
        allList.addAll(batchList);
      }
    } catch (Exception e) {

    } finally {
      parser.close();
    }
    if (allList.size() == 0) {
      return;
    }
    List<String> timeseries = new ArrayList<>();
    Map<String, String> headMap = new HashMap<>();
    getTimeseries(parser, allList.get(0), timeseries, headMap);
    doDataValidation(allList, timeseries, headMap, session, type);
  }

  public void doDataValidation(
      List<CSVRecord> allList,
      List<String> timeseries,
      Map<String, String> headMap,
      Session session,
      ValidationType type)
      throws StatementExecutionException, IoTDBConnectionException {
    Map<Long, CSVRecord> csvMap = new HashMap<>();
    csvMap =
        allList.stream()
            .collect(
                Collectors.toMap(
                    c -> {
                      if (c.get("Time") != null && !"".equals(c.get("Time"))) {
                        return CsvFileTransParser.formatToTimestamp(c.get("Time"));
                      }
                      return 0L;
                    },
                    Function.identity()));

    long end =
        csvMap.keySet().stream()
            .max(
                (o1, o2) -> {
                  if (o1 > o2) {
                    return 1;
                  } else if (o1 < o2) {
                    return -1;
                  } else {
                    return 0;
                  }
                })
            .get();

    long begin =
        csvMap.keySet().stream()
            .min(
                (o1, o2) -> {
                  if (o1 > o2) {
                    return 1;
                  } else if (o1 < o2) {
                    return -1;
                  } else {
                    return 0;
                  }
                })
            .get();

    end += 1;
    doValidateData(csvMap, begin, end, timeseries, headMap, session, type);
  }

  protected void doValidateData(
      Map<Long, CSVRecord> recordMap,
      Long begin,
      Long end,
      List<String> timeseries,
      Map<String, String> headMap,
      Session session,
      ValidationType validationType)
      throws StatementExecutionException, IoTDBConnectionException {
    int dataSetCount = 0;
    SessionDataSet dataSet = getSessionDataSet(timeseries, begin, end, session);
    List<String> typeList = dataSet.getColumnTypes();
    List<String> columnNameList = dataSet.getColumnNames();
    for (int i = 0; i < timeseries.size(); i++) {
      if (!timeseries.get(i).equals(columnNameList.get(i + 1))) {
        assert false
            : "datavalidation failed, because the num of timeseries in the csv is more than the num of the database; the timeseries is :"
                + timeseries.get(i);
      }
    }
    if (columnNameList.indexOf("Time") == 0) {
      columnNameList.remove(0);
      typeList.remove(0);
    }

    while (dataSet.hasNext()) {
      dataSetCount++;
      RowRecord sdataRecord = dataSet.next();
      if (recordMap.get(sdataRecord.getTimestamp()) != null) {
        CSVRecord csvRecord = recordMap.get(sdataRecord.getTimestamp());
        for (int i = 0; i < columnNameList.size(); i++) {
          StringBuilder validateFailedMsg = new StringBuilder();
          String cname = columnNameList.get(i);
          String ctype = typeList.get(i);
          Field field = sdataRecord.getFields().get(i);
          assert compare(csvRecord.get(headMap.get(cname)), field, ctype)
              : validateFailedMsg
                  .append("\n data validation failed; Time：")
                  .append(sdataRecord.getTimestamp())
                  .append(",\ntype:")
                  .append(ctype)
                  .append(",\ncloumn name:")
                  .append(cname)
                  .append(",\nexpected value:")
                  .append(csvRecord.get(headMap.get(cname)))
                  .append(",actural value :[")
                  .append(field.getStringValue())
                  .append("]")
                  .toString();
          recordMap.remove(sdataRecord.getTimestamp());
        }
      } else {
        if (validationType.equals(ValidationType.EQUAL)) {
          throw new AssertionError(
              " data validation failed, the num of the cvs file is not equals the nums in the database;Time:"
                  + sdataRecord.getTimestamp());
        }
        continue;
      }
    }
    if (recordMap.size() != 0) {
      throw new AssertionError(
          " data validation failed, the num of the cvs file more than the nums in the database;");
    }
    if (dataSetCount == 0) {
      throw new AssertionError(" data validation failed, dataSetCount is 0");
    }
  }

  protected SessionDataSet getSessionDataSet(
      List<String> timeSeries, Long begin, Long end, Session session)
      throws StatementExecutionException, IoTDBConnectionException {
    return session.executeRawDataQuery(timeSeries, begin, end + 1);
  }

  private void getTimeseries(
      CsvFileTransParser parser,
      CSVRecord record,
      List<String> timeseries,
      Map<String, String> headMap) {

    List<String> headerNames = parser.csvParser.getHeaderNames();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parser.parseHeaders(headerNames, headerTypeMap, headerNameMap);

    String timeSerie = null;

    if (!headerNames.contains("Device")) {
      String s = headerNameMap.get(headerNames.get(1));
      timeSerie = s.substring(0, s.lastIndexOf("."));
    }

    if (headerNames.contains("Device")) {
      timeSerie = record.get("Device") + ".";
    }
    for (String headerNameFull : headerNames) {
      String headerName = headerNameMap.get(headerNameFull);
      if (headerNameFull.equals("Device")) {
        continue;
      }
      if (!headerNameFull.equals("Time")) {
        String buff =
            timeSerie
                + headerName.substring(
                    headerName.lastIndexOf(".") == -1 ? 0 : headerName.lastIndexOf("."),
                    headerName.length());
        timeseries.add(buff);
        headMap.put(buff, headerNameFull);
      }
    }
  }

  private boolean hasDeviceColumn(CSVRecord record) {
    try {
      record.get("Device");
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static boolean compare(String recordValue, Field field, String type)
      throws StatementExecutionException {
    // csv导入的规则
    if ((recordValue == null || "".equals(recordValue))
        && field.getObjectValue(TSDataType.valueOf(type)) == null) {
      return true;
    }
    switch (type) {
      case "TEXT":
        if (recordValue.startsWith("\"") && recordValue.endsWith("\"")) {
          recordValue = recordValue.substring(1, recordValue.length() - 1);
        }
        if ((field.getStringValue() == null) && (recordValue == null || "".equals(recordValue))) {
          return true;
        }
        return recordValue.equals(field.getStringValue());
      case "BOOLEAN":
        if (!"true".equals(recordValue) && !"false".equals(recordValue)) {
          recordValue = null;
        }
        if (field.getDataType() == null && recordValue == null) {
          return true;
        }
        return Boolean.valueOf(recordValue).equals(field.getBoolV());
      case "INT32":
        return Integer.valueOf(recordValue) == field.getIntV();
      case "INT64":
        return Long.valueOf(recordValue) == field.getLongV();
      case "FLOAT":
        return Float.valueOf(recordValue) == field.getFloatV();
      case "DOUBLE":
        return Double.valueOf(recordValue) == field.getDoubleV();
      default:
        return recordValue.equals(field.getObjectValue(TSDataType.valueOf(type)));
    }
  }
}
