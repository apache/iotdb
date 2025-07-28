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

package org.apache.iotdb;

import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings({"squid:S106"})
public class TabletExample {

  private static final String TIME_STR = "time";

  /**
   * load csv data.
   *
   * @param measureTSTypeInfos key: measurement name, value: measurement data type
   * @param dataFileName the csv file name to load
   * @return key: measurement name, value: series in format of {@link ArrayList}
   * @throws IOException if the csv format is incorrect
   */
  private static Map<String, ArrayList<Object>> loadCSVData(
      Map<String, TSDataType> measureTSTypeInfos, String dataFileName) throws IOException {
    measureTSTypeInfos.put(TIME_STR, TSDataType.INT64);
    try (BufferedReader reader = new BufferedReader(new FileReader(dataFileName))) {
      String headline = reader.readLine();
      if (headline == null) {
        throw new IOException("Given csv data file has not headers");
      }
      // check the csv file format
      String[] fileColumns = headline.split(",");
      Map<String, Integer> columnToIdMap = new HashMap<>();
      for (int col = 0; col < fileColumns.length; col++) {
        String columnName = fileColumns[col];
        if (columnToIdMap.containsKey(columnName)) {
          throw new IOException(
              String.format("csv file contains duplicate columns: %s", columnName));
        }
        columnToIdMap.put(columnName, col);
      }
      Map<String, ArrayList<Object>> ret = new HashMap<>();
      // make sure that all measurements can be found from the data file
      for (Entry<String, TSDataType> entry : measureTSTypeInfos.entrySet()) {
        String measurement = entry.getKey();
        if (!columnToIdMap.containsKey(entry.getKey())) {
          throw new IOException(String.format("measurement %s's is not in csv file.", measurement));
        } else {
          ret.put(measurement, new ArrayList<>());
        }
      }

      String line;
      while ((line = reader.readLine()) != null) {
        String[] items = line.split(",");
        for (Entry<String, TSDataType> entry : measureTSTypeInfos.entrySet()) {
          String measurement = entry.getKey();
          TSDataType dataType = entry.getValue();
          int idx = columnToIdMap.get(measurement);
          switch (dataType) {
            case BOOLEAN:
              ret.get(measurement).add(Boolean.parseBoolean(items[idx]));
              break;
            case DATE:
            case INT32:
              ret.get(measurement).add(Integer.parseInt(items[idx]));
              break;
            case TIMESTAMP:
            case INT64:
              ret.get(measurement).add(Long.parseLong(items[idx]));
              break;
            case FLOAT:
              ret.get(measurement).add(Float.parseFloat(items[idx]));
              break;
            case DOUBLE:
              ret.get(measurement).add(Double.parseDouble(items[idx]));
              break;
            case STRING:
            case BLOB:
            case TEXT:
              ret.get(measurement).add(BytesUtils.valueOf(items[idx]));
              break;
            case VECTOR:
              throw new IOException(String.format("data type %s is not yet.", TSDataType.VECTOR));
            default:
              throw new IOException("no type");
          }
        }
      }
      return ret;
    } finally {
      measureTSTypeInfos.remove(TIME_STR);
    }
  }

  /**
   * Read csv file and insert tablet to IoTDB
   *
   * @param args: arg(with default value): arg0: dataFileName(sample.csv), arg1: rowSize(10000),
   *     arg2: colSize(5000).
   */
  public static void main(String[] args) throws Exception {

    try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {
      session.open();
      String dataFileName = "sample.csv";
      int rowSize = 10000;
      int colSize = 5000;
      if (args.length > 1) {
        dataFileName = args[0];
      }
      if (args.length > 2) {
        rowSize = Integer.parseInt(args[1]);
      }
      if (args.length > 3) {
        colSize = Integer.parseInt(args[2]);
      }

      // construct the tablet's measurements.
      Map<String, TSDataType> measureTSTypeInfos = new HashMap<>();
      measureTSTypeInfos.put("s0", TSDataType.BOOLEAN);
      measureTSTypeInfos.put("s1", TSDataType.FLOAT);
      measureTSTypeInfos.put("s2", TSDataType.INT32);
      measureTSTypeInfos.put("s3", TSDataType.DOUBLE);
      measureTSTypeInfos.put("s4", TSDataType.INT64);
      measureTSTypeInfos.put("s5", TSDataType.TEXT);
      List<IMeasurementSchema> schemas = new ArrayList<>();
      measureTSTypeInfos.forEach((mea, type) -> schemas.add(new MeasurementSchema(mea, type)));

      System.out.println(
          String.format(
              "Test Java: csv file name: %s, row: %d, col: %d", dataFileName, rowSize, colSize));
      System.out.println(String.format("Total points: %d", rowSize * colSize * schemas.size()));

      // test start
      long allStart = System.nanoTime();

      Map<String, ArrayList<Object>> data = loadCSVData(measureTSTypeInfos, dataFileName);
      long loadCost = System.nanoTime() - allStart;

      long insertCost = 0;
      for (int i = 0; i < colSize; i++) {
        String deviceId = "root.sg" + i % 8 + "." + i;

        Tablet ta = new Tablet(deviceId, schemas, rowSize);
        for (int t = 0; t < rowSize; t++) {
          ta.addTimestamp(t, (Long) data.get(TIME_STR).get(t));
          for (Entry<String, TSDataType> entry : measureTSTypeInfos.entrySet()) {
            String mea = entry.getKey();
            ta.addValue(mea, t, data.get(mea).get(t));
          }
        }
        long insertSt = System.nanoTime();
        session.insertTablet(ta, false);
        insertCost += (System.nanoTime() - insertSt);
      }
      // test end
      long allEnd = System.nanoTime();

      session.executeNonQueryStatement("delete timeseries root.*");

      System.out.println(String.format("load cost: %.3f", ((float) loadCost / 1000_000_000)));
      System.out.println(
          String.format(
              "construct tablet cost: %.3f",
              ((float) (allEnd - allStart - insertCost - loadCost) / 1000_000_000)));
      System.out.println(
          String.format("insert tablet cost: %.3f", ((float) insertCost / 1000_000_000)));
      System.out.println(
          String.format("total cost: %.3f", ((float) (allEnd - allStart) / 1000_000_000)));
      System.out.println(String.format("%.3f", ((float) loadCost / 1000_000_000)));
    }
  }
}
