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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SuppressWarnings("squid:S106")
public class Xianyi {

  private static Session session;
  public static final String prefix = "root.SC.JZG.00.";
  public static final String device = "BHE"; // BHE, BHN, BHZ
  public static final String measurement = "D";
  public static final String path = prefix + device + "." + measurement;

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder().host(LOCAL_HOST).port(6667).username("root").password("root").build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    try {
      session.setStorageGroup("root.SC");
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw e;
      }
    }

    if (!session.checkTimeseriesExists(path)) {
      session.createTimeseries(path, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.GZIP);
    }
    try {
      insertTablet();
    } catch (ParseException | IOException e) {
      e.printStackTrace();
    }

    session.close();
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet()
      throws IoTDBConnectionException, StatementExecutionException, ParseException, IOException {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema(measurement, TSDataType.INT32));

    Tablet tablet = new Tablet(prefix + device, schemaList, 100);

    for (int fileId = 1; fileId <= 30; fileId++) {
      File file =
          new File(
              "/Users/samperson1997/git/iotdb/data/2017/SC/JZG/"
                  + measurement
                  + ".D1/SC.JZG.00."
                  + measurement
                  + ".D.2017."
                  + generateIndexString(fileId)
                  + ".txt");
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      BufferedReader reader = new BufferedReader(new FileReader(file));

      String tempString;
      int idx = 0;
      long startTime = 0L;

      while ((tempString = reader.readLine()) != null) {
        tempString = tempString.trim();
        if (tempString.startsWith("TraceID")
            || tempString.startsWith("DATA")
            || tempString.startsWith("Record")
            || tempString.startsWith("RECORD")
            || tempString.startsWith("Start")) {
          continue;
        }
        if (tempString.startsWith("Segment")) {
          String dateStr = tempString.split(" ")[1];
          Date date = sdf.parse(dateStr.substring(0, 10) + " " + dateStr.substring(11, 19));
          startTime = date.getTime();
          idx = 0;
          continue;
        }
        String[] values = tempString.split(" ");
        for (String s : values) {
          if (s.equals("")) {
            continue;
          }
          long time = startTime + idx * 10;
          int value = Integer.parseInt(s);
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, time);
          tablet.addValue(measurement, rowIndex, value);
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            session.insertTablet(tablet, true);
            tablet.reset();
          }
          idx++;
        }
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static String generateIndexString(int curIndex) {
    StringBuilder res = new StringBuilder(String.valueOf(curIndex));
    String target = String.valueOf(100);
    while (res.length() < target.length()) {
      res.insert(0, "0");
    }
    return res.toString();
  }
}
