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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * This example shows how to insert and select Hybrid Timeseries by session Hybrid Timeseries
 * includes Aligned Timeseries and Normal Timeseries
 */
public class HybridTimeseriesSessionExample {

  private static Session session;
  private static final String ROOT_SG1_ALIGNEDDEVICE = "root.sg_1.aligned_device";
  private static final String ROOT_SG1_D1 = "root.sg_1.d1";
  private static final String ROOT_SG1_D2 = "root.sg_1.d2";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    insertRecord(ROOT_SG1_D2, 0, 100);
    insertTabletWithAlignedTimeseriesMethod(0, 100);
    insertRecord(ROOT_SG1_D1, 0, 100);
    session.executeNonQueryStatement("flush");
    selectTest();

    session.close();
  }

  private static void selectTest() throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select ** from root.sg_1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }
  /** Method 1 for insert tablet with aligned timeseries */
  private static void insertTabletWithAlignedTimeseriesMethod(int minTime, int maxTime)
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_ALIGNEDDEVICE, schemaList);
    long timestamp = minTime;

    for (long row = minTime; row < maxTime; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, row * 10 + 1L);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (int) (row * 10 + 2));

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertRecord(String deviceId, int minTime, int maxTime)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s2");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = minTime; time < maxTime; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }
}
