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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("squid:S106")
public class VectorSessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1 = "root.sg_1.d1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    createTemplate();
    insertTabletWithAlignedTimeseriesMethod1();
    insertTabletWithAlignedTimeseriesMethod2();

    insertNullableTabletWithAlignedTimeseries();
    selectTest();
    selectWithValueFilterTest();

    selectWithGroupByTest();
    selectWithLastTest();

    selectWithAggregationTest();

    selectWithAlignByDeviceTest();

    session.close();
  }

  private static void selectTest() throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select s1 from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet = session.executeQueryStatement("select * from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void selectWithAlignByDeviceTest()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet =
        session.executeQueryStatement("select * from root.sg_1 align by device");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void selectWithValueFilterTest()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet =
        session.executeQueryStatement("select s1 from root.sg_1.d1 where s1 > 0");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select * from root.sg_1.d1 where time > 50 and s1 > 0 and s2 > 10000");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void selectWithAggregationTest()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select count(s1) from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select sum(*) from root.sg_1.d1 where time > 50 and s1 > 0 and s2 > 10000");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void selectWithGroupByTest()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select count(s1) from root.sg_1.d1 GROUP BY ([1, 100), 20ms)");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select count(*) from root.sg_1.d1 where time > 50 and s1 > 0 and s2 > 10000"
                + " GROUP BY ([50, 100), 10ms)");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void selectWithLastTest()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select last s1 from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet = session.executeQueryStatement("select last * from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  // be sure template is coordinate with tablet
  private static void createTemplate()
      throws StatementExecutionException, IoTDBConnectionException {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> vectorMeasurement = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      vectorMeasurement.add("s" + i);
    }
    measurementList.add(vectorMeasurement);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    List<TSDataType> vectorDatatype = new ArrayList<>();
    vectorDatatype.add(TSDataType.INT64);
    vectorDatatype.add(TSDataType.INT32);
    dataTypeList.add(vectorDatatype);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    List<TSEncoding> vectorEncoding = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      vectorEncoding.add(TSEncoding.RLE);
    }
    encodingList.add(vectorEncoding);

    List<CompressionType> compressionTypeList = new ArrayList<>();
    compressionTypeList.add(CompressionType.SNAPPY);

    session.createDeviceTemplate(
        "template1", measurementList, dataTypeList, encodingList, compressionTypeList);
    session.setDeviceTemplate("template1", "root.sg_1");
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private static void insertTabletWithAlignedTimeseriesMethod1()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(
        new VectorMeasurementSchema(
            new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT32}));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList);
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(
          schemaList.get(0).getValueMeasurementIdList().get(0),
          rowIndex,
          new SecureRandom().nextLong());
      tablet.addValue(
          schemaList.get(0).getValueMeasurementIdList().get(1),
          rowIndex,
          new SecureRandom().nextInt());

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    session.executeNonQueryStatement("flush");
  }

  /** Method 2 for insert tablet with aligned timeseries */
  private static void insertTabletWithAlignedTimeseriesMethod2()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(
        new VectorMeasurementSchema(
            new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT32}));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;

      long[] sensor1 = (long[]) values[0];
      sensor1[row] = new SecureRandom().nextLong();

      int[] sensor2 = (int[]) values[1];
      sensor2[row] = new SecureRandom().nextInt();

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet, true);
      tablet.reset();
    }

    session.executeNonQueryStatement("flush");
  }

  private static void insertNullableTabletWithAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(
        new VectorMeasurementSchema(
            new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT32}));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    // Use the bitMap to mark the null value point
    BitMap[] bitMaps = new BitMap[values.length];
    tablet.bitMaps = bitMaps;

    bitMaps[1] = new BitMap(tablet.getMaxRowNumber());
    for (long time = 100; time < 200; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;

      long[] sensor1 = (long[]) values[0];
      sensor1[row] = new SecureRandom().nextLong();

      int[] sensor2 = (int[]) values[1];
      sensor2[row] = new SecureRandom().nextInt();

      // mark this point as null value
      if (time % 5 == 0) {
        bitMaps[1].mark(row);
      }

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
        bitMaps[1].reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet, true);
      tablet.reset();
    }

    session.executeNonQueryStatement("flush");
  }
}
