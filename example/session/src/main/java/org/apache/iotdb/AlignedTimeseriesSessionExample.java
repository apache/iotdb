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
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.InternalNode;
import org.apache.iotdb.session.template.MeasurementNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"squid:S106", "squid:S1144", "squid:S125"})
public class AlignedTimeseriesSessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1 = "root.sg_1.d1";
  private static final String ROOT_SG1_D1_VECTOR2 = "root.sg_1.d1.vector2";
  private static final String ROOT_SG1_D1_VECTOR3 = "root.sg_1.d1.vector3";
  private static final String ROOT_SG2_D1_VECTOR4 = "root.sg_2.d1.vector4";
  private static final String ROOT_SG2_D1_VECTOR5 = "root.sg_2.d1.vector5";
  private static final String ROOT_SG2_D1_VECTOR6 = "root.sg_2.d1.vector6";
  private static final String ROOT_SG2_D1_VECTOR7 = "root.sg_2.d1.vector7";
  private static final String ROOT_SG2_D1_VECTOR8 = "root.sg_2.d1.vector8";
  public static final String FLUSH = "flush";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    //    createTemplate();
    createAlignedTimeseries();

    insertAlignedRecord();
    //    insertAlignedRecords();
    //    insertAlignedRecordsOfOneDevice();

    //    insertAlignedStringRecord();
    //    insertAlignedStringRecords();

    //    insertTabletWithAlignedTimeseriesMethod1();
    //    insertTabletWithAlignedTimeseriesMethod2();
    //    insertNullableTabletWithAlignedTimeseries();
    //    insertTabletsWithAlignedTimeseries();
    session.executeNonQueryStatement(FLUSH);
    selectTest();
    selectWithValueFilterTest();
    selectWithLastTest();
    selectWithLastTestWithoutValueFilter();
    session.executeNonQueryStatement("delete from root.sg_1.d1.s1 where time <= 5");
    System.out.println("execute sql delete from root.sg_1.d1.s1 where time <= 5");
    selectTest();
    selectWithValueFilterTest();
    selectWithLastTest();
    selectWithLastTestWithoutValueFilter();
    session.executeNonQueryStatement("delete from root.sg_1.d1.s2 where time <= 3");
    System.out.println("execute sql delete from root.sg_1.d1.s2 where time <= 3");

    selectTest();
    selectWithValueFilterTest();
    selectWithLastTest();
    selectWithLastTestWithoutValueFilter();
    session.executeNonQueryStatement("delete from root.sg_1.d1.s1 where time <= 10");
    System.out.println("execute sql delete from root.sg_1.d1.s1 where time <= 10");
    selectTest();
    selectWithValueFilterTest();
    selectWithLastTest();
    selectWithLastTestWithoutValueFilter();

    //    selectWithValueFilterTest();
    //    selectWithGroupByTest();
    //    selectWithLastTest();

    //    selectWithAggregationTest();

    //    selectWithAlignByDeviceTest();

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
        session.executeQueryStatement("select s1 from root.sg_1.d1 where s1 > 3 and time < 9");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select * from root.sg_1.d1 where time < 8 and s1 > 3 and s2 > 5");
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
    dataSet = session.executeQueryStatement("select count(*) from root.sg_1.d1");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select sum(*) from root.sg_1.d1.vector where time > 50 and s1 > 0 and s2 > 10000");
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
            "select count(s1) from root.sg_1.d1.vector GROUP BY ([1, 100), 20ms)");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
    dataSet =
        session.executeQueryStatement(
            "select count(*) from root.sg_1.d1.vector where time > 50 and s1 > 0 and s2 > 10000"
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

  private static void selectWithLastTestWithoutValueFilter()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet =
        session.executeQueryStatement("select last s1 from root.sg_1.d1 where time >= 5");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();

    dataSet = session.executeQueryStatement("select last * from root.sg_1.d1 where time >= 5");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();

    dataSet = session.executeQueryStatement("select last * from root.sg_1.d1 where time >= 20");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();
  }

  private static void createAlignedTimeseries()
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      measurements.add("s" + i);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT32);
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      encodings.add(TSEncoding.RLE);
      compressors.add(CompressionType.SNAPPY);
    }
    session.createAlignedTimeseries(
        ROOT_SG1_D1, measurements, dataTypes, encodings, compressors, null, null, null);
  }

  // be sure template is coordinate with tablet
  private static void createTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    Template template = new Template("template1");
    InternalNode iNodeVector = new InternalNode("vector", true);
    MeasurementNode mNodeS1 =
        new MeasurementNode("s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS2 =
        new MeasurementNode("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);

    iNodeVector.addChild(mNodeS1);
    iNodeVector.addChild(mNodeS2);

    template.addToTemplate(iNodeVector);

    session.createSchemaTemplate(template);
    session.setSchemaTemplate("template1", "root.sg_1");
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private static void insertTabletWithAlignedTimeseriesMethod1()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList);
    long timestamp = 1;

    for (long row = 1; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(
          schemaList.get(0).getMeasurementId(), rowIndex, new SecureRandom().nextLong());
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, new SecureRandom().nextInt());

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

    session.executeNonQueryStatement(FLUSH);
  }

  /** Method 2 for insert tablet with aligned timeseries */
  private static void insertTabletWithAlignedTimeseriesMethod2()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR2, schemaList);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 100; time < 200; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;

      long[] sensor1 = (long[]) values[0];
      sensor1[row] = new SecureRandom().nextLong();

      int[] sensor2 = (int[]) values[1];
      sensor2[row] = new SecureRandom().nextInt();

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet, true);
      tablet.reset();
    }

    session.executeNonQueryStatement(FLUSH);
  }

  private static void insertNullableTabletWithAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR3, schemaList);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    // Use the bitMap to mark the null value point
    BitMap[] bitMaps = new BitMap[values.length];
    tablet.bitMaps = bitMaps;

    bitMaps[1] = new BitMap(tablet.getMaxRowNumber());
    for (long time = 200; time < 300; time++) {
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
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
        bitMaps[1].reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet, true);
      tablet.reset();
    }

    session.executeNonQueryStatement(FLUSH);
  }

  private static void insertAlignedRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    // first file we have both sensots' data
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT32);

    for (long time = 0; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time);
      values.add((int) time);
      session.insertAlignedRecord(ROOT_SG1_D1, time, measurements, types, values);
    }
    session.executeNonQueryStatement(FLUSH);
    // second file we only have s1's data
    measurements.clear();
    types.clear();
    measurements.add("s1");
    types.add(TSDataType.INT64);
    for (long time = 10; time < 20; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time);
      session.insertAlignedRecord(ROOT_SG1_D1, time, measurements, types, values);
    }
  }

  private static void insertAlignedStringRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");

    for (long time = 0; time < 1; time++) {
      List<String> values = new ArrayList<>();
      values.add("3");
      values.add("4");
      session.insertAlignedRecord(ROOT_SG2_D1_VECTOR5, time, measurements, values);
    }
  }

  private static void insertAlignedRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();

    for (long time = 1; time < 5; time++) {
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");

      List<TSDataType> types = new ArrayList<>();
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT32);

      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2);

      deviceIds.add(ROOT_SG2_D1_VECTOR4);
      times.add(time);
      measurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecords(deviceIds, times, measurementsList, typeList, valueList);
  }

  private static void insertAlignedStringRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> valueList = new ArrayList<>();

    for (long time = 1; time < 5; time++) {
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");

      List<String> values = new ArrayList<>();
      values.add("3");
      values.add("4");

      deviceIds.add(ROOT_SG2_D1_VECTOR5);
      times.add(time);
      measurementsList.add(measurements);
      valueList.add(values);
    }
    session.insertAlignedRecords(deviceIds, times, measurementsList, valueList);
  }

  private static void insertAlignedRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();

    for (long time = 10; time < 15; time++) {
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");

      List<TSDataType> types = new ArrayList<>();
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT32);

      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2);

      times.add(time);
      measurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecordsOfOneDevice(
        ROOT_SG2_D1_VECTOR4, times, measurementsList, typeList, valueList);
  }

  private static void insertTabletsWithAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    List<IMeasurementSchema> schemaList1 = new ArrayList<>();
    schemaList1.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList1.add(new MeasurementSchema("s2", TSDataType.INT64));

    List<IMeasurementSchema> schemaList2 = new ArrayList<>();
    schemaList2.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList2.add(new MeasurementSchema("s2", TSDataType.INT64));

    List<IMeasurementSchema> schemaList3 = new ArrayList<>();
    schemaList3.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList3.add(new MeasurementSchema("s2", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG2_D1_VECTOR6, schemaList1, 100);
    Tablet tablet2 = new Tablet(ROOT_SG2_D1_VECTOR7, schemaList2, 100);
    Tablet tablet3 = new Tablet(ROOT_SG2_D1_VECTOR8, schemaList3, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG2_D1_VECTOR6, tablet1);
    tabletMap.put(ROOT_SG2_D1_VECTOR7, tablet2);
    tabletMap.put(ROOT_SG2_D1_VECTOR8, tablet3);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 2; i++) {
        long value = new SecureRandom().nextLong();
        tablet1.addValue(schemaList1.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList2.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList3.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertAlignedTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertAlignedTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;
    long[] timestamps3 = tablet3.timestamps;
    Object[] values3 = tablet3.values;

    for (long time = 0; time < 100; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 2; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertAlignedTablets(tabletMap, true);

        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
    }

    if (tablet1.rowSize != 0) {
      session.insertAlignedTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }
  }
}
