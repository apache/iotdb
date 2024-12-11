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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SessionCacheLeaderTest {

  private static final List<TEndPoint> endpoints =
      new ArrayList<TEndPoint>() {
        {
          add(new TEndPoint("127.0.0.1", 55560)); // default endpoint
          add(new TEndPoint("127.0.0.1", 55561));
          add(new TEndPoint("127.0.0.1", 55562));
          add(new TEndPoint("127.0.0.1", 55563));
        }
      };

  private Session session;

  // just for simulation
  public static TEndPoint getDeviceIdBelongedEndpoint(String deviceId) {
    if (deviceId.startsWith("root.sg1")) {
      return endpoints.get(0);
    } else if (deviceId.startsWith("root.sg2")) {
      return endpoints.get(1);
    } else if (deviceId.startsWith("root.sg3")) {
      return endpoints.get(2);
    } else if (deviceId.startsWith("root.sg4")) {
      return endpoints.get(3);
    }

    return endpoints.get(deviceId.hashCode() % endpoints.size());
  }

  public static TEndPoint getDeviceIdBelongedEndpoint(IDeviceID deviceId) {
    if (deviceId.equals(new StringArrayDeviceID("table1", "id0"))) {
      return endpoints.get(0);
    } else if (deviceId.equals(new StringArrayDeviceID("table1", "id1"))) {
      return endpoints.get(1);
    } else if (deviceId.equals(new StringArrayDeviceID("table1", "id2"))) {
      return endpoints.get(2);
    } else if (deviceId.equals(new StringArrayDeviceID("table1", "id3"))) {
      return endpoints.get(3);
    }

    return endpoints.get(deviceId.hashCode() % endpoints.size());
  }

  @Test
  public void testInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String deviceId = "root.sg2.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertStringRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String deviceId = "root.sg2.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }

    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertRecords() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    List<String> allDeviceIds =
        new ArrayList<String>() {
          {
            add("root.sg1.d1");
            add("root.sg2.d1");
            add("root.sg3.d1");
            add("root.sg4.d1");
          }
        };
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);

      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
    deviceIds.clear();
    measurementsList.clear();
    valuesList.clear();
    timestamps.clear();

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }
    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);

    assertEquals(4, session.deviceIdToEndpoint.size());
    for (String deviceId : allDeviceIds) {
      assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    }
    assertEquals(4, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertStringRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    List<String> allDeviceIds =
        new ArrayList<String>() {
          {
            add("root.sg1.d1");
            add("root.sg2.d1");
            add("root.sg3.d1");
            add("root.sg4.d1");
          }
        };
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }
    session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
    deviceIds.clear();
    measurementsList.clear();
    valuesList.clear();
    timestamps.clear();

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }
    session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);

    assertEquals(4, session.deviceIdToEndpoint.size());
    for (String deviceId : allDeviceIds) {
      assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    }
    assertEquals(4, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String deviceId = "root.sg2.d2";
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> datatypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    addLine(
        times,
        measurements,
        datatypes,
        values,
        3L,
        "s1",
        "s2",
        TSDataType.INT32,
        TSDataType.INT32,
        1,
        2);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        2L,
        "s2",
        "s3",
        TSDataType.INT32,
        TSDataType.INT64,
        3,
        4L);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        1L,
        "s4",
        "s5",
        TSDataType.FLOAT,
        TSDataType.BOOLEAN,
        5.0f,
        Boolean.TRUE);
    session.insertRecordsOfOneDevice(deviceId, times, measurements, datatypes, values);

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    session.insertRecordsOfOneDevice(deviceId, times, measurements, datatypes, values);

    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertTablet() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String deviceId = "root.sg2.d2";
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
    Tablet tablet = new Tablet(deviceId, schemaList, 100);
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.getRowSize() != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.getRowSize() != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertTablets() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    List<String> allDeviceIds =
        new ArrayList<String>() {
          {
            add("root.sg1.d1");
            add("root.sg2.d1");
            add("root.sg3.d1");
            add("root.sg4.d1");
          }
        };
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(allDeviceIds.get(1), schemaList, 100);
    Tablet tablet2 = new Tablet(allDeviceIds.get(2), schemaList, 100);
    Tablet tablet3 = new Tablet(allDeviceIds.get(3), schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(allDeviceIds.get(1), tablet1);
    tabletMap.put(allDeviceIds.get(2), tablet2);
    tabletMap.put(allDeviceIds.get(3), tablet3);

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.getRowSize();
      int row2 = tablet2.getRowSize();
      int row3 = tablet3.getRowSize();
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementName(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementName(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementName(), row3, value);
      }
      if (tablet1.getRowSize() == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.getRowSize() != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.getRowSize();
      int row2 = tablet2.getRowSize();
      int row3 = tablet3.getRowSize();
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementName(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementName(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementName(), row3, value);
      }
      if (tablet1.getRowSize() == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.getRowSize() != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    assertEquals(3, session.deviceIdToEndpoint.size());
    for (String deviceId : allDeviceIds.subList(1, allDeviceIds.size())) {
      assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    }
    assertEquals(4, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertRelationalTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false, "table");
    session.open();
    assertNull(session.tableModelDeviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String tableName = "table1";
    List<String> measurements = new ArrayList<>();
    measurements.add("id");
    measurements.add("s1");
    measurements.add("s2");
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.STRING);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT64);
    List<ColumnCategory> columnTypeList = new ArrayList<>();
    columnTypeList.add(ColumnCategory.ID);
    columnTypeList.add(ColumnCategory.MEASUREMENT);
    columnTypeList.add(ColumnCategory.MEASUREMENT);
    Tablet tablet = new Tablet(tableName, measurements, dataTypes, columnTypeList, 50);
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(measurements.get(0), rowIndex, "id" + (rowIndex % 4));
      for (int s = 1; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(measurements.get(s), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.getRowSize() != 0) {
      session.insertRelationalTablet(tablet);
      tablet.reset();
    }

    assertNull(session.tableModelDeviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true, "table");
    session.open();
    assertEquals(0, session.tableModelDeviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(measurements.get(0), rowIndex, "id" + (rowIndex % 4));
      for (int s = 1; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(measurements.get(s), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.getRowSize() != 0) {
      session.insertRelationalTablet(tablet);
      tablet.reset();
    }

    assertEquals(4, session.tableModelDeviceIdToEndpoint.size());
    assertEquals(4, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertRecordsWithSessionBroken() throws StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);

    List<String> allDeviceIds =
        new ArrayList<String>() {
          {
            add("root.sg1.d1");
            add("root.sg2.d1");
            add("root.sg3.d1");
            add("root.sg4.d1");
          }
        };
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);

      if (time != 0 && time % 100 == 0) {
        try {
          session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
          Assert.fail();
        } catch (IoTDBConnectionException e) {
          Assert.assertEquals(
              "the session connection = TEndPoint(ip:127.0.0.1, port:55560) is broken",
              e.getMessage());
        }
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    try {
      session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
      fail();
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals(
          "the session connection = TEndPoint(ip:127.0.0.1, port:55560) is broken", e.getMessage());
    }
    deviceIds.clear();
    measurementsList.clear();
    valuesList.clear();
    timestamps.clear();

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }

    // with leader cache
    // reset connection
    session = new MockSession("127.0.0.1", 55560, true);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());
    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      deviceIds.add(allDeviceIds.get((int) (time % allDeviceIds.size())));
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        try {
          session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException e) {
          Assert.fail(e.getMessage());
        }
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    // set connection as broken, due to we enable the cache leader, when we called
    // ((MockSession) session).getLastConstructedSessionConnection(), the session's endpoint has
    // been changed to TEndPoint(ip:127.0.0.1, port:55561)
    Assert.assertEquals(
        "MockSessionConnection{ endPoint=TEndPoint(ip:127.0.0.1, port:55561)}",
        ((MockSession) session).getLastConstructedSessionConnection().toString());
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);
    try {
      session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertEquals(3, session.deviceIdToEndpoint.size());
    for (Map.Entry<String, TEndPoint> endPointMap : session.deviceIdToEndpoint.entrySet()) {
      assertEquals(getDeviceIdBelongedEndpoint(endPointMap.getKey()), endPointMap.getValue());
    }
    assertEquals(3, session.endPointToSessionConnection.size());
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertTabletsWithSessionBroken() throws StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    // set the session connection as broken
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);
    List<String> allDeviceIds =
        new ArrayList<String>() {
          {
            add("root.sg1.d1");
            add("root.sg2.d1");
            add("root.sg3.d1");
            add("root.sg4.d1");
          }
        };
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(allDeviceIds.get(1), schemaList, 100);
    Tablet tablet2 = new Tablet(allDeviceIds.get(2), schemaList, 100);
    Tablet tablet3 = new Tablet(allDeviceIds.get(3), schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(allDeviceIds.get(1), tablet1);
    tabletMap.put(allDeviceIds.get(2), tablet2);
    tabletMap.put(allDeviceIds.get(3), tablet3);

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.getRowSize();
      int row2 = tablet2.getRowSize();
      int row3 = tablet3.getRowSize();
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementName(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementName(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementName(), row3, value);
      }
      if (tablet1.getRowSize() == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
          fail();
        } catch (IoTDBConnectionException e) {
          assertEquals(
              "the session connection = TEndPoint(ip:127.0.0.1, port:55560) is broken",
              e.getMessage());
        }
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }

    // with leader cache
    // rest the session connection
    session = new MockSession("127.0.0.1", 55560, true);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.getRowSize();
      int row2 = tablet2.getRowSize();
      int row3 = tablet3.getRowSize();
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementName(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementName(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementName(), row3, value);
      }
      if (tablet1.getRowSize() == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
        } catch (IoTDBConnectionException e) {
          fail(e.getMessage());
        }
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    // set the session connection as broken
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);
    // set connection as broken, due to we enable the cache leader, when we called
    // ((MockSession) session).getLastConstructedSessionConnection(), the session's endpoint has
    // been changed to EndPoint(ip:127.0.0.1, port:55562)
    assertEquals(
        "MockSessionConnection{ endPoint=TEndPoint(ip:127.0.0.1, port:55562)}",
        ((MockSession) session).getLastConstructedSessionConnection().toString());

    for (long row = 0; row < 10; row++) {
      int row1 = tablet1.getRowSize();
      int row2 = tablet2.getRowSize();
      int row3 = tablet3.getRowSize();
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementName(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementName(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementName(), row3, value);
      }
      if (tablet1.getRowSize() == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
        } catch (IoTDBConnectionException e) {
          fail(e.getMessage());
        }
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }
    try {
      session.insertTablets(tabletMap, true);
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }
    tablet1.reset();
    tablet2.reset();
    tablet3.reset();

    assertEquals(2, session.deviceIdToEndpoint.size());
    for (Map.Entry<String, TEndPoint> endPointEntry : session.deviceIdToEndpoint.entrySet()) {
      assertEquals(getDeviceIdBelongedEndpoint(endPointEntry.getKey()), endPointEntry.getValue());
    }
    assertEquals(3, session.endPointToSessionConnection.size());
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertRelationalTabletWithSessionBroken() throws StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false, "table");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertNull(session.tableModelDeviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    // set the session connection as broken
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);

    String tableName = "table1";
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<ColumnCategory> columnTypeList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("id", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    columnTypeList.add(ColumnCategory.ID);
    columnTypeList.add(ColumnCategory.MEASUREMENT);
    columnTypeList.add(ColumnCategory.MEASUREMENT);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypeList,
            50);
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementName(), rowIndex, "id" + (rowIndex % 4));
      for (int s = 1; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        try {
          session.insertRelationalTablet(tablet);
          fail();
        } catch (IoTDBConnectionException e) {
          assertEquals(
              "the session connection = TEndPoint(ip:127.0.0.1, port:55560) is broken",
              e.getMessage());
        }
        tablet.reset();
      }
      timestamp++;
    }

    assertNull(session.tableModelDeviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }

    // with leader cache
    // rest the session connection
    session = new MockSession("127.0.0.1", 55560, true, "table");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertEquals(0, session.tableModelDeviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementName(), rowIndex, "id" + (rowIndex % 4));
      for (int s = 1; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        try {
          session.insertRelationalTablet(tablet);
        } catch (IoTDBConnectionException e) {
          Assert.fail(e.getMessage());
        }
        tablet.reset();
      }
      timestamp++;
    }

    // set the session connection as broken
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);
    // set connection as broken, due to we enable the cache leader, when we called
    // ((MockSession) session).getLastConstructedSessionConnection(), the session's endpoint has
    // been changed to EndPoint(ip:127.0.0.1, port:55562)
    Assert.assertEquals(
        "MockSessionConnection{ endPoint=TEndPoint(ip:127.0.0.1, port:55562)}",
        ((MockSession) session).getLastConstructedSessionConnection().toString());

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementName(), rowIndex, "id" + (rowIndex % 4));
      for (int s = 1; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        try {
          session.insertRelationalTablet(tablet);
        } catch (IoTDBConnectionException e) {
          Assert.fail(e.getMessage());
        }
        tablet.reset();
      }
      timestamp++;
    }

    assertEquals(3, session.tableModelDeviceIdToEndpoint.size());
    for (Map.Entry<IDeviceID, TEndPoint> endPointEntry :
        session.tableModelDeviceIdToEndpoint.entrySet()) {
      assertEquals(getDeviceIdBelongedEndpoint(endPointEntry.getKey()), endPointEntry.getValue());
    }
    assertEquals(3, session.endPointToSessionConnection.size());
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void addLine(
      List<Long> times,
      List<List<String>> measurements,
      List<List<TSDataType>> datatypes,
      List<List<Object>> values,
      long time,
      String s1,
      String s2,
      TSDataType s1type,
      TSDataType s2type,
      Object value1,
      Object value2) {
    List<String> tmpMeasurements = new ArrayList<>();
    List<TSDataType> tmpDataTypes = new ArrayList<>();
    List<Object> tmpValues = new ArrayList<>();
    tmpMeasurements.add(s1);
    tmpMeasurements.add(s2);
    tmpDataTypes.add(s1type);
    tmpDataTypes.add(s2type);
    tmpValues.add(value1);
    tmpValues.add(value2);
    times.add(time);
    measurements.add(tmpMeasurements);
    datatypes.add(tmpDataTypes);
    values.add(tmpValues);
  }

  static class MockSession extends Session {

    private MockSessionConnection lastConstructedSessionConnection;

    public MockSession(String host, int rpcPort, boolean enableRedirection) {
      super(
          host,
          rpcPort,
          SessionConfig.DEFAULT_USER,
          SessionConfig.DEFAULT_PASSWORD,
          SessionConfig.DEFAULT_FETCH_SIZE,
          null,
          SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
          SessionConfig.DEFAULT_MAX_FRAME_SIZE,
          enableRedirection,
          SessionConfig.DEFAULT_VERSION);
      this.enableAutoFetch = false;
    }

    public MockSession(String host, int rpcPort, boolean enableRedirection, String sqlDialect) {
      this(host, rpcPort, enableRedirection);
      this.sqlDialect = sqlDialect;
      this.enableAutoFetch = false;
    }

    @Override
    public SessionConnection constructSessionConnection(
        Session session, TEndPoint endpoint, ZoneId zoneId) {
      lastConstructedSessionConnection = new MockSessionConnection(session, endpoint, zoneId);
      return lastConstructedSessionConnection;
    }

    public MockSessionConnection getLastConstructedSessionConnection() {
      return lastConstructedSessionConnection;
    }
  }

  static class MockSessionConnection extends SessionConnection {

    private TEndPoint endPoint;
    private boolean connectionBroken;
    private IoTDBConnectionException ioTDBConnectionException;

    public MockSessionConnection(Session session, TEndPoint endPoint, ZoneId zoneId) {
      super(session.sqlDialect);
      this.endPoint = endPoint;
      ioTDBConnectionException =
          new IoTDBConnectionException(
              String.format("the session connection = %s is broken", endPoint.toString()));
    }

    @Override
    public void close() {}

    @Override
    protected void insertRecord(TSInsertRecordReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw new RedirectException(getDeviceIdBelongedEndpoint(request.prefixPath));
    }

    @Override
    protected void insertRecord(TSInsertStringRecordReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw new RedirectException(getDeviceIdBelongedEndpoint(request.prefixPath));
    }

    @Override
    protected void insertRecords(TSInsertRecordsReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw getRedirectException(request.getPrefixPaths());
    }

    @Override
    protected void insertRecords(TSInsertStringRecordsReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw getRedirectException(request.getPrefixPaths());
    }

    @Override
    protected void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw new RedirectException(getDeviceIdBelongedEndpoint(request.prefixPath));
    }

    @Override
    protected void insertTablet(TSInsertTabletReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      if (request.writeToTable) {
        if (request.size >= 50) {
          // multi devices
          List<TEndPoint> endPoints = new ArrayList<>();
          for (int i = 0; i < request.size; i++) {
            endPoints.add(endpoints.get(i % 4));
          }
          throw new RedirectException(endPoints);
        }
      } else {
        throw new RedirectException(getDeviceIdBelongedEndpoint(request.prefixPath));
      }
    }

    @Override
    protected void insertTablets(TSInsertTabletsReq request)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw getRedirectException(request.getPrefixPaths());
    }

    private RedirectException getRedirectException(List<String> deviceIds) {
      Map<String, TEndPoint> deviceEndPointMap = new HashMap<>();
      for (String deviceId : deviceIds) {
        deviceEndPointMap.put(deviceId, getDeviceIdBelongedEndpoint(deviceId));
      }
      return new RedirectException(deviceEndPointMap);
    }

    public boolean isConnectionBroken() {
      return connectionBroken;
    }

    public void setConnectionBroken(boolean connectionBroken) {
      this.connectionBroken = connectionBroken;
    }

    @Override
    public String toString() {
      return "MockSessionConnection{" + " endPoint=" + endPoint + "}";
    }
  }
}
