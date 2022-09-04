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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SessionCacheLeaderUT {

  private static final List<EndPoint> endpoints =
      new ArrayList<EndPoint>() {
        {
          add(new EndPoint("127.0.0.1", 55560)); // default endpoint
          add(new EndPoint("127.0.0.1", 55561)); // meta leader endpoint
          add(new EndPoint("127.0.0.1", 55562));
          add(new EndPoint("127.0.0.1", 55563));
        }
      };

  private Session session;

  // just for simulation
  public static EndPoint getDeviceIdBelongedEndpoint(String deviceId) {
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

  @Test
  public void testSetStorageGroup() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    session.setStorageGroup("root.sg1");

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    session.setStorageGroup("root.sg1");

    assertNotEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testDeleteStorageGroups()
      throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    session.deleteStorageGroups(
        new ArrayList<String>() {
          {
            add("root.sg1");
            add("root.sg2");
            add("root.sg3");
          }
        });

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    session.deleteStorageGroups(
        new ArrayList<String>() {
          {
            add("root.sg1");
            add("root.sg2");
            add("root.sg3");
          }
        });

    assertNotEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    session.insertRecordsOfOneDevice(deviceId, times, measurements, datatypes, values);

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertTablet() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);

    String deviceId = "root.sg2.d2";
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));
    Tablet tablet = new Tablet(deviceId, schemaList, 100);
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }

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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }

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

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(1, session.deviceIdToEndpoint.size());
    assertEquals(2, session.endPointToSessionConnection.size());
    session.close();
  }

  @Test
  public void testInsertTablets() throws IoTDBConnectionException, StatementExecutionException {
    // without leader cache
    session = new MockSession("127.0.0.1", 55560, false);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(allDeviceIds.get(1), schemaList, 100);
    Tablet tablet2 = new Tablet(allDeviceIds.get(2), schemaList, 100);
    Tablet tablet3 = new Tablet(allDeviceIds.get(3), schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(allDeviceIds.get(1), tablet1);
    tabletMap.put(allDeviceIds.get(2), tablet2);
    tabletMap.put(allDeviceIds.get(3), tablet3);

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    session.close();

    // with leader cache
    session = new MockSession("127.0.0.1", 55560, true);
    session.open();
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(3, session.deviceIdToEndpoint.size());
    for (String deviceId : allDeviceIds.subList(1, allDeviceIds.size())) {
      assertEquals(getDeviceIdBelongedEndpoint(deviceId), session.deviceIdToEndpoint.get(deviceId));
    }
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
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
        } catch (IoTDBConnectionException e) {
          Assert.assertEquals(
              "the session connection = EndPoint(ip:127.0.0.1, port:55560) is broken",
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
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals(
          "the session connection = EndPoint(ip:127.0.0.1, port:55560) is broken", e.getMessage());
    }
    deviceIds.clear();
    measurementsList.clear();
    valuesList.clear();
    timestamps.clear();

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals(
          "the session connection = EndPoint(ip:127.0.0.1, port:55560) is broken", e.getMessage());
    }

    // with leader cache
    // reset connection
    session = new MockSession("127.0.0.1", 55560, true);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    // been changed to EndPoint(ip:127.0.0.1, port:55562)
    Assert.assertEquals(
        "MockSessionConnection{ endPoint=EndPoint(ip:127.0.0.1, port:55562)}",
        ((MockSession) session).getLastConstructedSessionConnection().toString());
    ((MockSession) session).getLastConstructedSessionConnection().setConnectionBroken(true);
    try {
      session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals(
          "the session connection = EndPoint(ip:127.0.0.1, port:55562) is broken", e.getMessage());
    }
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(3, session.deviceIdToEndpoint.size());
    for (Map.Entry<String, EndPoint> endPointMap : session.deviceIdToEndpoint.entrySet()) {
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
      Assert.fail(e.getMessage());
    }
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
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
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(allDeviceIds.get(1), schemaList, 100);
    Tablet tablet2 = new Tablet(allDeviceIds.get(2), schemaList, 100);
    Tablet tablet3 = new Tablet(allDeviceIds.get(3), schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(allDeviceIds.get(1), tablet1);
    tabletMap.put(allDeviceIds.get(2), tablet2);
    tabletMap.put(allDeviceIds.get(3), tablet3);

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
        } catch (IoTDBConnectionException e) {
          assertEquals(
              "the session connection = EndPoint(ip:127.0.0.1, port:55560) is broken",
              e.getMessage());
        }
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      try {
        session.insertTablets(tabletMap, true);
      } catch (IoTDBConnectionException e) {
        Assert.fail(e.getMessage());
      }

      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertNull(session.deviceIdToEndpoint);
    assertNull(session.endPointToSessionConnection);
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }

    // with leader cache
    // rest the session connection
    session = new MockSession("127.0.0.1", 55560, true);
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      Assert.fail(e.getMessage());
    }
    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(0, session.deviceIdToEndpoint.size());
    assertEquals(1, session.endPointToSessionConnection.size());

    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
        } catch (IoTDBConnectionException e) {
          Assert.fail(e.getMessage());
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
    Assert.assertEquals(
        "MockSessionConnection{ endPoint=EndPoint(ip:127.0.0.1, port:55562)}",
        ((MockSession) session).getLastConstructedSessionConnection().toString());

    for (long row = 0; row < 10; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        try {
          session.insertTablets(tabletMap, true);
        } catch (IoTDBConnectionException e) {
          Assert.fail(e.getMessage());
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
      Assert.assertEquals(
          "the session connection = EndPoint(ip:127.0.0.1, port:55562) is broken", e.getMessage());
    }
    tablet1.reset();
    tablet2.reset();
    tablet3.reset();

    assertEquals(session.metaSessionConnection, session.defaultSessionConnection);
    assertEquals(2, session.deviceIdToEndpoint.size());
    for (Map.Entry<String, EndPoint> endPointEntry : session.deviceIdToEndpoint.entrySet()) {
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

    public MockSession(String host, int rpcPort, boolean enableCacheLeader) {
      super(
          host,
          rpcPort,
          Config.DEFAULT_USER,
          Config.DEFAULT_PASSWORD,
          Config.DEFAULT_FETCH_SIZE,
          null,
          Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
          Config.DEFAULT_MAX_FRAME_SIZE,
          enableCacheLeader);
    }

    @Override
    public SessionConnection constructSessionConnection(
        Session session, EndPoint endpoint, ZoneId zoneId) {
      lastConstructedSessionConnection = new MockSessionConnection(session, endpoint, zoneId);
      return lastConstructedSessionConnection;
    }

    public MockSessionConnection getLastConstructedSessionConnection() {
      return lastConstructedSessionConnection;
    }
  }

  static class MockSessionConnection extends SessionConnection {

    private EndPoint endPoint;
    private boolean connectionBroken;
    private IoTDBConnectionException ioTDBConnectionException;

    public MockSessionConnection(Session session, EndPoint endPoint, ZoneId zoneId) {
      super();
      this.endPoint = endPoint;
      ioTDBConnectionException =
          new IoTDBConnectionException(
              String.format("the session connection = %s is broken", endPoint.toString()));
    }

    @Override
    public void close() {}

    @Override
    protected void setStorageGroup(String storageGroup)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw new RedirectException(endpoints.get(1));
    }

    @Override
    protected void deleteStorageGroups(List<String> storageGroups)
        throws RedirectException, IoTDBConnectionException {
      if (isConnectionBroken()) {
        throw ioTDBConnectionException;
      }
      throw new RedirectException(endpoints.get(1));
    }

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
      throw new RedirectException(getDeviceIdBelongedEndpoint(request.prefixPath));
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
      Map<String, EndPoint> deviceEndPointMap = new HashMap<>();
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
