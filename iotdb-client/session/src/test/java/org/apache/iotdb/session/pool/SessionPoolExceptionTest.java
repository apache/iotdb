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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SessionPoolExceptionTest {

  @Mock private ISessionPool sessionPool;

  @Mock private Session session;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(Collections.singletonList("host:11"))
            .user("user")
            .password("password")
            .maxSize(10)
            .enableAutoFetch(false)
            .build();
    ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
    queue.add(session);
    Whitebox.setInternalState(sessionPool, "queue", queue);
  }

  @After
  public void tearDown() {
    // Close the session pool after each test
    if (null != sessionPool) {
      sessionPool.close();
    }
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertRecords() throws Exception {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertRecords(anyList(), anyList(), anyList(), anyList(), anyList());
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    List<List<Object>> valuesList =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    sessionPool.insertRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertTablet() throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertTablet(any(Tablet.class), anyBoolean());
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.BOOLEAN);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    sessionPool.insertTablet(tablet);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertTablets() throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertTablets(anyMap(), anyBoolean());
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.BOOLEAN);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device2", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.insertTablets(map);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertAlignedRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertAlignedRecords(anyList(), anyList(), anyList(), anyList(), anyList());
    List<String> deviceIds = Arrays.asList("alignedDevice3", "alignedDevice4");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    List<List<Object>> valuesList =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    sessionPool.insertAlignedRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertAlignedTablets()
      throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertAlignedTablets(anyMap(), anyBoolean());
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.BOOLEAN);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("alignedDevice2", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.insertAlignedTablets(map);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertRecordsOfOneDevice(
            anyString(), anyList(), anyList(), anyList(), anyList(), anyBoolean());
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    List<List<Object>> valuesList =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    sessionPool.insertRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesList);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testInsertStringRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertStringRecordsOfOneDevice(anyString(), anyList(), anyList(), anyList(), anyBoolean());
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", "50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertStringRecordsOfOneDevice("device1", timeList, measurementsList, valuesList);
  }

  @Test
  public void testInsertRecordsOfOneDeviceWithNoSort()
      throws IoTDBConnectionException, StatementExecutionException {
    Mockito.doThrow(new IoTDBConnectionException(""))
        .when(session)
        .insertRecordsOfOneDevice(anyString(), anyList(), anyList(), anyList(), anyList());
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    List<List<Object>> valuesList =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    sessionPool.insertRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesList);
  }

  @Test
  public void testInsertRecords2() throws Exception {
    ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
    queue.add(session);
    Whitebox.setInternalState(sessionPool, "queue", queue);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    List<List<Object>> valuesList =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    try {
      sessionPool.insertRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      assertTrue(e instanceof IoTDBConnectionException);
    }
  }

  @Test
  public void testEmptyNodeUrls() {
    try {
      ISessionPool failedSession = new SessionPool(Collections.emptyList(), "root", "root", 1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("nodeUrls shouldn't be empty.", e.getMessage());
    }
  }
}
