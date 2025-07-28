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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.InternalNode;
import org.apache.iotdb.session.template.MeasurementNode;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SessionPoolTest {

  @Mock private ISessionPool sessionPool;

  @Mock private Session session;
  @Mock TSExecuteStatementResp execResp;
  private long queryId;

  private long statementId;

  @Mock private IClientRPCService.Iface client;

  private final TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  @Mock private TSFetchMetadataResp fetchMetadataResp;
  @Mock private TSFetchResultsResp fetchResultsResp;

  @Test
  public void testBuilder() {
    SessionPool pool =
        new SessionPool.Builder()
            .host("localhost")
            .port(1234)
            .maxSize(10)
            .user("abc")
            .password("123")
            .fetchSize(1)
            .waitToGetSessionTimeoutInMs(2)
            .enableRedirection(true)
            .enableRecordsAutoConvertTablet(true)
            .enableCompression(true)
            .zoneId(ZoneOffset.UTC)
            .connectionTimeoutInMs(3)
            .version(Version.V_1_0)
            .build();

    assertEquals("localhost", pool.getHost());
    assertEquals(1234, pool.getPort());
    assertEquals("abc", pool.getUser());
    assertEquals("123", pool.getPassword());
    assertEquals(10, pool.getMaxSize());
    assertEquals(1, pool.getFetchSize());
    assertEquals(2, pool.getWaitToGetSessionTimeoutInMs());
    assertTrue(pool.isEnableRedirection());
    assertTrue(pool.isEnableCompression());
    assertEquals(3, pool.getConnectionTimeoutInMs());
    assertEquals(ZoneOffset.UTC, pool.getZoneId());
    assertEquals(Version.V_1_0, pool.getVersion());
  }

  @Test
  public void testBuilder2() {
    SessionPool pool =
        new SessionPool.Builder()
            .nodeUrls(Arrays.asList("127.0.0.1:1234"))
            .maxSize(10)
            .user("abc")
            .password("123")
            .fetchSize(1)
            .waitToGetSessionTimeoutInMs(2)
            .enableRedirection(true)
            .enableRecordsAutoConvertTablet(true)
            .enableCompression(true)
            .zoneId(ZoneOffset.UTC)
            .connectionTimeoutInMs(3)
            .version(Version.V_1_0)
            .thriftDefaultBufferSize(1024)
            .thriftMaxFrameSize(67108864)
            .build();

    assertEquals("abc", pool.getUser());
    assertEquals("123", pool.getPassword());
    assertEquals(10, pool.getMaxSize());
    assertEquals(1, pool.getFetchSize());
    assertEquals(2, pool.getWaitToGetSessionTimeoutInMs());
    assertTrue(pool.isEnableRedirection());
    assertTrue(pool.isEnableCompression());
    assertEquals(3, pool.getConnectionTimeoutInMs());
    assertEquals(ZoneOffset.UTC, pool.getZoneId());
    assertEquals(Version.V_1_0, pool.getVersion());
    pool.setQueryTimeout(12345);
    assertEquals(12345, pool.getQueryTimeout());
    pool.setVersion(Version.V_0_13);
    assertEquals(Version.V_0_13, pool.getVersion());
  }

  @Before
  public void setUp() throws IoTDBConnectionException, StatementExecutionException, TException {
    // Initialize the session pool before each test
    MockitoAnnotations.initMocks(this);
    execResp.queryResult = FakedFirstFetchTsBlockResult();

    Mockito.when(client.executeStatementV2(any(TSExecuteStatementReq.class))).thenReturn(execResp);
    Mockito.when(execResp.getQueryId()).thenReturn(queryId);
    Mockito.when(execResp.getStatus()).thenReturn(successStatus);

    Mockito.when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
    Mockito.when(fetchMetadataResp.getStatus()).thenReturn(successStatus);

    Mockito.when(client.fetchResultsV2(any(TSFetchResultsReq.class))).thenReturn(fetchResultsResp);
    Mockito.when(fetchResultsResp.getStatus()).thenReturn(successStatus);

    TSStatus closeResp = successStatus;
    Mockito.when(client.closeOperation(any(TSCloseOperationReq.class))).thenReturn(closeResp);

    sessionPool =
        new SessionPool.Builder()
            .host("host")
            .port(11)
            .user("user")
            .password("password")
            .maxSize(5)
            .enableAutoFetch(false)
            .build();

    ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
    queue.add(session);

    // set SessionPool's internal field state
    Whitebox.setInternalState(sessionPool, "queue", queue);
  }

  @After
  public void tearDown() {
    // Close the session pool after each test
    if (null != sessionPool) {
      sessionPool.close();
    }
  }

  @Test
  public void testInsertTablet() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    sessionPool.insertTablet(tablet);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testFetchSize() {
    sessionPool.setFetchSize(1000);
    Assert.assertEquals(1000, sessionPool.getFetchSize());
  }

  @Test
  public void testSetEnableRedirection() {
    sessionPool.setEnableRedirection(false);
    Assert.assertEquals(false, sessionPool.isEnableRedirection());
  }

  @Test
  public void testEnableQueryRedirection() {
    sessionPool.setEnableQueryRedirection(true);
    Assert.assertEquals(true, sessionPool.isEnableQueryRedirection());
  }

  @Test
  public void testTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    String zoneId = ZoneId.systemDefault().getId();
    sessionPool.setTimeZone(ZoneId.systemDefault().getId());
    Assert.assertEquals(zoneId, sessionPool.getZoneId().toString());
  }

  @Test
  public void testTestInsertTablet1() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    sessionPool.testInsertTablet(tablet);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertTablet2() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    sessionPool.testInsertTablet(tablet, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertTablets() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.testInsertTablets(map);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertTablets2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.testInsertTablets(map, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertRecords() throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    sessionPool.testInsertRecords(deviceIds, timeList, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertRecords2()
      throws IoTDBConnectionException, StatementExecutionException {
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
        Arrays.asList(Arrays.asList(11.20, "ssq1"), Arrays.asList(11.21, "ssq2"));
    sessionPool.testInsertRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.testInsertRecord(
        "device1", 1L, Arrays.asList("temperature", "humidity"), Arrays.asList("11", "12"));
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testTestInsertRecord2() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.testInsertRecord(
        "device1",
        1L,
        Arrays.asList("temperature", "humidity"),
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
        Arrays.asList("11", "12"));
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("alignedDevice1", schemas, timestamp, values, partBitMap, 2);
    sessionPool.insertAlignedTablet(tablet);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertTablets() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device2", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.insertTablets(map);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedTablets()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("alignedDevice2", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    sessionPool.insertAlignedTablets(map);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecords()
      throws IoTDBConnectionException, StatementExecutionException {
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
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
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
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecordsOfOneDeviceWithSort()
      throws IoTDBConnectionException, StatementExecutionException {
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
        "device1", timeList, measurementsList, typesList, valuesList, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertOneRecordsOfOneDevice2()
      throws IoTDBConnectionException, StatementExecutionException {
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
    sessionPool.insertOneDeviceRecords(
        "device1", timeList, measurementsList, typesList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertOneRecordsOfOneDevice3()
      throws IoTDBConnectionException, StatementExecutionException {
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
    sessionPool.insertOneDeviceRecords(
        "device1", timeList, measurementsList, typesList, valuesList, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
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
    sessionPool.insertAlignedRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecordsOfOneDevice2()
      throws IoTDBConnectionException, StatementExecutionException {
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
    sessionPool.insertAlignedRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesList, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertStringRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", "50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertStringRecordsOfOneDevice("device1", timeList, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertStringRecordsOfOneDevice2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", "50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedStringRecordsOfOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", "50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertAlignedStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedStringRecordsOfOneDevice2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", "50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertAlignedStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecords2() throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", " 50.0f"), Arrays.asList("220.0", "1.5"));
    sessionPool.insertRecords(deviceIds, timeList, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecords2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(1L, 2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("25.0f", " 50.0f"), Arrays.asList("220.0", "1.5"));

    sessionPool.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurementsList = Arrays.asList("temperature", "humidity");
    List<TSDataType> typesList = Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE);
    sessionPool.insertRecord("device1", 3L, measurementsList, typesList, "25.0", "50.0");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecord2() throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurementsList = Arrays.asList("temperature", "humidity");
    List<TSDataType> typesList = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
    List<Object> valuesList = Arrays.asList("25.0f", " 50.0f");
    sessionPool.insertRecord("device1", 4L, measurementsList, typesList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertRecord3() throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurementsList = Arrays.asList("temperature", "humidity");
    List<String> valuesList = Arrays.asList("25.0f", " 50.0f");
    sessionPool.insertRecord("device1", 4L, measurementsList, valuesList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testGetTimestampPrecision()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.getTimestampPrecision();
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    String multiSeriesId = "alignedDevice1";
    long time = 5L;
    List<String> multiMeasurementComponents = Arrays.asList("temperature", "humidity");
    List<TSDataType> types = Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32);
    List<Object> values = Arrays.asList(true, 11);
    sessionPool.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, types, values);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testInsertAlignedRecord2()
      throws IoTDBConnectionException, StatementExecutionException {
    String multiSeriesId = "alignedDevice1";
    long time = 5L;
    List<String> multiMeasurementComponents = Arrays.asList("temperature", "humidity");
    List<String> values = Arrays.asList("12ws", "11ws");
    sessionPool.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, values);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device1.temperature";
    sessionPool.deleteTimeseries(path);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteTimeseriesList()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device1.temperature", "root.device1.humidity");
    sessionPool.deleteTimeseries(paths);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device1.temperature";
    long time = 2L;
    sessionPool.deleteData(path, time);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteData2() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device1.temperature", "root.device1.humidity");
    long time = 3L;
    sessionPool.deleteData(paths, time);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteData3() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device1.temperature", "root.device1.humidity");
    sessionPool.deleteData(
        paths, System.currentTimeMillis() - 1000 * 60, System.currentTimeMillis());
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testSetStorageGroup() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.setStorageGroup("root.device1");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteStorageGroup()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteStorageGroup("root.device1");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteStorageGroups()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> sgs = Arrays.asList("root.device2", "root.device3");
    sessionPool.deleteStorageGroups(sgs);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateDatabase() throws IoTDBConnectionException, StatementExecutionException {
    String database = "root.device1.temperature";
    sessionPool.createDatabase(database);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteDatabase() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device2.humidity";
    sessionPool.deleteDatabase(path);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteDatabase2() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device2.temperature", "root.device2.humidity");
    sessionPool.deleteDatabases(paths);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device3.temperature";
    TSDataType dataType = TSDataType.BOOLEAN;
    TSEncoding encoding = TSEncoding.RLE;
    CompressionType compressor = CompressionType.SNAPPY;
    sessionPool.createTimeseries(path, dataType, encoding, compressor);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateTimeseries2() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device3.humidity";
    TSDataType dataType = TSDataType.BOOLEAN;
    TSEncoding encoding = TSEncoding.RLE;
    CompressionType compressor = CompressionType.SNAPPY;
    Map<String, String> props = new HashMap<>();
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "vt1");
    Map<String, String> attributes = new HashMap<>();
    attributes.put("att1", "av1");
    String measurementAlias = " atmosphere";
    sessionPool.createTimeseries(
        path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "device4";
    List<String> measurements = Arrays.asList("temperature", "humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.RLE);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    List<String> measurementAlias = Arrays.asList("centigrade degree", "atmosphere");
    sessionPool.createAlignedTimeseries(
        deviceId, measurements, dataTypes, encodings, compressors, measurementAlias);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateAlignedTimeseries2()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "device4";
    List<String> measurements = Arrays.asList("temperature", "humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.RLE);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    List<String> measurementAlias = Arrays.asList("centigrade degree", "atmosphere");
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("tag1", "v1");
    Map<String, String> tagMap2 = new HashMap<>();
    tagMap2.put("tag2", "v2");
    Map<String, String> attrMap = new HashMap<>();
    attrMap.put("attr1", "vt1");
    Map<String, String> attrMap2 = new HashMap<>();
    attrMap2.put("attr2", "vt2");
    List<Map<String, String>> tags = Arrays.asList(tagMap, tagMap2);
    List<Map<String, String>> attrs = Arrays.asList(attrMap, attrMap2);
    sessionPool.createAlignedTimeseries(
        deviceId, measurements, dataTypes, encodings, compressors, measurementAlias, tags, attrs);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device5.temperature", "root.device5.humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.RLE);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    List<Map<String, String>> propsList = new ArrayList<>();
    List<Map<String, String>> tagsList = Arrays.asList();
    List<Map<String, String>> attributesList = Arrays.asList();
    List<String> measurementAliasList = Arrays.asList("centigrade degree", "atmosphere");
    sessionPool.createMultiTimeseries(
        paths,
        dataTypes,
        encodings,
        compressors,
        propsList,
        tagsList,
        attributesList,
        measurementAliasList);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCheckTimeseriesExists()
      throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device5.temperature";
    sessionPool.checkTimeseriesExists(path);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    Template templateShareTime = new Template("template1", true);
    MeasurementNode measurementNode =
        new MeasurementNode(
            "root.ut0.sensor1",
            Enum.valueOf(TSDataType.class, "INT32"),
            Enum.valueOf(TSEncoding.class, "PLAIN"),
            Enum.valueOf(CompressionType.class, "SNAPPY"));
    templateShareTime.addToTemplate(measurementNode);
    sessionPool.createSchemaTemplate(templateShareTime);
    Template template = new Template("template2", false);
    measurementNode =
        new MeasurementNode(
            "root.ut0.sensor2",
            Enum.valueOf(TSDataType.class, "FLOAT"),
            Enum.valueOf(TSEncoding.class, "PLAIN"),
            Enum.valueOf(CompressionType.class, "SNAPPY"));
    template.addToTemplate(measurementNode);
    sessionPool.createSchemaTemplate(template);

    template = new Template("template1");
    InternalNode iNodeVector = new InternalNode("vector", true);
    MeasurementNode mNodeS1 =
        new MeasurementNode("s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS2 =
        new MeasurementNode("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);
    iNodeVector.addChild(mNodeS1);
    iNodeVector.addChild(mNodeS2);
    template.addToTemplate(iNodeVector);
    sessionPool.createSchemaTemplate(template);
    assertEquals(2, iNodeVector.getChildren().size());
    assertEquals(false, iNodeVector.getChildren().get("s1").isShareTime());
    iNodeVector.deleteChild(iNodeVector);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateSchemaTemplate2()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    List<String> measurements = Arrays.asList("root.ut.temperature", "root.ut.humidity");
    List<String> measurements1 = Arrays.asList("root.ut1.temperature", "root.ut1.humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    sessionPool.createSchemaTemplate(
        "template3", measurements, dataTypes, encodings, compressionTypes, false);
    sessionPool.createSchemaTemplate(
        "template4", measurements1, dataTypes, encodings, compressionTypes, true);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateSchemaTemplate3()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    List<String> schemaNames = Arrays.asList("schema1");
    List<List<String>> measurements =
        Arrays.asList(Arrays.asList("root.ut1.temperature", "root.ut1.humidity"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.INT32));
    List<List<TSEncoding>> encodings =
        Arrays.asList(Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN));
    List<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    sessionPool.createSchemaTemplate(
        "template3", schemaNames, measurements, dataTypes, encodings, compressionTypes);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testAddAlignedMeasurementsInTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    List<String> measurements = Arrays.asList("root.ut2.temperature", "root.ut2.humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    sessionPool.addAlignedMeasurementsInTemplate(
        "template3", measurements, dataTypes, encodings, compressionTypes);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testAddAlignedMeasurementInTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    sessionPool.addAlignedMeasurementInTemplate(
        "template4",
        "root.ut3.temperature",
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testAddUnalignedMeasurementsInTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    List<String> measurements = Arrays.asList("root.ut4.temperature", "root.ut4.humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.INT32);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    sessionPool.addUnalignedMeasurementsInTemplate(
        "template5", measurements, dataTypes, encodings, compressionTypes);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testAddUnalignedMeasurementInTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    sessionPool.addUnalignedMeasurementInTemplate(
        "template5",
        "root.ut5.temperature",
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDeleteNodeInTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    sessionPool.deleteNodeInTemplate("template1", "root.ut0.sensor1");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCountMeasurementsInTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.countMeasurementsInTemplate("template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testIsMeasurementInTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.isMeasurementInTemplate("template2", "root.ut0.sensor2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testIsPathExistInTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.isPathExistInTemplate("template2", "root.ut0.sensor2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testShowMeasurementsInTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.showMeasurementsInTemplate("template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testShowMeasurementsInTemplate2()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.showMeasurementsInTemplate("template2", "root.ut0.**");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testShowAllTemplates() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.showAllTemplates();
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testShowPathsTemplateSetOn()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.showPathsTemplateSetOn("template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testShowPathsTemplateUsingOn()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.showPathsTemplateUsingOn("template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testSortTablet() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device", schemas, timestamp, values, partBitMap, 2);
    sessionPool.sortTablet(tablet);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testSetSchemaTemplate() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.setSchemaTemplate("template2", "root.ut0.sensor2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testUnSetSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.unsetSchemaTemplate("root.ut0.sensor2", "template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testDropSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.dropSchemaTemplate("template2");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testCreateTimeseriesUsingSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> devicePaths = Arrays.asList("root.ut3", "root.ut4");
    sessionPool.createTimeseriesUsingSchemaTemplate(devicePaths);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteQueryStatement2()
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "show version";
    SessionDataSetWrapper sessionDataSetWrapper = null;
    SessionDataSet sessionDataSet =
        new SessionDataSet(
            sql,
            execResp.getColumns(),
            execResp.getDataTypeList(),
            null,
            queryId,
            statementId,
            client,
            0,
            execResp.queryResult,
            true,
            10,
            true,
            10,
            ZoneId.systemDefault(),
            1000,
            false,
            execResp.getColumnIndex2TsBlockColumnIndexList());
    Mockito.when(session.executeQueryStatement(any(String.class), eq(50)))
        .thenReturn(sessionDataSet);
    sessionDataSetWrapper = sessionPool.executeQueryStatement(sql, 50);
    sessionDataSetWrapper.setSessionDataSet(sessionDataSet);
    sessionPool.closeResultSet(sessionDataSetWrapper);
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteQueryStatement3()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.executeQueryStatement("show version");
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteNonQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.executeNonQueryStatement(
        "create timeseries root.test.g_0.d_7815.s_7818 WITH datatype=boolean, encoding=PLAIN");
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteRawDataQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.test.g_0.d_7815.s_7818");
    sessionPool.executeRawDataQuery(
        paths, System.currentTimeMillis() - 1000 * 60 * 24l, System.currentTimeMillis(), 50);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteLastDataQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.test.g_0.d_7815.s_7818");
    sessionPool.executeLastDataQuery(paths, System.currentTimeMillis() - 1000 * 60 * 24l);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteLastDataQuery2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.test.g_0.d_7815.s_7818");
    sessionPool.executeLastDataQuery(paths);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteLastDataQuery3()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.test.g_0.d_7815.s_7818");
    sessionPool.executeLastDataQuery(paths, System.currentTimeMillis() - 1000 * 60 * 24l, 50);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteLastDataQueryForOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("s_7817", "s_7818");
    sessionPool.executeLastDataQueryForOneDevice(
        "root.test.g_0", "root.test.g_0.d_7818", paths, true);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteAggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("s_7817", "s_7818");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.MAX_VALUE, TAggregationType.LAST_VALUE);
    sessionPool.executeAggregationQuery(paths, aggregations);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteAggregationQuery2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("s_7817", "s_7818");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.MAX_VALUE, TAggregationType.LAST_VALUE);
    sessionPool.executeAggregationQuery(
        paths,
        aggregations,
        System.currentTimeMillis() - 1000 * 60 * 24l,
        System.currentTimeMillis(),
        500);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testExecuteAggregationQuery3()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("s_7817", "s_7818");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.MAX_VALUE, TAggregationType.LAST_VALUE);
    sessionPool.executeAggregationQuery(
        paths,
        aggregations,
        System.currentTimeMillis() - 1000 * 60 * 24l,
        System.currentTimeMillis());
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testEexecuteAggregationQuery4()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("s_7817", "s_7818");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.MAX_VALUE, TAggregationType.LAST_VALUE);
    sessionPool.executeAggregationQuery(
        paths,
        aggregations,
        System.currentTimeMillis() - 1000 * 60 * 24l,
        System.currentTimeMillis(),
        500,
        500 * 1000);
    assertEquals(
        0,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testFetchAllConnections() throws IoTDBConnectionException {
    sessionPool.fetchAllConnections();
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test
  public void testGetBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.getBackupConfiguration();
    assertEquals(
        1,
        ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
  }

  @Test(expected = StatementExecutionException.class)
  public void testInsertAlignedTabletsWithStatementExecutionException()
      throws IoTDBConnectionException, StatementExecutionException {
    ISessionPool sessionPool = Mockito.mock(ISessionPool.class);
    Map<String, Tablet> tablets = new HashMap<>();
    boolean sorted = true;
    doThrow(new StatementExecutionException(""))
        .when(sessionPool)
        .insertAlignedTablets(tablets, sorted);
    sessionPool.insertAlignedTablets(tablets, sorted);
  }

  @Test
  public void testInsertRecords() throws Exception {
    // 调用 insertRecords 方法
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

  @Test
  public void testExecuteQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    String testSql = "select s2,s1,s0,s2 from root.vehicle.d0 ";

    List<String> columns = new ArrayList<>();
    columns.add("root.vehicle.d0.s2");
    columns.add("root.vehicle.d0.s1");
    columns.add("root.vehicle.d0.s0");
    columns.add("root.vehicle.d0.s2");

    List<String> dataTypeList = new ArrayList<>();
    dataTypeList.add("FLOAT");
    dataTypeList.add("INT64");
    dataTypeList.add("INT32");
    dataTypeList.add("FLOAT");

    List<Integer> columnIndex2TsBlockColumnIndexList = Arrays.asList(0, 1, 2, 3);

    Mockito.when(execResp.isSetColumns()).thenReturn(true);
    Mockito.when(execResp.getColumns()).thenReturn(columns);
    Mockito.when(execResp.isSetDataTypeList()).thenReturn(true);
    Mockito.when(execResp.getDataTypeList()).thenReturn(dataTypeList);
    Mockito.when(execResp.isSetOperationType()).thenReturn(true);
    Mockito.when(execResp.getOperationType()).thenReturn("QUERY");
    Mockito.when(execResp.isSetQueryId()).thenReturn(true);
    Mockito.when(execResp.getQueryId()).thenReturn(queryId);
    Mockito.when(execResp.isIgnoreTimeStamp()).thenReturn(false);
    Mockito.when(execResp.getColumnIndex2TsBlockColumnIndexList())
        .thenReturn(columnIndex2TsBlockColumnIndexList);

    SessionDataSet sessionDataSet =
        new SessionDataSet(
            testSql,
            execResp.getColumns(),
            execResp.getDataTypeList(),
            null,
            queryId,
            statementId,
            client,
            0,
            execResp.queryResult,
            true,
            10,
            true,
            10,
            ZoneId.systemDefault(),
            1000,
            false,
            execResp.getColumnIndex2TsBlockColumnIndexList());

    Mockito.when(session.executeQueryStatement(any(String.class))).thenReturn(sessionDataSet);

    SessionDataSetWrapper sessionDataSetWrapper = sessionPool.executeQueryStatement(testSql);
    List<String> columnNames = sessionDataSetWrapper.getColumnNames();
    List<String> columnTypes = sessionDataSetWrapper.getColumnTypes();
    SessionDataSet.DataIterator dataIterator = sessionDataSetWrapper.iterator();
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < columnNames.size(); i++) {
      result.append(columnNames.get(i));
      if (i < columnNames.size() - 1) {
        result.append(",");
      }
    }
    result.append("\n");
    for (int i = 0; i < columnTypes.size(); i++) {
      result.append(columnTypes.get(i));
      if (i < columnTypes.size() - 1) {
        result.append(",");
      }
    }
    result.append("\n");
    while (dataIterator.next()) {
      for (int i = 0; i < columnNames.size(); i++) {
        result.append(dataIterator.getObject(columnNames.get(i)));
        if (i < columnNames.size() - 1) {
          result.append(",");
        }
      }
      result.append("\n");
    }
    sessionDataSetWrapper.close();
    String exResult =
        "root.vehicle.d0.s2,root.vehicle.d0.s1,root.vehicle.d0.s0,root.vehicle.d0.s2\n"
            + "FLOAT,INT64,INT32,FLOAT\n"
            + "2.22,40000,null,2.22\n"
            + "3.33,null,null,3.33\n"
            + "4.44,null,null,4.44\n"
            + "null,50000,null,null\n"
            + "null,199,null,null\n"
            + "null,199,null,null\n"
            + "null,199,null,null\n"
            + "11.11,199,33333,11.11\n"
            + "1000.11,55555,22222,1000.11\n";
    Assert.assertEquals(exResult, result.toString());
  }

  private List<ByteBuffer> FakedFirstFetchTsBlockResult() {
    List<TSDataType> tsDataTypeList = new ArrayList<>();
    tsDataTypeList.add(TSDataType.FLOAT); // root.vehicle.d0.s2
    tsDataTypeList.add(TSDataType.INT64); // root.vehicle.d0.s1
    tsDataTypeList.add(TSDataType.INT32); // root.vehicle.d0.s0

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(tsDataTypeList);

    Object[][] input = {
      {
        2L, 2.22F, 40000L, null,
      },
      {
        3L, 3.33F, null, null,
      },
      {
        4L, 4.44F, null, null,
      },
      {
        50L, null, 50000L, null,
      },
      {
        100L, null, 199L, null,
      },
      {
        101L, null, 199L, null,
      },
      {
        103L, null, 199L, null,
      },
      {
        105L, 11.11F, 199L, 33333,
      },
      {
        1000L, 1000.11F, 55555L, 22222,
      }
    };
    for (int row = 0; row < input.length; row++) {
      tsBlockBuilder.getTimeColumnBuilder().writeLong((long) input[row][0]);
      if (input[row][1] != null) {
        tsBlockBuilder.getColumnBuilder(0).writeFloat((float) input[row][1]);
      } else {
        tsBlockBuilder.getColumnBuilder(0).appendNull();
      }
      if (input[row][2] != null) {
        tsBlockBuilder.getColumnBuilder(1).writeLong((long) input[row][2]);
      } else {
        tsBlockBuilder.getColumnBuilder(1).appendNull();
      }
      if (input[row][3] != null) {
        tsBlockBuilder.getColumnBuilder(2).writeInt((int) input[row][3]);
      } else {
        tsBlockBuilder.getColumnBuilder(2).appendNull();
      }

      tsBlockBuilder.declarePosition();
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = new TsBlockSerde().serialize(tsBlockBuilder.build());
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Collections.singletonList(tsBlock);
  }
}
