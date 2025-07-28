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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

public class SessionTest {

  @Mock private ISession session;

  @Mock private SessionConnection sessionConnection;

  @Before
  public void setUp() throws IoTDBConnectionException, StatementExecutionException {
    MockitoAnnotations.initMocks(this);
    session =
        new Session.Builder()
            .host("host")
            .port(11)
            .username("user")
            .password("pwd")
            .enableAutoFetch(false)
            .build();
    Whitebox.setInternalState(session, "defaultSessionConnection", sessionConnection);
    TSQueryTemplateResp resp = new TSQueryTemplateResp();
    resp.setMeasurements(Arrays.asList("root.sg1.d1.s1"));
    Mockito.when(sessionConnection.querySchemaTemplate(any())).thenReturn(resp);
    HashMap<String, TEndPoint> deviceIdToEndpoint = new HashMap<>();
    deviceIdToEndpoint.put("device1", new TEndPoint());
    deviceIdToEndpoint.put("device2", new TEndPoint());
    Whitebox.setInternalState(session, "deviceIdToEndpoint", deviceIdToEndpoint);
    HashMap<TEndPoint, SessionConnection> endPointToSessionConnection = new HashMap<>();
    endPointToSessionConnection.put(new TEndPoint(), sessionConnection);
    Whitebox.setInternalState(session, "endPointToSessionConnection", endPointToSessionConnection);
  }

  @After
  public void tearDown() throws IoTDBConnectionException {
    // Close the session pool after each test
    if (null != session) {
      session.close();
    }
  }

  @Test
  public void testBuildSession() {
    Session session1 =
        new Session.Builder()
            .nodeUrls(Arrays.asList("host:port"))
            .username("username")
            .password("pwd")
            .build();
    session1 =
        new Session.Builder()
            .host("host")
            .port(12)
            .username("username")
            .password("pwd")
            .fetchSize(1000)
            .zoneId(ZoneId.systemDefault())
            .enableRedirection(true)
            .enableRecordsAutoConvertTablet(true)
            .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
            .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
            .version(Version.V_0_13)
            .timeOut(500l)
            .build();
  }

  @Test
  public void testTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    String timeZone = session.getTimeZone();
    timeZone = "UTC";
    session.setTimeZone(timeZone);
    session.setTimeZoneOfSession(timeZone);
    assertEquals(timeZone, ((Session) session).zoneId.toString());
  }

  @Test
  public void testSetStorageGroup() throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup("root.sg1");
  }

  @Test
  public void testDeleteStorageGroup()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteStorageGroup("root.sg1");
  }

  @Test
  public void testDeleteStorageGroups()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteStorageGroups(Arrays.asList("root.sg1"));
  }

  @Test
  public void testCreateDatabase() throws IoTDBConnectionException, StatementExecutionException {
    session.createDatabase("root.sg1");
  }

  @Test
  public void testDeleteDatabase() throws IoTDBConnectionException, StatementExecutionException {
    session.deleteDatabase("root.sg1");
  }

  @Test
  public void testDeleteDatabases() throws IoTDBConnectionException, StatementExecutionException {
    session.deleteDatabases(Arrays.asList("root.sg1"));
  }

  @Test
  public void testCreateTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.device3.humidity";
    TSDataType dataType = TSDataType.BOOLEAN;
    TSEncoding encoding = TSEncoding.RLE;
    CompressionType compressor = CompressionType.SNAPPY;
    session.createTimeseries(path, dataType, encoding, compressor);
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.device3.humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.BOOLEAN);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE);
    List<CompressionType> compressors = Arrays.asList(CompressionType.SNAPPY);
    Map<String, String> props = new HashMap<>();
    List<Map<String, String>> propList = Arrays.asList(props);
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "vt1");
    List<Map<String, String>> tagsList = Arrays.asList(tags);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("att1", "av1");
    List<Map<String, String>> attributesList = Arrays.asList(attributes);
    List<String> measurementAliasList = Arrays.asList("atmosphere");
    session.createMultiTimeseries(
        paths,
        dataTypes,
        encodings,
        compressors,
        propList,
        tagsList,
        attributesList,
        measurementAliasList);
  }

  @Test
  public void testCheckTimeseriesExists()
      throws IoTDBConnectionException, StatementExecutionException {
    session.checkTimeseriesExists("root.sg1.d1.s1");
  }

  @Test
  public void testSetAndGetQueryTimeout() {
    long timeoutInMs = 5000l;
    session.setQueryTimeout(timeoutInMs);
    long queryTimeout = session.getQueryTimeout();
    Assert.assertEquals(timeoutInMs, queryTimeout);
  }

  @Test
  public void testInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = Arrays.asList("s1", "s2");
    List<TSDataType> types = Arrays.asList(TSDataType.TEXT, TSDataType.FLOAT);
    session.insertRecord("root.sg1.d1", 1691999031779l, measurements, types, "测试", 22.3f);
  }

  @Test
  public void testDeleteTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    session.deleteTimeseries("root.sg1.d1.s1");
  }

  @Test
  public void testDeleteTimeseriesList()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteTimeseries(Arrays.asList("root.sg1.d1.s1"));
  }

  @Test
  public void testInsertAlignedRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = Arrays.asList("s1", "s2");
    List<TSDataType> types = Arrays.asList(TSDataType.TEXT, TSDataType.FLOAT);
    List<Object> values = Arrays.asList("测试", 22.3f);
    session.insertAlignedRecord("root.sg1.d1", 1691999031779l, measurements, types, values);
    List<Object> values0 = Arrays.asList(null, 22.3f);
    session.insertAlignedRecord("root.sg1.d1", 1691999031779l, measurements, types, values0);
    List<String> values1 = Arrays.asList("测试", "22.3f");
    session.insertAlignedRecord("root.sg1.d1", 1691999031779l, measurements, values1);
    List<String> values2 = Arrays.asList("测试");
    session.insertAlignedRecord("root.sg1.d1", 1691999031779l, measurements, values2);
  }

  @Test
  public void testExecuteQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeQueryStatement("show version");
  }

  @Test
  public void testExecuteQueryStatementWithTimeout()
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeQueryStatement("show version", 500l);
  }

  @Test
  public void testExecuteNonQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "create timeseries root.温度检测.天气.a002 WITH DATATYPE=text, ENCODING=PLAIN,DEADBAND=SDT,COMPDEV=2;");
  }

  @Test
  public void testExecuteRawDataQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    session.executeRawDataQuery(paths, 2l, 10l, 500l);
  }

  @Test
  public void testExecuteLastDataQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    session.executeLastDataQuery(paths, 10l);
  }

  @Test
  public void testExecuteLastDataQueryTimeout()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    session.executeLastDataQuery(paths, 10l, 500l);
  }

  @Test
  public void testExecuteLastDataQueryWithPaths()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    session.executeLastDataQuery(paths);
  }

  @Test
  public void testExecuteAggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE);
    session.executeAggregationQuery(paths, aggregations);
  }

  @Test
  public void testExecuteAggregationQueryWithStartTimeEndTime()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE);
    session.executeAggregationQuery(paths, aggregations, 2l, 10l);
  }

  @Test
  public void testExecuteAggregationQueryWithInterval()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE);
    session.executeAggregationQuery(paths, aggregations, 2l, 10000l, 5000);
  }

  @Test
  public void testExecuteAggregationQueryWithIntervalSlidingStep()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE);
    session.executeAggregationQuery(paths, aggregations, 2l, 100000l, 5000, 5000);
  }

  @Test
  public void testCreateTimeseriesWithTag()
      throws IoTDBConnectionException, StatementExecutionException {
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
    session.createTimeseries(
        path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
  }

  @Test
  public void testCreateAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = Arrays.asList("temperature", "humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    List<String> measurementAlias = Arrays.asList("atmosphere", "centigrade");
    session.createAlignedTimeseries(
        "root.device3", measurements, dataTypes, encodings, compressors, measurementAlias);
  }

  @Test
  public void testCreateAlignedTimeseriesWithTags()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = Arrays.asList("temperature", "humidity");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "vt1");
    List<Map<String, String>> tagsList = Arrays.asList(tags);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("att1", "av1");
    List<Map<String, String>> attributesList = Arrays.asList(attributes);
    List<String> measurementAlias = Arrays.asList("atmosphere", "centigrade");
    session.createAlignedTimeseries(
        "root.device3",
        measurements,
        dataTypes,
        encodings,
        compressors,
        measurementAlias,
        tagsList,
        attributesList);
  }

  @Test
  public void testInsertRecordsDirectionException()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", true);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 3L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test
  public void testInsertRecordsNoDirectionException()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", false);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 3L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecordsException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords2Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test
  public void testInsertAlignedRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", false);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 6L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test
  public void testInsertAlignedRecordsEnableRedirection()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", true);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 6L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedRecordsException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedRecordsExceptionEnableRedirection()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedRecords2Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test
  public void testInsertAlignedRecordsWithTypeException()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", true);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 7L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test
  public void testInsertAlignedRecordsWithType2Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", false);
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L, 7L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords5Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesListObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords6Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords7Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords8Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesListObj);
  }

  @Test
  public void testInsertRecordsRawException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L, 3L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertStringRecordsOfOneDevice("device1", timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertRecords9Exception()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertStringRecordsOfOneDevice("device1", timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertStringRecordsOfOneDeviceSortedException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertStringRecordsOfOneDevice("device1", timeList, measurementsList, valuesList, true);
  }

  @Test
  public void testInsertAlignedRecordsOfOneDeviceRawException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L, 3L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesListObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedRecordsOfOneDeviceSortedException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.insertAlignedRecordsOfOneDevice(
        "device1", timeList, measurementsList, typesList, valuesListObj, false);
  }

  @Test
  public void testInsertAlignedStringRecordsOfOneDeviceRaw()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L, 3L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedStringRecordsOfOneDeviceException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertAlignedStringRecordsOfOneDeviceSortedException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.insertAlignedStringRecordsOfOneDevice(
        "device1", timeList, measurementsList, valuesList, true);
  }

  @Test
  public void testTestInsertRecordsException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<String>> valuesList =
        Arrays.asList(Arrays.asList("11", "12"), Arrays.asList("10", "11"));
    session.testInsertRecords(deviceIds, timeList, measurementsList, valuesList);
  }

  @Test
  public void testTestInsertRecordsObjectException()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> deviceIds = Arrays.asList("device1", "device2");
    List<Long> timeList = Arrays.asList(2L);
    List<List<String>> measurementsList =
        Arrays.asList(
            Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
    List<List<Object>> valuesListObj =
        Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
    List<List<TSDataType>> typesList =
        Arrays.asList(
            Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    session.testInsertRecords(deviceIds, timeList, measurementsList, typesList, valuesListObj);
  }

  @Test
  public void testTestInsertRecordException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.testInsertRecord(
        "device1", 1L, Arrays.asList("temperature", "humidity"), Arrays.asList("220.0", "1.5"));
  }

  @Test
  public void testTestInsertRecordWithDataTypeException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.testInsertRecord(
        "device1",
        1L,
        Arrays.asList("temperature", "humidity"),
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
        Arrays.asList(220.0f, 1.5f));
  }

  @Test
  public void testDeleteDataException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteData("root.sg1.d1.s1", System.currentTimeMillis());
  }

  @Test
  public void testDeleteDataListException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteData(Arrays.asList("root.sg1.d1.s1"), System.currentTimeMillis());
  }

  @Test
  public void testDeleteDataListWithStartTimeAndEndTimeException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteData(
        Arrays.asList("root.sg1.d1.s1"),
        System.currentTimeMillis() - 1000 * 60 * 20,
        System.currentTimeMillis());
  }

  @Test
  public void testSetSchemaTemplateException()
      throws IoTDBConnectionException, StatementExecutionException {
    session.setSchemaTemplate("template1", "prefixPath");
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
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertTablet(tablet);
  }

  @Test
  public void testInsertTabletOutOfOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {5l, 2l};
    boolean[][] values = new boolean[][] {{true, false}, {true, false}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertTablet(tablet);
  }

  @Test
  public void testInsertAlignedTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    int[][] values = new int[][] {{12, 22}, {14, 34}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertAlignedTablet(tablet);
  }

  @Test
  public void testInsertAlignedTablet2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    int[][] values = new int[][] {{12, 22}, {14, 34}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertAlignedTablet(tablet);
  }

  @Test
  public void testInsertTabletsSorted()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {2l, 1l};
    int[][] values = new int[][] {{34, 42}, {40, 42}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertTablets(map);
    session.insertTablets(map, true);
    session.insertAlignedTablets(map);
    session.insertAlignedTablets(map, true);
    session.testInsertTablet(tablet);
    session.testInsertTablet(tablet, true);
    session.testInsertTablets(map);
    session.testInsertTablets(map, true);
  }

  @Test
  public void testInsertAlignedTablets()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.FLOAT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {2l, 1l};
    float[][] values = new float[][] {{1.1f, 1.0f}, {1.2f, 1.0f}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertAlignedTablets(map);
  }

  @Test
  public void testInsertAlignedTabletsSorted()
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.DOUBLE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {2l, 1l};
    double[][] values = new double[][] {{22.2, 22.0}, {21.5, 23.0}};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertAlignedTablets(map, false);
  }

  @Test
  public void testInsertAlignedTabletsSortedEnableRedirection()
      throws IoTDBConnectionException, StatementExecutionException {
    Whitebox.setInternalState(session, "enableRedirection", false);
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.TEXT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {2l, 3l};
    Binary[][] values =
        new Binary[][] {
          {
            new Binary("test", TSFileConfig.STRING_CHARSET),
            new Binary("test2", TSFileConfig.STRING_CHARSET)
          },
          {
            new Binary("test", TSFileConfig.STRING_CHARSET),
            new Binary("test1", TSFileConfig.STRING_CHARSET)
          }
        };
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    Map<String, Tablet> map = new HashMap<>();
    map.put("one", tablet);
    session.insertAlignedTablets(map, false);
    session.setEnableRedirection(true);
    Assert.assertTrue(session.isEnableRedirection());
    session.setEnableQueryRedirection(true);
    Assert.assertTrue(session.isEnableQueryRedirection());
  }

  @Test
  public void testTestInsertTablet() throws IoTDBConnectionException, StatementExecutionException {
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
    session.testInsertTablet(tablet);
  }

  @Test
  public void testTestInsertTabletSorted()
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
    session.testInsertTablet(tablet, true);
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
    session.testInsertTablets(map);
  }

  @Test
  public void testTestInsertTabletsSorted()
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
    session.testInsertTablets(map, true);
  }

  @Test
  public void testCreateSchemaTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    List<String> measurement = Arrays.asList("root.ut1.temperature", "root.ut1.humidity");
    session.createSchemaTemplate(
        "template4",
        measurement,
        Arrays.asList(TSDataType.FLOAT, TSDataType.INT32),
        Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN),
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
        true);
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateSchemaTemplate2()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> schemaNames = Arrays.asList("schema1");
    List<List<String>> measurements =
        Arrays.asList(Arrays.asList("root.ut1.temperature", "root.ut1.humidity"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.INT32));
    List<List<TSEncoding>> encodings =
        Arrays.asList(Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN));
    List<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    session.createSchemaTemplate(
        "template3", schemaNames, measurements, dataTypes, encodings, compressionTypes);
  }

  @Test
  public void testAddAlignedMeasurementsInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.addAlignedMeasurementsInTemplate(
        "template1",
        Arrays.asList("root.sg1.d1.s1"),
        Arrays.asList(TSDataType.INT64),
        Arrays.asList(TSEncoding.PLAIN),
        Arrays.asList(CompressionType.SNAPPY));
  }

  @Test
  public void testAddAlignedMeasurementInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.addAlignedMeasurementInTemplate(
        "template1", "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
  }

  @Test
  public void testAddUnalignedMeasurementsInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.addUnalignedMeasurementsInTemplate(
        "template1",
        Arrays.asList("root.sg1.d1.s1"),
        Arrays.asList(TSDataType.INT64),
        Arrays.asList(TSEncoding.PLAIN),
        Arrays.asList(CompressionType.SNAPPY));
  }

  @Test
  public void testAddUnalignedMeasurementInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.addUnalignedMeasurementInTemplate(
        "template1", "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
  }

  @Test
  public void testDeleteNodeInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.deleteNodeInTemplate("template1", "root.sg1.d1.s1");
  }

  @Test
  public void testCountMeasurementsInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.countMeasurementsInTemplate("template1");
  }

  @Test
  public void testIsMeasurementInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.isMeasurementInTemplate("template1", "root.sg1.d1.s1");
  }

  @Test
  public void testIsPathExistInTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    session.isPathExistInTemplate("template1", "root.sg1.d1.s1");
  }

  @Test
  public void testShowMeasurementsInTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    session.showMeasurementsInTemplate("template1");
  }

  @Test
  public void testShowMeasurementsInTemplatePattern()
      throws IoTDBConnectionException, StatementExecutionException {
    session.showMeasurementsInTemplate("template1", "root.sg1.**");
  }

  @Test
  public void testShowAllTemplates() throws IoTDBConnectionException, StatementExecutionException {
    session.showAllTemplates();
  }

  @Test
  public void testShowPathsTemplateSetOn()
      throws IoTDBConnectionException, StatementExecutionException {
    session.showPathsTemplateSetOn("template1");
  }

  @Test
  public void testShowPathsTemplateUsingOn()
      throws IoTDBConnectionException, StatementExecutionException {
    session.showPathsTemplateUsingOn("template1");
  }

  @Test
  public void testUnsetSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    session.unsetSchemaTemplate("root.sg1.d1.**", "template1");
  }

  @Test
  public void testDropSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    session.dropSchemaTemplate("template1");
  }

  @Test
  public void testCreateTimeseriesUsingSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    session.createTimeseriesUsingSchemaTemplate(Arrays.asList("root.sg1.d1", "root.sg1.d2"));
  }

  @Test
  public void testFetchAllConnections() throws IoTDBConnectionException {
    session.fetchAllConnections();
  }

  @Test
  public void testGetBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    session.getBackupConfiguration();
  }

  @Test
  public void testEmptyNodeUrls() {
    try {
      ISession failedSession = new Session(Collections.emptyList(), "root", "root");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("nodeUrls shouldn't be empty.", e.getMessage());
    }
  }

  @Test
  public void testTimeoutUsingBuilder() {
    ISession session1 = new Session.Builder().timeOut(1).build();
    assertEquals(1L, session1.getQueryTimeout());
  }
}
