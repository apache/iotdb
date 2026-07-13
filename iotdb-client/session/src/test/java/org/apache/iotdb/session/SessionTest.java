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
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
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
import org.mockito.ArgumentCaptor;
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
import static org.mockito.ArgumentMatchers.anyList;

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
            .nodeUrls(Collections.nCopies(2, "host:port"))
            .username("username")
            .password("pwd")
            .build();
    session1 =
        new Session.Builder()
            .nodeUrls(Collections.unmodifiableList(Arrays.asList("host:port1", "host:port2")))
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
  public void testMergeRelationalTabletsWithHighDuplicatedColumns() throws Exception {
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1,
            "d1",
            11L,
            1.1);
    final Tablet second =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2,
            "d2",
            22L,
            true);

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, second));

    assertEquals(1, mergedTablets.size());
    final Tablet mergedTablet = mergedTablets.get(0);
    assertEquals("table1", mergedTablet.getTableName());
    assertEquals(2, mergedTablet.getRowSize());
    assertEquals(Arrays.asList("tag1", "s1", "s2", "s3"), getMeasurementNames(mergedTablet));
    assertEquals(1L, mergedTablet.getTimestamp(0));
    assertEquals(2L, mergedTablet.getTimestamp(1));
    assertEquals(11L, mergedTablet.getValue(0, 1));
    assertEquals(22L, mergedTablet.getValue(1, 1));
    assertEquals(1.1, (double) mergedTablet.getValue(0, 2), 0.001);
    Assert.assertNull(mergedTablet.getBitMaps()[0]);
    Assert.assertNull(mergedTablet.getBitMaps()[1]);
    Assert.assertTrue(mergedTablet.isNull(1, 2));
    Assert.assertTrue(mergedTablet.isNull(0, 3));
    Assert.assertTrue((boolean) mergedTablet.getValue(1, 3));
  }

  @Test
  public void testMergeRelationalTabletsDoesNotCrossUnmergeableTablet() throws Exception {
    final List<String> firstMeasurements =
        Arrays.asList("tag1", "color", "sticky", "s1", "s2", "s3");
    final List<String> secondMeasurements =
        Arrays.asList("tag1", "color", "sticky", "s4", "s5", "s6");
    final List<TSDataType> dataTypes =
        Arrays.asList(
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.ATTRIBUTE,
            ColumnCategory.ATTRIBUTE,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    final Tablet first =
        createRelationalTablet(
            "table1",
            firstMeasurements,
            dataTypes,
            columnTypes,
            1,
            "d1",
            "red",
            "keep",
            11L,
            12L,
            13L);
    final Tablet second =
        createRelationalTablet(
            "table1",
            secondMeasurements,
            dataTypes,
            columnTypes,
            2,
            "d1",
            "blue",
            "replace",
            21L,
            22L,
            23L);
    final Tablet third =
        createRelationalTablet(
            "table1",
            firstMeasurements,
            dataTypes,
            columnTypes,
            3,
            "d1",
            "red",
            "keep",
            31L,
            32L,
            33L);

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(
            session, "mergeRelationalTablets", Arrays.asList(first, second, third));

    assertEquals(3, mergedTablets.size());
    Assert.assertSame(first, mergedTablets.get(0));
    Assert.assertSame(second, mergedTablets.get(1));
    Assert.assertSame(third, mergedTablets.get(2));
  }

  @Test
  public void testMergeRelationalTabletsDoesNotReorderTabletTimeRanges() throws Exception {
    final List<String> measurements = Arrays.asList("tag1", "color", "s1");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);
    final Tablet first =
        createRelationalTablet(
            "table1", measurements, dataTypes, columnTypes, 10, "d1", "red", 10L);
    final Tablet second =
        createRelationalTablet("table1", measurements, dataTypes, columnTypes, 1, "d1", "blue", 1L);

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, second));

    assertEquals(2, mergedTablets.size());
    Assert.assertSame(first, mergedTablets.get(0));
    Assert.assertSame(second, mergedTablets.get(1));
  }

  @Test
  public void testMergeRelationalTabletsChecksColumnsAddedByEarlierTablets() throws Exception {
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1,
            "d1",
            11L,
            1.1);
    final Tablet second =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2,
            "d2",
            22L,
            true);
    final Tablet conflicting =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            3,
            "d3",
            33L,
            3);

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(
            session, "mergeRelationalTablets", Arrays.asList(first, second, conflicting));

    assertEquals(2, mergedTablets.size());
    assertEquals(2, mergedTablets.get(0).getRowSize());
    Assert.assertSame(conflicting, mergedTablets.get(1));
  }

  @Test(timeout = 10000)
  public void testMergeLargeRelationalTabletList() throws Exception {
    final int tabletCount = 5000;
    final List<Tablet> tablets = new ArrayList<>(tabletCount);
    for (int i = 0; i < tabletCount; i++) {
      tablets.add(
          createRelationalTablet(
              "table1",
              Arrays.asList("tag1", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT64),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
              i,
              "d" + i,
              (long) i));
    }

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", tablets);

    assertEquals(1, mergedTablets.size());
    assertEquals(tabletCount, mergedTablets.get(0).getRowSize());
    assertEquals((long) tabletCount - 1, mergedTablets.get(0).getValue(tabletCount - 1, 1));
  }

  @Test
  public void testMergeRelationalTabletsSkipLowDuplicatedColumnsAndDifferentTables()
      throws Exception {
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1,
            "d1",
            11L,
            1.1);
    final Tablet second =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s3", "s4"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2,
            "d2",
            22,
            true);
    final Tablet third =
        createRelationalTablet(
            "table2",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            3,
            "d3",
            33L,
            3.3);
    final Tablet fourth =
        createRelationalTablet(
            "table0",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            4,
            "d4",
            44L,
            4.4);

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(
            session, "mergeRelationalTablets", Arrays.asList(first, second, third, fourth));

    assertEquals(4, mergedTablets.size());
    Assert.assertSame(fourth, mergedTablets.get(0));
    Assert.assertSame(first, mergedTablets.get(1));
    Assert.assertSame(second, mergedTablets.get(2));
    Assert.assertSame(third, mergedTablets.get(3));
  }

  @Test
  public void testMergeRelationalTabletsStopAfterConsecutiveMisses() throws Exception {
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1,
            "d1",
            11L,
            1.1);
    final Tablet second =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s3", "s4"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2,
            "d2",
            22,
            true);

    for (int i = 0; i < 10; i++) {
      final List<Tablet> mergedTablets =
          Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, second));
      assertEquals(2, mergedTablets.size());
    }

    final Tablet mergeable =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            3,
            "d3",
            33L,
            false);
    final List<Tablet> tablets = Arrays.asList(first, mergeable);
    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", tablets);
    Assert.assertSame(tablets, mergedTablets);
  }

  @Test
  public void testMergeRelationalTabletsResetConsecutiveMissesAfterMerge() throws Exception {
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1,
            "d1",
            11L,
            1.1);
    final Tablet unmergeable =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s3", "s4"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2,
            "d2",
            22,
            true);
    final Tablet mergeable =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            3,
            "d3",
            33L,
            false);

    for (int i = 0; i < 9; i++) {
      Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, unmergeable));
    }
    final List<Tablet> firstMergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, mergeable));
    assertEquals(1, firstMergedTablets.size());
    for (int i = 0; i < 9; i++) {
      Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, unmergeable));
    }

    final List<Tablet> mergedTablets =
        Whitebox.invokeMethod(session, "mergeRelationalTablets", Arrays.asList(first, mergeable));
    assertEquals(1, mergedTablets.size());
  }

  @Test
  public void testMergeTabletsDisabledWhenMergeCostExceedsHalfOfInsertCost() throws Exception {
    for (int i = 0; i < 9; i++) {
      Whitebox.invokeMethod(session, "recordMergeTabletsCost", 11L, 20L);
      Assert.assertTrue(Whitebox.getInternalState(session, "enableMergeTablets"));
    }

    Whitebox.invokeMethod(session, "recordMergeTabletsCost", 11L, 20L);

    Assert.assertFalse(Whitebox.getInternalState(session, "enableMergeTablets"));
  }

  @Test
  public void testMergeTabletsKeepsEnabledWhenMergeCostIsNotTooHigh() throws Exception {
    for (int i = 0; i < 10; i++) {
      Whitebox.invokeMethod(session, "recordMergeTabletsCost", 5L, 20L);
    }

    Assert.assertTrue(Whitebox.getInternalState(session, "enableMergeTablets"));
    assertEquals(0, (int) Whitebox.getInternalState(session, "mergeTabletsPerformanceCheckCount"));
    assertEquals(0L, (long) Whitebox.getInternalState(session, "mergeTabletsCostInNanos"));
    assertEquals(0L, (long) Whitebox.getInternalState(session, "insertTabletsCostInNanos"));
  }

  @Test
  public void testMergeTabletsCostNotRecordedWhenInsertFails() throws Exception {
    Mockito.doThrow(new StatementExecutionException("expected"))
        .when(sessionConnection)
        .insertTablets(any(TSInsertTabletsReq.class), anyList());
    final Tablet first =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s2"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            1L,
            "tag1",
            11L,
            1.1);
    final Tablet second =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            2L,
            "tag2",
            22L,
            true);

    try {
      ((Session) session).insertRelationalTablets(Arrays.asList(first, second));
      fail("Exception expected");
    } catch (StatementExecutionException e) {
      assertEquals("expected", e.getMessage());
    }

    assertEquals(0, (int) Whitebox.getInternalState(session, "mergeTabletsPerformanceCheckCount"));
    assertEquals(0L, (long) Whitebox.getInternalState(session, "mergeTabletsCostInNanos"));
    assertEquals(0L, (long) Whitebox.getInternalState(session, "insertTabletsCostInNanos"));
  }

  @Test
  public void testInsertRelationalTabletsUseTableModelLeaderCache() throws Exception {
    final Tablet tablet =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1L,
            "tag1",
            11L);
    final SessionConnection redirectedSessionConnection = Mockito.mock(SessionConnection.class);
    final TEndPoint endPoint = new TEndPoint("127.0.0.2", 6667);
    final Map<IDeviceID, TEndPoint> tableModelDeviceIdToEndpoint = new HashMap<>();
    tableModelDeviceIdToEndpoint.put(tablet.getDeviceID(0), endPoint);
    final Map<TEndPoint, SessionConnection> endPointToSessionConnection = new HashMap<>();
    endPointToSessionConnection.put(endPoint, redirectedSessionConnection);
    Whitebox.setInternalState(session, "enableRedirection", true);
    Whitebox.setInternalState(
        session, "tableModelDeviceIdToEndpoint", tableModelDeviceIdToEndpoint);
    Whitebox.setInternalState(session, "endPointToSessionConnection", endPointToSessionConnection);

    ((Session) session).insertRelationalTablets(Collections.singletonList(tablet));

    Mockito.verify(redirectedSessionConnection)
        .insertTablets(any(TSInsertTabletsReq.class), anyList());
    Mockito.verify(sessionConnection, Mockito.never())
        .insertTablets(any(TSInsertTabletsReq.class), anyList());
  }

  @Test
  public void testInsertRelationalTabletsUpdatesTableModelLeaderCache() throws Exception {
    final Tablet tablet =
        createRelationalTablet(
            "table1",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1L,
            "tag1",
            11L);
    final IDeviceID deviceID = tablet.getDeviceID(0);
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.2", 6667);
    final Map<TEndPoint, SessionConnection> endPointToSessionConnection = new HashMap<>();
    endPointToSessionConnection.put(redirectEndPoint, Mockito.mock(SessionConnection.class));
    Whitebox.setInternalState(session, "enableRedirection", true);
    Whitebox.setInternalState(session, "tableModelDeviceIdToEndpoint", new HashMap<>());
    Whitebox.setInternalState(session, "endPointToSessionConnection", endPointToSessionConnection);
    Mockito.doThrow(
            new RedirectException(Collections.singletonMap(deviceID.toString(), redirectEndPoint)))
        .when(sessionConnection)
        .insertTablets(any(TSInsertTabletsReq.class), anyList());

    ((Session) session).insertRelationalTablets(Collections.singletonList(tablet));

    final Map<IDeviceID, TEndPoint> tableModelDeviceIdToEndpoint =
        Whitebox.getInternalState(session, "tableModelDeviceIdToEndpoint");
    assertEquals(redirectEndPoint, tableModelDeviceIdToEndpoint.get(deviceID));
    Mockito.verify(sessionConnection).insertTablets(any(TSInsertTabletsReq.class), anyList());
  }

  @Test
  public void testSplitRelationalTabletAllocatesExactCapacityPerDevice() throws Exception {
    final int rowCount = 1000;
    final List<String> measurements = Arrays.asList("tag1", "s1");
    final Tablet tablet =
        new Tablet(
            "table1",
            measurements,
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            rowCount);
    for (int row = 0; row < rowCount; row++) {
      tablet.addTimestamp(row, row);
      tablet.addValue(measurements.get(0), row, "d" + row);
      tablet.addValue(measurements.get(1), row, (long) row);
    }
    Whitebox.setInternalState(session, "tableModelDeviceIdToEndpoint", new HashMap<>());
    final Map<SessionConnection, Object> tabletGroup = new HashMap<>();

    Whitebox.invokeMethod(session, "addRelationalTabletToGroup", tabletGroup, tablet);

    assertEquals(1, tabletGroup.size());
    final List<Tablet> groupedTablets =
        Whitebox.getInternalState(tabletGroup.values().iterator().next(), "tablets");
    assertEquals(rowCount, groupedTablets.size());
    int totalCapacity = 0;
    for (final Tablet groupedTablet : groupedTablets) {
      assertEquals(1, groupedTablet.getMaxRowNumber());
      assertEquals(1, groupedTablet.getRowSize());
      totalCapacity += groupedTablet.getMaxRowNumber();
    }
    assertEquals(rowCount, totalCapacity);
  }

  @Test
  public void testInsertRelationalTabletsPreservesOrderWhenSplittingByDevice() throws Exception {
    final List<String> measurements = Arrays.asList("tag1", "color", "s1");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);
    final Tablet first =
        createRelationalTablet(
            "table1",
            measurements,
            dataTypes,
            columnTypes,
            new long[] {1, 101},
            new Object[][] {{"d1", "red", 11L}, {"d2", "green", 12L}});
    final Tablet second =
        createRelationalTablet(
            "table1", measurements, dataTypes, columnTypes, 2, "d1", "blue", 21L);
    final Tablet third =
        createRelationalTablet(
            "table1",
            measurements,
            dataTypes,
            columnTypes,
            new long[] {3, 103},
            new Object[][] {{"d1", null, 31L}, {"d2", "black", 32L}});
    Whitebox.setInternalState(session, "enableMergeTablets", false);
    Whitebox.setInternalState(session, "enableRedirection", true);
    Whitebox.setInternalState(
        session, "tableModelDeviceIdToEndpoint", new HashMap<IDeviceID, TEndPoint>());

    ((Session) session).insertRelationalTablets(Arrays.asList(first, second, third));

    final ArgumentCaptor<TSInsertTabletsReq> requestCaptor =
        ArgumentCaptor.forClass(TSInsertTabletsReq.class);
    Mockito.verify(sessionConnection).insertTablets(requestCaptor.capture(), anyList());
    final TSInsertTabletsReq request = requestCaptor.getValue();
    assertEquals(Arrays.asList(1, 1, 1, 1, 1), request.getSizeList());
    final List<Long> firstTimestamps = new ArrayList<>();
    for (int i = 0; i < request.getTimestampsListSize(); i++) {
      firstTimestamps.add(request.getTimestampsList().get(i).getLong(0));
    }
    assertEquals(Arrays.asList(1L, 101L, 2L, 3L, 103L), firstTimestamps);
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

  @Test
  public void testEnableMergeTabletsUsingBuilder() {
    Session sessionWithMergeTabletsDisabled =
        new Session.Builder().enableMergeTablets(false).build();
    Assert.assertFalse(
        Whitebox.getInternalState(sessionWithMergeTabletsDisabled, "enableMergeTablets"));
  }

  private Tablet createRelationalTablet(
      final String tableName,
      final List<String> measurements,
      final List<TSDataType> dataTypes,
      final List<ColumnCategory> columnTypes,
      final long timestamp,
      final Object... values) {
    final Tablet tablet = new Tablet(tableName, measurements, dataTypes, columnTypes, 1);
    tablet.addTimestamp(0, timestamp);
    for (int i = 0; i < values.length; i++) {
      tablet.addValue(measurements.get(i), 0, values[i]);
    }
    return tablet;
  }

  private Tablet createRelationalTablet(
      final String tableName,
      final List<String> measurements,
      final List<TSDataType> dataTypes,
      final List<ColumnCategory> columnTypes,
      final long[] timestamps,
      final Object[][] values) {
    final Tablet tablet =
        new Tablet(tableName, measurements, dataTypes, columnTypes, timestamps.length);
    for (int row = 0; row < timestamps.length; row++) {
      tablet.addTimestamp(row, timestamps[row]);
      for (int column = 0; column < values[row].length; column++) {
        tablet.addValue(measurements.get(column), row, values[row][column]);
      }
    }
    return tablet;
  }

  private List<String> getMeasurementNames(final Tablet tablet) {
    final List<String> measurementNames = new ArrayList<>();
    for (final IMeasurementSchema schema : tablet.getSchemas()) {
      measurementNames.add(schema.getMeasurementName());
    }
    return measurementNames;
  }
}
