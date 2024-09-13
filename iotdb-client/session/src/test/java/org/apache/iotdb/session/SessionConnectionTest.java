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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TCreateTimeseriesUsingSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSAppendSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSDropSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class SessionConnectionTest {

  @Mock private SessionConnection sessionConnection;
  @Mock private TTransport transport;
  @Mock private IClientRPCService.Iface client;

  @Mock private Session session;

  @Before
  public void setUp() throws IoTDBConnectionException, StatementExecutionException, TException {
    MockitoAnnotations.initMocks(this);
    sessionConnection = new SessionConnection();
    Whitebox.setInternalState(sessionConnection, "transport", transport);
    Whitebox.setInternalState(sessionConnection, "client", client);
    session =
        new Session.Builder()
            .nodeUrls(Collections.singletonList("127.0.0.1:12"))
            .username("root")
            .password("root")
            .enableAutoFetch(false)
            .build();
    Whitebox.setInternalState(sessionConnection, "session", session);
    Mockito.when(transport.isOpen()).thenReturn(true);
    TSStatus tsStatus = new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    TSStatus tsStatusSuccess = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    tsStatus.setSubStatus(
        Arrays.asList(new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())));
    Mockito.when(client.setStorageGroup(anyLong(), anyString())).thenReturn(tsStatus);
    TSExecuteStatementResp execResp = new TSExecuteStatementResp(tsStatus);
    execResp.setColumns(Arrays.asList());
    execResp.setDataTypeList(Arrays.asList());
    execResp.setQueryId(1l);
    execResp.setQueryResult(Arrays.asList());
    execResp.setIgnoreTimeStamp(false);
    execResp.setMoreData(false);
    Mockito.when(client.executeQueryStatementV2(any())).thenReturn(execResp);
    Mockito.when(client.executeFastLastDataQueryForOneDeviceV2(any())).thenReturn(execResp);
    Mockito.when(client.executeLastDataQueryV2(any())).thenReturn(execResp);
    Mockito.when(client.deleteStorageGroups(anyLong(), any())).thenReturn(tsStatus);
    Mockito.when(client.createTimeseries(any())).thenReturn(tsStatus);
    Mockito.when(client.createAlignedTimeseries(any())).thenReturn(tsStatus);
    Mockito.when(client.createMultiTimeseries(any())).thenReturn(tsStatus);
    Mockito.when(client.executeUpdateStatementV2(any())).thenReturn(execResp);
    Mockito.when(client.closeOperation(any())).thenReturn(tsStatus);
    Mockito.when(client.executeAggregationQueryV2(any())).thenReturn(execResp);
    Mockito.when(client.executeRawDataQueryV2(any())).thenReturn(execResp);
    Mockito.when(client.insertRecord(any())).thenReturn(tsStatus);
    Mockito.when(client.testInsertRecord(any())).thenReturn(tsStatus);
    Mockito.when(client.insertRecords(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.testInsertRecords(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.insertStringRecord(any())).thenReturn(tsStatus);
    Mockito.when(client.testInsertStringRecord(any())).thenReturn(tsStatus);
    Mockito.when(client.insertStringRecords(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.testInsertStringRecords(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.insertRecordsOfOneDevice(any())).thenReturn(tsStatus);
    Mockito.when(client.insertStringRecordsOfOneDevice(any())).thenReturn(tsStatus);
    Mockito.when(client.deleteTimeseries(anyLong(), any())).thenReturn(tsStatus);
    Mockito.when(client.insertTablet(any())).thenReturn(tsStatus);
    Mockito.when(client.testInsertTablet(any())).thenReturn(tsStatus);
    Mockito.when(client.insertTablets(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.testInsertTablets(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.deleteData(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.createSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.appendSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.pruneSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.querySchemaTemplate(any()))
        .thenReturn(new TSQueryTemplateResp(tsStatusSuccess, 1));
    Mockito.when(client.setSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.unsetSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.dropSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.createTimeseriesUsingSchemaTemplate(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.getBackupConfiguration())
        .thenReturn(new TSBackupConfigurationResp(tsStatusSuccess));
    Mockito.when(client.fetchAllConnectionsInfo()).thenReturn(new TSConnectionInfoResp());
    Mockito.when(client.setTimeZone(any())).thenReturn(tsStatusSuccess);
    Mockito.when(client.closeSession(any())).thenReturn(tsStatusSuccess);
  }

  @After
  public void close() throws IoTDBConnectionException {
    sessionConnection.close();
  }

  @Test(expected = NumberFormatException.class)
  public void testBuildSessionConnection() throws IoTDBConnectionException {
    session =
        new Session.Builder()
            .host("local")
            .port(12)
            .username("root")
            .password("root")
            .enableAutoFetch(false)
            .build();
    SessionConnection sessionConnection1 =
        new SessionConnection(
            session,
            ZoneId.systemDefault(),
            () -> Collections.singletonList(new TEndPoint("local", 12)),
            SessionConfig.MAX_RETRY_COUNT,
            SessionConfig.RETRY_INTERVAL_IN_MS,
            "tree",
            null);
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testBuildSessionConnection2() throws IoTDBConnectionException {
    session =
        new Session.Builder()
            .host("local")
            .port(12)
            .username("root")
            .password("root")
            .enableAutoFetch(false)
            .build();
    SessionConnection sessionConnection1 =
        new SessionConnection(
            session,
            new TEndPoint("localhost", 1234),
            ZoneId.systemDefault(),
            () -> Collections.singletonList(new TEndPoint("local", 12)),
            SessionConfig.MAX_RETRY_COUNT,
            SessionConfig.RETRY_INTERVAL_IN_MS,
            "tree",
            null);
  }

  @Test
  public void testSetStorageGroup() throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.setTimeZone(ZoneId.systemDefault().getId());
    sessionConnection.setStorageGroup("root.test1");
  }

  @Test
  public void testDeleteStorageGroups()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.deleteStorageGroups(Arrays.asList("root.test1"));
  }

  @Test
  public void testCreateTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.createTimeseries(new TSCreateTimeseriesReq());
  }

  @Test(expected = BatchExecutionException.class)
  public void testCreateTimeseriesRedirect()
      throws IoTDBConnectionException, StatementExecutionException, TException {
    TSStatus tsStatus = new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode());
    tsStatus.setSubStatus(Arrays.asList(new TSStatus()));
    Mockito.when(client.createTimeseries(any())).thenReturn(tsStatus);
    sessionConnection.createTimeseries(new TSCreateTimeseriesReq());
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTimeseriesException()
      throws IoTDBConnectionException, StatementExecutionException, TException {
    TSStatus tsStatus = new TSStatus(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
    tsStatus.setSubStatus(Arrays.asList(new TSStatus()));
    Mockito.when(client.createTimeseries(any())).thenReturn(tsStatus);
    sessionConnection.createTimeseries(new TSCreateTimeseriesReq());
  }

  @Test
  public void testCreateAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.createAlignedTimeseries(new TSCreateAlignedTimeseriesReq());
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.createMultiTimeseries(new TSCreateMultiTimeseriesReq());
  }

  @Test
  public void testCheckTimeseriesExists()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.checkTimeseriesExists("root.test1.dev1.s1", 500l);
  }

  @Test
  public void testExecuteQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.executeQueryStatement("show version", 500l);
  }

  @Test
  public void testExecuteNonQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.executeNonQueryStatement(
        "create timeseries root.stock.Legacy.0700HK.L1_BuyNo WITH datatype=BOOLEAN, encoding=PLAIN;");
  }

  @Test
  public void testExecuteRawDataQuery()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.executeRawDataQuery(
        Arrays.asList("root.test1"),
        System.currentTimeMillis() - 1000 * 60 * 24,
        System.currentTimeMillis(),
        500l);
  }

  @Test
  public void testExecuteLastDataQueryForOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.executeLastDataQueryForOneDevice(
        "db1", "dev1", Arrays.asList("s1", "s2"), true, 500l);
  }

  @Test
  public void testExecuteLastDataQuery()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.executeLastDataQuery(Arrays.asList("s1", "s2"), 5000, 500l);
  }

  @Test
  public void testExecuteAggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.executeAggregationQuery(
        Arrays.asList("s1", "s2"),
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE));
    sessionConnection.executeAggregationQuery(
        Arrays.asList("s1", "s2"),
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE),
        System.currentTimeMillis() - 1000 * 60 * 24,
        System.currentTimeMillis());
    sessionConnection.executeAggregationQuery(
        Arrays.asList("s1", "s2"),
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE),
        System.currentTimeMillis() - 1000 * 60 * 24,
        System.currentTimeMillis(),
        500l);
    sessionConnection.executeAggregationQuery(
        Arrays.asList("s1", "s2"),
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE),
        System.currentTimeMillis() - 1000 * 60 * 24,
        System.currentTimeMillis(),
        500l,
        500l);
  }

  @Test
  public void testExecuteAggregationQueryWithTime()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.executeAggregationQuery(
        Arrays.asList("s1", "s2"),
        Arrays.asList(TAggregationType.LAST_VALUE, TAggregationType.MAX_VALUE),
        System.currentTimeMillis() - 1000 * 60 * 24,
        System.currentTimeMillis());
  }

  @Test
  public void testTimeZone() {
    String zoneId = ZoneId.systemDefault().getId();
    sessionConnection.setTimeZoneOfSession(ZoneId.systemDefault().getId());
    Assert.assertEquals(zoneId, sessionConnection.getTimeZone());
  }

  @Test
  public void testInsertRecord()
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    sessionConnection.insertRecord(new TSInsertRecordReq());
    sessionConnection.insertRecord(new TSInsertStringRecordReq());
    sessionConnection.testInsertRecord(new TSInsertStringRecordReq());
    sessionConnection.testInsertRecord(new TSInsertRecordReq());
    sessionConnection.insertRecords(new TSInsertRecordsReq());
    sessionConnection.insertRecords(new TSInsertStringRecordsReq());
    sessionConnection.testInsertRecords(new TSInsertRecordsReq());
    sessionConnection.testInsertRecords(new TSInsertStringRecordsReq());
    sessionConnection.insertRecordsOfOneDevice(new TSInsertRecordsOfOneDeviceReq());
    sessionConnection.insertStringRecordsOfOneDevice(new TSInsertStringRecordsOfOneDeviceReq());
    sessionConnection.insertTablet(new TSInsertTabletReq());
    sessionConnection.testInsertTablet(new TSInsertTabletReq());
    sessionConnection.insertTablets(new TSInsertTabletsReq());
    sessionConnection.testInsertTablets(new TSInsertTabletsReq());
    sessionConnection.deleteTimeseries(Arrays.asList("root.sg1.d1.s1"));
  }

  @Test
  public void testDeleteData() throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.deleteData(new TSDeleteDataReq());
  }

  @Test
  public void testCreateSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.createSchemaTemplate(new TSCreateSchemaTemplateReq());
  }

  @Test
  public void testAppendSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.appendSchemaTemplate(new TSAppendSchemaTemplateReq());
  }

  @Test
  public void testPruneSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.pruneSchemaTemplate(new TSPruneSchemaTemplateReq());
  }

  @Test
  public void testQuerySchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.querySchemaTemplate(new TSQueryTemplateReq());
  }

  @Test
  public void testSetSchemaTemplate() throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.setSchemaTemplate(new TSSetSchemaTemplateReq());
  }

  @Test
  public void testUnsetSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.unsetSchemaTemplate(new TSUnsetSchemaTemplateReq());
  }

  @Test
  public void testDropSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.dropSchemaTemplate(new TSDropSchemaTemplateReq());
  }

  @Test
  public void testCreateTimeseriesUsingSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.createTimeseriesUsingSchemaTemplate(
        new TCreateTimeseriesUsingSchemaTemplateReq());
  }

  @Test
  public void testGetBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionConnection.getBackupConfiguration();
  }

  @Test
  public void testFetchAllConnections() throws IoTDBConnectionException {
    sessionConnection.fetchAllConnections();
  }

  @Test
  public void testToString() {
    sessionConnection.setEnableRedirect(true);
    Assert.assertEquals(true, sessionConnection.isEnableRedirect());
    sessionConnection.setEndPoint(new TEndPoint("localhost", 1234));
    Assert.assertEquals("localhost", sessionConnection.getEndPoint().getIp());
    sessionConnection.toString();
  }
}
