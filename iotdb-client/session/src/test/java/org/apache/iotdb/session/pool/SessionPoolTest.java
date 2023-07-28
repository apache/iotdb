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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

  @Before
  public void setUp() throws IoTDBConnectionException, StatementExecutionException, TException {
    // Initialize the session pool before each test
    MockitoAnnotations.initMocks(this);
    execResp.queryResult = FakedFirstFetchTsBlockResult();

    when(client.executeStatementV2(any(TSExecuteStatementReq.class))).thenReturn(execResp);
    when(execResp.getQueryId()).thenReturn(queryId);
    when(execResp.getStatus()).thenReturn(successStatus);

    when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
    when(fetchMetadataResp.getStatus()).thenReturn(successStatus);

    when(client.fetchResultsV2(any(TSFetchResultsReq.class))).thenReturn(fetchResultsResp);
    when(fetchResultsResp.getStatus()).thenReturn(successStatus);

    TSStatus closeResp = successStatus;
    when(client.closeOperation(any(TSCloseOperationReq.class))).thenReturn(closeResp);
  }

  @After
  public void tearDown() {
    // Close the session pool after each test
    sessionPool.close();
  }

  @Test
  public void testInsertRecords() throws IoTDBConnectionException, StatementExecutionException {
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

    verify(sessionPool, times(1))
        .insertRecords(
            ArgumentMatchers.eq(deviceIds),
            ArgumentMatchers.eq(timeList),
            ArgumentMatchers.eq(measurementsList),
            ArgumentMatchers.eq(typesList),
            ArgumentMatchers.eq(valuesList));
  }

  @Test(expected = StatementExecutionException.class)
  public void testInsertAlignedTabletsWithStatementExecutionException()
      throws IoTDBConnectionException, StatementExecutionException {
    Map<String, Tablet> tablets = new HashMap<>();
    boolean sorted = true;
    doThrow(new StatementExecutionException(""))
        .when(sessionPool)
        .insertAlignedTablets(tablets, sorted);
    sessionPool.insertAlignedTablets(tablets, sorted);
  }

  @Test
  public void testExecuteQueryStatement()
      throws IoTDBConnectionException, StatementExecutionException {
    // 构造测试数据
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

    when(execResp.isSetColumns()).thenReturn(true);
    when(execResp.getColumns()).thenReturn(columns);
    when(execResp.isSetDataTypeList()).thenReturn(true);
    when(execResp.getDataTypeList()).thenReturn(dataTypeList);
    when(execResp.isSetOperationType()).thenReturn(true);
    when(execResp.getOperationType()).thenReturn("QUERY");
    when(execResp.isSetQueryId()).thenReturn(true);
    when(execResp.getQueryId()).thenReturn(queryId);

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
            10);
    when(sessionPool.executeQueryStatement(any(String.class)))
        .thenReturn(new SessionDataSetWrapper(sessionDataSet, session, sessionPool));

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
