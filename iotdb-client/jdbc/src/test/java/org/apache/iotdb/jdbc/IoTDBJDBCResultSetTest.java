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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
   This class is designed to test the function of TsfileQueryResultSet.
   This class also sheds light on the complete execution process of a query sql from the jdbc perspective.

   The test utilizes the mockito framework to mock responses from an IoTDB server.
   The status of the IoTDB server mocked here is determined by the following sql commands:

   "CREATE DATABASE root.vehicle",
   "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
   "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
   "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
   "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
   "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
   "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
   "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
   "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
   "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
   "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
   "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
   "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
   "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
   "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
   "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
   "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
   "UPDATE root.vehicle.d0 SET s0 = 33333 WHERE time < 106 and time > 103",
   "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
   "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
   "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
   "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
   "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
   "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
   "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
   "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
   "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
   "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
   "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
   "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
   "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
   "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
   "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
   "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
   "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
   "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
   "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
*/

public class IoTDBJDBCResultSetTest {

  @Mock TSExecuteStatementResp execResp;
  private long queryId;
  private long sessionId;
  @Mock private IoTDBConnection connection;
  @Mock private IClientRPCService.Iface client;
  @Mock private Statement statement;
  @Mock private TSFetchMetadataResp fetchMetadataResp;
  @Mock private TSFetchResultsResp fetchResultsResp;

  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  private ZoneId zoneID = ZoneId.systemDefault();

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);

    statement = new IoTDBStatement(connection, client, sessionId, zoneID);

    execResp.queryResult = FakedFirstFetchTsBlockResult();

    when(connection.getTimeFactor()).thenReturn(1000);
    when(connection.isClosed()).thenReturn(false);
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

  @SuppressWarnings("resource")
  @Test
  public void testQuery() throws Exception {

    String testSql =
        "select *,s1,s0,s2 from root.vehicle.d0 where s1 > 190 or s2 < 10.0 "
            + "limit 20 slimit 4 soffset 2";

    /*
     * step 1: execute statement
     */
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
    when(execResp.isSetTableModel()).thenReturn(false);
    when(execResp.isIgnoreTimeStamp()).thenReturn(false);
    List<Integer> columnIndex2TsBlockColumnIndexList = new ArrayList<>(columns.size());
    columnIndex2TsBlockColumnIndexList.add(0);
    columnIndex2TsBlockColumnIndexList.add(1);
    columnIndex2TsBlockColumnIndexList.add(2);
    columnIndex2TsBlockColumnIndexList.add(0);

    when(execResp.getColumnIndex2TsBlockColumnIndexList())
        .thenReturn(columnIndex2TsBlockColumnIndexList);
    doReturn("FLOAT")
        .doReturn("INT64")
        .doReturn("INT32")
        .doReturn("FLOAT")
        .when(fetchMetadataResp)
        .getDataType();

    boolean hasResultSet = statement.execute(testSql);
    Assert.assertTrue(hasResultSet);

    verify(fetchMetadataResp, times(0)).getDataType();

    /*
     * step 2: fetch result
     */
    fetchResultsResp.hasResultSet = true; // at the first time to fetch

    try (ResultSet resultSet = statement.getResultSet()) {
      // check columnInfoMap
      Assert.assertEquals(1, resultSet.findColumn("Time"));
      Assert.assertEquals(2, resultSet.findColumn("root.vehicle.d0.s2"));
      Assert.assertEquals(3, resultSet.findColumn("root.vehicle.d0.s1"));
      Assert.assertEquals(4, resultSet.findColumn("root.vehicle.d0.s0"));

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      // check columnInfoList
      Assert.assertEquals("Time", resultSetMetaData.getColumnName(1));
      Assert.assertEquals("root.vehicle.d0.s2", resultSetMetaData.getColumnName(2));
      Assert.assertEquals("root.vehicle.d0.s1", resultSetMetaData.getColumnName(3));
      Assert.assertEquals("root.vehicle.d0.s0", resultSetMetaData.getColumnName(4));
      Assert.assertEquals("root.vehicle.d0.s2", resultSetMetaData.getColumnName(5));
      // check columnTypeList
      Assert.assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(1));
      Assert.assertEquals(Types.FLOAT, resultSetMetaData.getColumnType(2));
      Assert.assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(3));
      Assert.assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(4));
      Assert.assertEquals(Types.FLOAT, resultSetMetaData.getColumnType(5));
      // check fetched result
      int colCount = resultSetMetaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      List<Object> resultObjectList = new ArrayList<>();
      for (int i = 1; i < colCount + 1; i++) { // meta title
        resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      resultStr.append("\n");
      while (resultSet.next()) { // data
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
          resultObjectList.add(resultSet.getObject(i));
        }
        resultStr.append("\n");
        fetchResultsResp.hasResultSet = false; // at the second time to fetch
      }
      String standard =
          "Time,root.vehicle.d0.s2,root.vehicle.d0.s1,root.vehicle.d0.s0,root.vehicle.d0.s2,\n"
              + "2,2.22,40000,null,2.22,\n"
              + "3,3.33,null,null,3.33,\n"
              + "4,4.44,null,null,4.44,\n"
              + "50,null,50000,null,null,\n"
              + "100,null,199,null,null,\n"
              + "101,null,199,null,null,\n"
              + "103,null,199,null,null,\n"
              + "105,11.11,199,33333,11.11,\n"
              + "1000,1000.11,55555,22222,1000.11,\n"; // Note the LIMIT&OFFSET clause takes effect
      Assert.assertEquals(standard, resultStr.toString());
      List<Object> standardObject = new ArrayList<>();
      constructObjectList(standardObject);
      Assert.assertEquals(standardObject.size(), resultObjectList.size());
      for (int i = 0; i < standardObject.size(); i++) {
        Assert.assertEquals(standardObject.get(i), resultObjectList.get(i));
      }
    }

    // The client get TSQueryDataSet at the first request
    verify(fetchResultsResp, times(0)).getStatus();
  }

  private void constructObjectList(List<Object> standardObject) {
    Object[][] input = {
      {
        new Timestamp(2), 2.22F, 40000L, null, 2.22F,
      },
      {
        new Timestamp(3), 3.33F, null, null, 3.33F,
      },
      {
        new Timestamp(4), 4.44F, null, null, 4.44F,
      },
      {
        new Timestamp(50), null, 50000L, null, null,
      },
      {
        new Timestamp(100), null, 199L, null, null,
      },
      {
        new Timestamp(101), null, 199L, null, null,
      },
      {
        new Timestamp(103), null, 199L, null, null,
      },
      {
        new Timestamp(105), 11.11F, 199L, 33333, 11.11F,
      },
      {
        new Timestamp(1000), 1000.11F, 55555L, 22222, 1000.11F,
      }
    };
    for (Object[] row : input) {
      standardObject.addAll(Arrays.asList(row));
    }
  }

  // fake the first-time fetched result of 'testSql' from an IoTDB server
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
