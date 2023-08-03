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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.any;
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
  private IoTDBJDBCResultSet result;

  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  private ZoneId zoneID = ZoneId.systemDefault();

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);

    statement = new IoTDBStatement(connection, client, sessionId, zoneID);

    execResp.queryResult = FakedFirstFetchTsBlockResult();

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

    result =
        new IoTDBJDBCResultSet(
            statement,
            new ArrayList<>(),
            new ArrayList<>(),
            new HashMap<>(),
            false,
            client,
            "",
            1L,
            sessionId,
            null,
            null,
            (long) 60 * 1000,
            false);
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
        2L, 2.22F, 40000L, null, 2.22F,
      },
      {
        3L, 3.33F, null, null, 3.33F,
      },
      {
        4L, 4.44F, null, null, 4.44F,
      },
      {
        50L, null, 50000L, null, null,
      },
      {
        100L, null, 199L, null, null,
      },
      {
        101L, null, 199L, null, null,
      },
      {
        103L, null, 199L, null, null,
      },
      {
        105L, 11.11F, 199L, 33333, 11.11F,
      },
      {
        1000L, 1000.11F, 55555L, 22222, 1000.11F,
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

  @Test(expected = SQLException.class)
  public void testIsWrapperFor() throws SQLException {
    result.isWrapperFor(String.class);
  }

  @Test(expected = SQLException.class)
  public void testUnwrap() throws SQLException {
    result.unwrap(String.class);
  }

  @Test(expected = SQLException.class)
  public void testAbsolute() throws SQLException {
    result.absolute(1);
  }

  @Test(expected = SQLException.class)
  public void testBeforeFirst() throws SQLException {
    result.beforeFirst();
  }

  @Test(expected = SQLException.class)
  public void testAfterLast() throws SQLException {
    result.afterLast();
  }

  @Test(expected = SQLException.class)
  public void testCancelRowUpdates() throws SQLException {
    result.cancelRowUpdates();
  }

  @Test(expected = SQLException.class)
  public void testClearWarnings() throws SQLException {
    result.clearWarnings();
  }

  @Test(expected = SQLException.class)
  public void testDeleteRow() throws SQLException {
    result.deleteRow();
  }

  @Test(expected = SQLException.class)
  public void testFirst() throws SQLException {
    result.first();
  }

  @Test(expected = SQLException.class)
  public void testGetArrayInt() throws SQLException {
    result.getArray(1);
  }

  @Test(expected = SQLException.class)
  public void testGetArrayString() throws SQLException {
    result.getArray("");
  }

  @Test(expected = SQLException.class)
  public void testGetAsciiStreamInt() throws SQLException {
    result.getAsciiStream(1);
  }

  @Test(expected = SQLException.class)
  public void testGetAsciiStreamString() throws SQLException {
    result.getAsciiStream("");
  }

  @Test(expected = SQLException.class)
  public void previous() throws SQLException {
    result.previous();
  }

  @Test(expected = SQLException.class)
  public void refreshRow() throws SQLException {
    result.refreshRow();
  }

  @Test(expected = SQLException.class)
  public void relative() throws SQLException {
    result.relative(1);
  }

  @Test(expected = SQLException.class)
  public void rowDeleted() throws SQLException {
    result.rowDeleted();
  }

  @Test(expected = SQLException.class)
  public void rowInserted() throws SQLException {
    result.rowInserted();
  }

  @Test(expected = SQLException.class)
  public void rowUpdated() throws SQLException {
    result.rowUpdated();
  }

  @Test(expected = SQLException.class)
  public void updateArrayI() throws SQLException {
    result.updateArray(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateArrayS() throws SQLException {
    result.updateArray("", null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamI() throws SQLException {
    result.updateAsciiStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamS() throws SQLException {
    result.updateAsciiStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamII() throws SQLException {
    result.updateAsciiStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamSI() throws SQLException {
    result.updateAsciiStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamIL() throws SQLException {
    result.updateAsciiStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStreamSL() throws SQLException {
    result.updateAsciiStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBigDecimalI() throws SQLException {
    result.updateBigDecimal(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateBigDecimalS() throws SQLException {
    result.updateBigDecimal("", null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamI() throws SQLException {
    result.updateBinaryStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamS() throws SQLException {
    result.updateBinaryStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamII() throws SQLException {
    result.updateBinaryStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamSI() throws SQLException {
    result.updateBinaryStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamIL() throws SQLException {
    result.updateBinaryStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStreamSL() throws SQLException {
    result.updateBinaryStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBlobIB() throws SQLException {
    result.updateBlob(1, new SerialBlob(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlobSB() throws SQLException {
    result.updateBlob("", new SerialBlob(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlobI() throws SQLException {
    result.updateBlob(1, new ByteArrayInputStream(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlobS() throws SQLException {
    result.updateBlob("", new ByteArrayInputStream(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlobIL() throws SQLException {
    result.updateBlob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBlobSL() throws SQLException {
    result.updateBlob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBooleanI() throws SQLException {
    result.updateBoolean(1, true);
  }

  @Test(expected = SQLException.class)
  public void updateBooleanS() throws SQLException {
    result.updateBoolean("", true);
  }

  @Test(expected = SQLException.class)
  public void updateByteI() throws SQLException {
    byte b = 1;
    result.updateByte(1, b);
  }

  @Test(expected = SQLException.class)
  public void updateByteS() throws SQLException {
    byte b = 1;
    result.updateByte("", b);
  }

  @Test(expected = SQLException.class)
  public void updateBytesI() throws SQLException {
    result.updateBytes(1, new byte[] {});
  }

  @Test(expected = SQLException.class)
  public void updateBytesS() throws SQLException {
    result.updateBytes("", new byte[] {});
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamI() throws SQLException {
    result.updateCharacterStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamS() throws SQLException {
    result.updateCharacterStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamII() throws SQLException {
    result.updateCharacterStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamSI() throws SQLException {
    result.updateCharacterStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamIL() throws SQLException {
    result.updateCharacterStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStreamSL() throws SQLException {
    result.updateCharacterStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateClobIC() throws SQLException {
    result.updateClob(1, new SerialClob(new char[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateClobSC() throws SQLException {
    result.updateClob("", new SerialClob(new char[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateClobI() throws SQLException {
    result.updateClob(1, new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateClobS() throws SQLException {
    result.updateClob("", new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateClobIL() throws SQLException {
    result.updateClob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateClobSL() throws SQLException {
    result.updateClob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateDateI() throws SQLException {
    result.updateDate(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateDateS() throws SQLException {
    result.updateDate("", null);
  }

  @Test(expected = SQLException.class)
  public void updateDoubleI() throws SQLException {
    result.updateDouble(1, 1d);
  }

  @Test(expected = SQLException.class)
  public void updateDoubleS() throws SQLException {
    result.updateDouble("", 1d);
  }

  @Test(expected = SQLException.class)
  public void updateFloatI() throws SQLException {
    result.updateFloat(1, 1f);
  }

  @Test(expected = SQLException.class)
  public void updateFloatS() throws SQLException {
    result.updateFloat("", 1f);
  }

  @Test(expected = SQLException.class)
  public void updateIntI() throws SQLException {
    result.updateInt(1, 1);
  }

  @Test(expected = SQLException.class)
  public void updateIntS() throws SQLException {
    result.updateInt("", 1);
  }

  @Test(expected = SQLException.class)
  public void updateLongI() throws SQLException {
    result.updateLong(1, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateLongS() throws SQLException {
    result.updateLong("", 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStreamI() throws SQLException {
    result.updateNCharacterStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStreamS() throws SQLException {
    result.updateNCharacterStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStreamIL() throws SQLException {
    result.updateNCharacterStream(1, null, 1l);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStreamSL() throws SQLException {
    result.updateNCharacterStream("", null, 1l);
  }

  @Test(expected = SQLException.class)
  public void updateNClobI() throws SQLException {
    result.updateNClob(1, new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateNClobS() throws SQLException {
    result.updateNClob("", new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateNClobIL() throws SQLException {
    result.updateNClob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNClobSL() throws SQLException {
    result.updateNClob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNStringI() throws SQLException {
    result.updateNString(1, "");
  }

  @Test(expected = SQLException.class)
  public void updateNStringS() throws SQLException {
    result.updateNString("", "");
  }

  @Test(expected = SQLException.class)
  public void updateNullI() throws SQLException {
    result.updateNull(1);
  }

  @Test(expected = SQLException.class)
  public void updateNullS() throws SQLException {
    result.updateNull("");
  }

  @Test(expected = SQLException.class)
  public void updateObjectI() throws SQLException {
    result.updateObject(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateObjectS() throws SQLException {
    result.updateObject("", null);
  }

  @Test(expected = SQLException.class)
  public void updateObjectII() throws SQLException {
    result.updateObject(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateObjectSI() throws SQLException {
    result.updateObject("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateRefI() throws SQLException {
    result.updateRef(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateRefS() throws SQLException {
    result.updateRef("", null);
  }

  @Test(expected = SQLException.class)
  public void updateRow() throws SQLException {
    result.updateRow();
  }

  @Test(expected = SQLException.class)
  public void updateRowIdI() throws SQLException {
    result.updateRowId(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateRowIdS() throws SQLException {
    result.updateRowId("", null);
  }

  @Test(expected = SQLException.class)
  public void updateSQLXMLI() throws SQLException {
    result.updateSQLXML(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateSQLXMLS() throws SQLException {
    result.updateSQLXML("", null);
  }

  @Test(expected = SQLException.class)
  public void updateShortI() throws SQLException {
    short s = 1;
    result.updateShort(1, s);
  }

  @Test(expected = SQLException.class)
  public void updateShortS() throws SQLException {
    short s = 1;
    result.updateShort("", s);
  }

  @Test(expected = SQLException.class)
  public void updateStringI() throws SQLException {
    result.updateString(1, "");
  }

  @Test(expected = SQLException.class)
  public void updateStringS() throws SQLException {
    result.updateString("", "");
  }

  @Test(expected = SQLException.class)
  public void updateTimeI() throws SQLException {
    result.updateTime(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateTimeS() throws SQLException {
    result.updateTime("", null);
  }

  @Test(expected = SQLException.class)
  public void updateTimestampI() throws SQLException {
    result.updateTimestamp(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateTimestampS() throws SQLException {
    result.updateTimestamp("", null);
  }

  @Test(expected = SQLException.class)
  public void getBinaryStreamI() throws SQLException {
    result.getBinaryStream(1);
  }

  @Test(expected = SQLException.class)
  public void getBinaryStreamS() throws SQLException {
    result.getBinaryStream("");
  }

  @Test(expected = SQLException.class)
  public void getBlobI() throws SQLException {
    result.getBlob(1);
  }

  @Test(expected = SQLException.class)
  public void getBlobS() throws SQLException {
    result.getBlob("");
  }

  @Test(expected = SQLException.class)
  public void getByteI() throws SQLException {
    result.getByte(1);
  }

  @Test(expected = SQLException.class)
  public void getByteS() throws SQLException {
    result.getByte("");
  }

  @Test(expected = SQLException.class)
  public void getBytesI() throws SQLException {
    result.getBytes(1);
  }

  @Test(expected = SQLException.class)
  public void getBytesS() throws SQLException {
    result.getBytes("");
  }

  @Test(expected = SQLException.class)
  public void getCharacterStreamI() throws SQLException {
    result.getCharacterStream(1);
  }

  @Test(expected = SQLException.class)
  public void getCharacterStreamS() throws SQLException {
    result.getCharacterStream("");
  }

  @Test(expected = SQLException.class)
  public void getClobI() throws SQLException {
    result.getClob(1);
  }

  @Test(expected = SQLException.class)
  public void getClobS() throws SQLException {
    result.getClob("");
  }

  @Test(expected = SQLException.class)
  public void getCursorName() throws SQLException {
    result.getCursorName();
  }

  @Test(expected = SQLException.class)
  public void getDateI() throws SQLException {
    result.getDate(1, null);
  }

  @Test(expected = SQLException.class)
  public void getDateS() throws SQLException {
    result.getDate("", null);
  }

  @Test(expected = SQLException.class)
  public void setFetchDirection() throws SQLException {
    result.setFetchDirection(1);
  }

  @Test(expected = SQLException.class)
  public void getFetchSize() throws SQLException {
    result.getFetchSize();
  }

  @Test(expected = SQLException.class)
  public void setFetchSize() throws SQLException {
    result.setFetchSize(1);
  }

  @Test(expected = SQLException.class)
  public void getNCharacterStreamI() throws SQLException {
    result.getNCharacterStream(1);
  }

  @Test(expected = SQLException.class)
  public void getNCharacterStreamS() throws SQLException {
    result.getNCharacterStream("");
  }

  @Test(expected = SQLException.class)
  public void getNClobI() throws SQLException {
    result.getNClob(1);
  }

  @Test(expected = SQLException.class)
  public void getNClobS() throws SQLException {
    result.getNClob("");
  }

  @Test(expected = SQLException.class)
  public void getNStringI() throws SQLException {
    result.getNString(1);
  }

  @Test(expected = SQLException.class)
  public void getNStringS() throws SQLException {
    result.getNString("");
  }

  @Test(expected = SQLException.class)
  public void getObjectIM() throws SQLException {
    result.getObject(1, new HashMap<>());
  }

  @Test(expected = SQLException.class)
  public void getObjectSM() throws SQLException {
    result.getObject("", new HashMap<>());
  }

  @Test(expected = SQLException.class)
  public void getObjectIC() throws SQLException {
    result.getObject(1, String.class);
  }

  @Test(expected = SQLException.class)
  public void getObjectSC() throws SQLException {
    result.getObject("", String.class);
  }

  @Test(expected = SQLException.class)
  public void getRefI() throws SQLException {
    result.getRef(1);
  }

  @Test(expected = SQLException.class)
  public void getRefS() throws SQLException {
    result.getRef("");
  }

  @Test(expected = SQLException.class)
  public void getRow() throws SQLException {
    result.getRow();
  }

  @Test(expected = SQLException.class)
  public void getRowIdI() throws SQLException {
    result.getRowId(1);
  }

  @Test(expected = SQLException.class)
  public void getRowIdS() throws SQLException {
    result.getRowId("");
  }

  @Test(expected = SQLException.class)
  public void getSQLXMLI() throws SQLException {
    result.getSQLXML(1);
  }

  @Test(expected = SQLException.class)
  public void getSQLXMLS() throws SQLException {
    result.getSQLXML("");
  }

  @Test(expected = SQLException.class)
  public void getShortI() throws SQLException {
    result.getShort(1);
  }

  @Test(expected = SQLException.class)
  public void getShortS() throws SQLException {
    result.getShort("");
  }

  @Test(expected = SQLException.class)
  public void getTimeI() throws SQLException {
    result.getTime(1, null);
  }

  @Test(expected = SQLException.class)
  public void getTimeS() throws SQLException {
    result.getTime("", null);
  }

  @Test(expected = SQLException.class)
  public void getTimestampI() throws SQLException {
    result.getTimestamp(1, null);
  }

  @Test(expected = SQLException.class)
  public void getTimestampS() throws SQLException {
    result.getTimestamp("", null);
  }

  @Test(expected = SQLException.class)
  public void getURLI() throws SQLException {
    result.getURL(1);
  }

  @Test(expected = SQLException.class)
  public void getURLS() throws SQLException {
    result.getURL("");
  }

  @Test(expected = SQLException.class)
  public void getUnicodeStreamI() throws SQLException {
    result.getUnicodeStream(1);
  }

  @Test(expected = SQLException.class)
  public void getUnicodeStreamS() throws SQLException {
    result.getUnicodeStream("");
  }

  @Test(expected = SQLException.class)
  public void insertRow() throws SQLException {
    result.insertRow();
  }

  @Test(expected = SQLException.class)
  public void isAfterLast() throws SQLException {
    result.isAfterLast();
  }

  @Test(expected = SQLException.class)
  public void isBeforeFirst() throws SQLException {
    result.isBeforeFirst();
  }

  @Test(expected = SQLException.class)
  public void isFirst() throws SQLException {
    result.isFirst();
  }

  @Test(expected = SQLException.class)
  public void isLast() throws SQLException {
    result.isLast();
  }

  @Test(expected = SQLException.class)
  public void last() throws SQLException {
    result.last();
  }

  @Test(expected = SQLException.class)
  public void moveToCurrentRow() throws SQLException {
    result.moveToCurrentRow();
  }

  @Test(expected = SQLException.class)
  public void moveToInsertRow() throws SQLException {
    result.moveToInsertRow();
  }
}
