/**
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * This class is designed to test the function of databaseMetaData which is used to fetch metadata from IoTDB. (1) get
 * all columns' name under a given path, e.g., databaseMetaData.getColumns("col", "root", null, null); (2) get all
 * devices under a given column e.g., databaseMetaData.getColumns("device", "vehicle", null, null); (3) show timeseries
 * path e.g., databaseMetaData.getColumns("ts", "root.vehicle.d0.s0", null, null); (4) show storage group
 * databaseMetaData.getColumns("sg", null, null, null); (5) show metadata in json
 * ((TsfileDatabaseMetadata)databaseMetaData).getMetadataInJson()
 * <p>
 * The tests utilize the mockito framework to mock responses from an IoTDB server. The status of the IoTDB server mocked
 * here is determined by the following four sql commands: SET STORAGE GROUP TO root.vehicle; CREATE TIMESERIES
 * root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE; CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64,
 * ENCODING=RLE; CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;
 */

public class IoTDBDatabaseMetadataTest {

  @Mock
  private IoTDBConnection connection;

  @Mock
  private TSIService.Iface client;

  @Mock
  private TSFetchMetadataResp fetchMetadataResp;

  private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);

  private DatabaseMetaData databaseMetaData;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.getMetaData()).thenReturn(new IoTDBDatabaseMetadata(connection, client));

    when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
    when(fetchMetadataResp.getStatus()).thenReturn(Status_SUCCESS);

    databaseMetaData = connection.getMetaData();
  }

  /**
   * get all columns' name under a given path
   */
  @SuppressWarnings("resource")
  @Test
  public void AllColumns() throws Exception {
    List<String> columnList = new ArrayList<>();
    columnList.add("root.vehicle.d0.s0");
    columnList.add("root.vehicle.d0.s1");
    columnList.add("root.vehicle.d0.s2");

    when(fetchMetadataResp.getColumnsList()).thenReturn(columnList);

    String standard =
        "Column,\n" + "root.vehicle.d0.s0,\n" + "root.vehicle.d0.s1,\n" + "root.vehicle.d0.s2,\n";
    try {
      ResultSet resultSet = databaseMetaData.getColumns(Constant.CatalogColumn, "root", null, null);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int colCount = resultSetMetaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 1; i < colCount + 1; i++) {
        resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      resultStr.append("\n");
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

  /**
   * get all devices under a given column
   */
  @SuppressWarnings("resource")
  @Test
  public void device() throws Exception {
    List<String> columnList = new ArrayList<>();
    columnList.add("root.vehicle.d0");

    when(fetchMetadataResp.getColumnsList()).thenReturn(columnList);

    String standard = "Column,\n" + "root.vehicle.d0,\n";
    try {
      ResultSet resultSet = databaseMetaData
          .getColumns(Constant.CatalogDevice, "vehicle", null, null);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int colCount = resultSetMetaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 1; i < colCount + 1; i++) {
        resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      resultStr.append("\n");
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

  /**
   * show timeseries <path> usage 1
   */
  @SuppressWarnings({"resource", "serial"})
  @Test
  public void ShowTimeseriesPath1() throws Exception {
    List<List<String>> tslist = new ArrayList<>();
    tslist.add(new ArrayList<String>(4) {
      {
        add("root.vehicle.d0.s0");
        add("root.vehicle");
        add("INT32");
        add("RLE");
      }
    });
    tslist.add(new ArrayList<String>(4) {
      {
        add("root.vehicle.d0.s1");
        add("root.vehicle");
        add("INT64");
        add("RLE");
      }
    });
    tslist.add(new ArrayList<String>(4) {
      {
        add("root.vehicle.d0.s2");
        add("root.vehicle");
        add("FLOAT");
        add("RLE");
      }
    });

    when(fetchMetadataResp.getShowTimeseriesList()).thenReturn(tslist);

    String standard = "Timeseries,Storage Group,DataType,Encoding,\n"
        + "root.vehicle.d0.s0,root.vehicle,INT32,RLE,\n"
        + "root.vehicle.d0.s1,root.vehicle,INT64,RLE,\n"
        + "root.vehicle.d0.s2,root.vehicle,FLOAT,RLE,\n";
    try {
      ResultSet resultSet = databaseMetaData
          .getColumns(Constant.CatalogTimeseries, "root", null, null);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int colCount = resultSetMetaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 1; i < colCount + 1; i++) {
        resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      resultStr.append("\n");
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

  /**
   * show timeseries <path> usage 2: Get information about a specific column, e.g., DataType
   */
  @SuppressWarnings({"resource", "serial"})
  @Test
  public void ShowTimeseriesPath2() throws Exception {
    List<List<String>> tslist = new ArrayList<>();
    tslist.add(new ArrayList<String>(4) {
      {
        add("root.vehicle.d0.s0");
        add("root.vehicle");
        add("INT32");
        add("RLE");
      }
    });

    when(fetchMetadataResp.getShowTimeseriesList()).thenReturn(tslist);

    String standard = "DataType,\n" + "INT32,\n";
    try {
      ResultSet resultSet = databaseMetaData
          .getColumns(Constant.CatalogTimeseries, "root.vehicle.d0.s0", null,
              null);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      StringBuilder resultStr = new StringBuilder();
      resultStr.append(resultSetMetaData.getColumnName(3)).append(",\n");
      while (resultSet.next()) {
        resultStr.append(resultSet.getString(IoTDBMetadataResultSet.GET_STRING_TIMESERIES_DATATYPE))
            .append(",");
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

  /**
   * show storage group
   */
  @SuppressWarnings("resource")
  @Test
  public void ShowStorageGroup() throws Exception {
    Set<String> sgSet = new HashSet<>();
    sgSet.add("root.vehicle");
    when(fetchMetadataResp.getShowStorageGroups()).thenReturn(sgSet);

    String standard = "Storage Group,\n" + "root.vehicle,\n";
    try {
      ResultSet resultSet = databaseMetaData
          .getColumns(Constant.CatalogStorageGroup, null, null, null);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int colCount = resultSetMetaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 1; i < colCount + 1; i++) {
        resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      resultStr.append("\n");
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

  /**
   * show metadata in json
   */
  @SuppressWarnings("resource")
  @Test
  public void ShowTimeseriesInJson() throws Exception {
    String metadataInJson =
        "===  Timeseries Tree  ===\n" + "\n" + "root:{\n" + "    vehicle:{\n" + "        d0:{\n"
            + "            s0:{\n" + "                 DataType: INT32,\n"
            + "                 Encoding: RLE,\n"
            + "                 args: {},\n" + "                 StorageGroup: root.vehicle \n"
            + "            },\n"
            + "            s1:{\n" + "                 DataType: INT64,\n"
            + "                 Encoding: RLE,\n"
            + "                 args: {},\n" + "                 StorageGroup: root.vehicle \n"
            + "            },\n"
            + "            s2:{\n" + "                 DataType: FLOAT,\n"
            + "                 Encoding: RLE,\n"
            + "                 args: {},\n" + "                 StorageGroup: root.vehicle \n"
            + "            }\n"
            + "        }\n" + "    }\n" + "}";

    when(fetchMetadataResp.getMetadataInJson()).thenReturn(metadataInJson);

    String res = ((IoTDBDatabaseMetadata) databaseMetaData).getMetadataInJson();
    assertEquals(metadataInJson, res);
  }
}
