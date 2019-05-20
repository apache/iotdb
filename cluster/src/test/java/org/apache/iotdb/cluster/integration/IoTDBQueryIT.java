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
package org.apache.iotdb.cluster.integration;

import static org.apache.iotdb.cluster.utils.Utils.insertBatchData;
import static org.apache.iotdb.cluster.utils.Utils.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBResultMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBQueryIT {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  private static final String URL = "127.0.0.1:6667/";

  private String[] createSQLs = {
      "SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.test",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE"

  };
  private String[] insertSQLs = {
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
      "insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(17,false)",
      "insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(22,1001,'1002')",
      "insert into root.vehicle.d0(timestamp,s1) values(29,'1003')",
      "insert into root.vehicle.d1(timestamp,s2) values(21,1004.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(25,1005.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(27,true)",
      "insert into root.test.d0(timestamp,s0) values(10,106)",
      "insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.test.d0(timestamp,s1) values(16,'109')",
      "insert into root.test.d1.g0(timestamp,s0) values(1,110)",
      "insert into root.test.d0(timestamp,s0) values(30,1006)",
      "insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
      "insert into root.test.d0(timestamp,s1) values(36,'1090')",
      "insert into root.test.d1.g0(timestamp,s0) values(10,1100)",
      "insert into root.vehicle.d0(timestamp,s0) values(6,120)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(38,121,'122')",
      "insert into root.vehicle.d0(timestamp,s1) values(9,'123')",
      "insert into root.vehicle.d0(timestamp,s0) values(16,128)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(18,189,'198')",
      "insert into root.vehicle.d0(timestamp,s1) values(99,'1234')",
      "insert into root.vehicle.d1(timestamp,s2) values(14,1024.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(29,1205.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(33,true)",
      "insert into root.test.d0(timestamp,s0) values(15,126)",
      "insert into root.test.d0(timestamp,s0,s1) values(8,127,'128')",
      "insert into root.test.d0(timestamp,s1) values(20,'129')",
      "insert into root.test.d1.g0(timestamp,s0) values(14,430)",
      "insert into root.test.d0(timestamp,s0) values(150,426)",
      "insert into root.test.d0(timestamp,s0,s1) values(80,427,'528')",
      "insert into root.test.d0(timestamp,s1) values(2,'1209')",
      "insert into root.test.d1.g0(timestamp,s0) values(4,330)"
  };
  private String[] queryStatementsWithoutFilter = {
      "select * from root",
      "select * from root.vehicle",
      "select * from root.test",
      "select vehicle.d0.s0, test.d0.s1 from root",
      "select vehicle.d1.s2, vehicle.d1.s3 ,test.d1.g0.s0 from root"
  };

  private Map<Integer, List<String>> queryCorrentResultsWithoutFilter = new HashMap<>();

  private String[] queryStatementsWithFilter = {
      "select * from root.vehicle where d0.s0 > 10",
      "select * from root.vehicle where d0.s0 < 101",
      "select * from root.vehicle where d0.s0 > 10 and d0.s0 < 101 or time = 12",
      "select * from root.test where d0.s0 > 10",
      "select * from root.test where d0.s0 > 10 and d0.s0 < 200 or d0.s0 = 3",
      "select * from root where vehicle.d0.s0 > 10 and test.d0.s0 < 101 or time = 20",
  };

  private Map<Integer, List<String>> queryCorrentResultsWithFilter = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    CLUSTER_CONFIG.createAllPath();
    server = Server.getInstance();
    server.start();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    EnvironmentUtils.cleanEnv();
  }

  private void initCorrectResultsWithoutFilter(){
    queryCorrentResultsWithoutFilter.put(0, new ArrayList<>());
    queryCorrentResultsWithoutFilter.put(1, new ArrayList<>());
    queryCorrentResultsWithoutFilter.put(2, new ArrayList<>());
    queryCorrentResultsWithoutFilter.put(3, new ArrayList<>());
    queryCorrentResultsWithoutFilter.put(4, new ArrayList<>());
    List<String> firstQueryRes = queryCorrentResultsWithoutFilter.get(0);
    firstQueryRes.add("1,null,null,null,null,null,null,110");
    firstQueryRes.add("2,null,null,null,null,null,1209,null");
    firstQueryRes.add("4,null,null,null,null,null,null,330");
    firstQueryRes.add("6,120,null,null,null,null,null,null");
    firstQueryRes.add("8,null,null,null,null,127,128,null");
    firstQueryRes.add("9,null,123,null,null,null,null,null");
    firstQueryRes.add("10,100,null,null,null,106,null,1100");
    firstQueryRes.add("11,null,null,104.0,null,null,null,null");
    firstQueryRes.add("12,101,102,null,null,null,null,null");
    firstQueryRes.add("14,null,null,1024.0,null,107,108,430");
    firstQueryRes.add("15,null,null,105.0,true,126,null,null");
    firstQueryRes.add("16,128,null,null,null,null,109,null");
    firstQueryRes.add("17,null,null,null,false,null,null,null");
    firstQueryRes.add("18,189,198,null,null,null,null,null");
    firstQueryRes.add("19,null,103,null,null,null,null,null");
    firstQueryRes.add("20,1000,null,null,null,null,129,null");
    firstQueryRes.add("21,null,null,1004.0,null,null,null,null");
    firstQueryRes.add("22,1001,1002,null,null,null,null,null");
    firstQueryRes.add("25,null,null,1005.0,true,null,null,null");
    firstQueryRes.add("27,null,null,null,true,null,null,null");
    firstQueryRes.add("29,null,1003,1205.0,true,null,null,null");
    firstQueryRes.add("30,null,null,null,null,1006,null,null");
    firstQueryRes.add("33,null,null,null,true,null,null,null");
    firstQueryRes.add("34,null,null,null,null,1007,1008,null");
    firstQueryRes.add("36,null,null,null,null,null,1090,null");
    firstQueryRes.add("38,121,122,null,null,null,null,null");
    firstQueryRes.add("80,null,null,null,null,427,528,null");
    firstQueryRes.add("99,null,1234,null,null,null,null,null");
    firstQueryRes.add("150,null,null,null,null,426,null,null");

    List<String> secondQueryRes = queryCorrentResultsWithoutFilter.get(1);
    secondQueryRes.add("6,120,null,null,null");
    secondQueryRes.add("9,null,123,null,null");
    secondQueryRes.add("10,100,null,null,null");
    secondQueryRes.add("11,null,null,104.0,null");
    secondQueryRes.add("12,101,102,null,null");
    secondQueryRes.add("14,null,null,1024.0,null");
    secondQueryRes.add("15,null,null,105.0,true");
    secondQueryRes.add("16,128,null,null,null");
    secondQueryRes.add("17,null,null,null,false");
    secondQueryRes.add("18,189,198,null,null");
    secondQueryRes.add("19,null,103,null,null");
    secondQueryRes.add("20,1000,null,null,null");
    secondQueryRes.add("21,null,null,1004.0,null");
    secondQueryRes.add("22,1001,1002,null,null");
    secondQueryRes.add("25,null,null,1005.0,true");
    secondQueryRes.add("27,null,null,null,true");
    secondQueryRes.add("29,null,1003,1205.0,true");
    secondQueryRes.add("33,null,null,null,true");
    secondQueryRes.add("38,121,122,null,null");
    secondQueryRes.add("99,null,1234,null,null");
    List<String> thirdQueryRes = queryCorrentResultsWithoutFilter.get(2);
    thirdQueryRes.add("1,null,null,110");
    thirdQueryRes.add("2,null,1209,null");
    thirdQueryRes.add("4,null,null,330");
    thirdQueryRes.add("8,127,128,null");
    thirdQueryRes.add("10,106,null,1100");
    thirdQueryRes.add("14,107,108,430");
    thirdQueryRes.add("15,126,null,null");
    thirdQueryRes.add("16,null,109,null");
    thirdQueryRes.add("20,null,129,null");
    thirdQueryRes.add("30,1006,null,null");
    thirdQueryRes.add("34,1007,1008,null");
    thirdQueryRes.add("36,null,1090,null");
    thirdQueryRes.add("80,427,528,null");
    thirdQueryRes.add("150,426,null,null");
    List<String> forthQueryRes = queryCorrentResultsWithoutFilter.get(3);
    forthQueryRes.add("2,null,1209");
    forthQueryRes.add("6,120,null");
    forthQueryRes.add("8,null,128");
    forthQueryRes.add("10,100,null");
    forthQueryRes.add("12,101,null");
    forthQueryRes.add("14,null,108");
    forthQueryRes.add("16,128,109");
    forthQueryRes.add("18,189,null");
    forthQueryRes.add("20,1000,129");
    forthQueryRes.add("22,1001,null");
    forthQueryRes.add("34,null,1008");
    forthQueryRes.add("36,null,1090");
    forthQueryRes.add("38,121,null");
    forthQueryRes.add("80,null,528");
    List<String> fifthQueryRes = queryCorrentResultsWithoutFilter.get(4);
    fifthQueryRes.add("1,null,null,110");
    fifthQueryRes.add("4,null,null,330");
    fifthQueryRes.add("10,null,null,1100");
    fifthQueryRes.add("11,104.0,null,null");
    fifthQueryRes.add("14,1024.0,null,430");
    fifthQueryRes.add("15,105.0,true,null");
    fifthQueryRes.add("17,null,false,null");
    fifthQueryRes.add("21,1004.0,null,null");
    fifthQueryRes.add("25,1005.0,true,null");
    fifthQueryRes.add("27,null,true,null");
    fifthQueryRes.add("29,1205.0,true,null");
    fifthQueryRes.add("33,null,true,null");
  }

  private void initCorrectResultsWithFilter(){
    queryCorrentResultsWithFilter.put(0, new ArrayList<>());
    queryCorrentResultsWithFilter.put(1, new ArrayList<>());
    queryCorrentResultsWithFilter.put(2, new ArrayList<>());
    queryCorrentResultsWithFilter.put(3, new ArrayList<>());
    queryCorrentResultsWithFilter.put(4, new ArrayList<>());
    queryCorrentResultsWithFilter.put(5, new ArrayList<>());
    List<String> firstQueryRes = queryCorrentResultsWithFilter.get(0);
    firstQueryRes.add("6,120,null,null,null");
    firstQueryRes.add("10,100,null,null,null");
    firstQueryRes.add("12,101,102,null,null");
    firstQueryRes.add("16,128,null,null,null");
    firstQueryRes.add("18,189,198,null,null");
    firstQueryRes.add("20,1000,null,null,null");
    firstQueryRes.add("22,1001,1002,null,null");
    firstQueryRes.add("38,121,122,null,null");

    List<String> secondQueryRes = queryCorrentResultsWithFilter.get(1);
    secondQueryRes.add("10,100,null,null,null");
    List<String> thirdQueryRes = queryCorrentResultsWithFilter.get(2);
    thirdQueryRes.add("10,100,null,null,null");
    thirdQueryRes.add("12,101,102,null,null");
    List<String> forthQueryRes = queryCorrentResultsWithFilter.get(3);
    forthQueryRes.add("8,127,128,null");
    forthQueryRes.add("10,106,null,1100");
    forthQueryRes.add("14,107,108,430");
    forthQueryRes.add("15,126,null,null");
    forthQueryRes.add("30,1006,null,null");
    forthQueryRes.add("34,1007,1008,null");
    forthQueryRes.add("80,427,528,null");
    forthQueryRes.add("150,426,null,null");
    List<String> fifthQueryRes = queryCorrentResultsWithFilter.get(4);
    fifthQueryRes.add("8,127,128,null");
    fifthQueryRes.add("10,106,null,1100");
    fifthQueryRes.add("14,107,108,430");
    fifthQueryRes.add("15,126,null,null");
    List<String> sixthQueryRes = queryCorrentResultsWithFilter.get(5);
    sixthQueryRes.add("20,1000,null,null,null,null,129,null");
  }

  @Test
  public void testLocalQueryWithoutFilter() throws Exception {
    initCorrectResultsWithoutFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();
      statement.execute("set read metadata level to 2");

      for(int i =0 ; i < queryStatementsWithoutFilter.length; i++) {
        String queryStatement = queryStatementsWithoutFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithoutFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      statement.close();
    }
  }

  @Test
  public void testLocalQueryWithFilter() throws Exception {

    initCorrectResultsWithFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithFilter.length; i++) {
        String queryStatement = queryStatementsWithFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      statement.close();
    }
  }

  @Test
  public void testRemoteQueryWithoutFilter() throws Exception {

    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    initCorrectResultsWithoutFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithoutFilter.length; i++) {
        String queryStatement = queryStatementsWithoutFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithoutFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
      statement.close();
    }
  }

  @Test
  public void testRemoteQueryWithFilter() throws Exception {

    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    initCorrectResultsWithFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithFilter.length; i++) {
        String queryStatement = queryStatementsWithFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
      statement.close();
    }
  }

  @Test
  public void testLocalQueryWithoutFilterByBatch() throws Exception {
    initCorrectResultsWithoutFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertBatchData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithoutFilter.length; i++) {
        String queryStatement = queryStatementsWithoutFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithoutFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      statement.close();
    }
  }

  @Test
  public void testLocalQueryWithFilterByBatch() throws Exception {

    initCorrectResultsWithFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertBatchData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithFilter.length; i++) {
        String queryStatement = queryStatementsWithFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      statement.close();
    }
  }

  @Test
  public void testRemoteQueryWithoutFilterByBatch() throws Exception {

    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    initCorrectResultsWithoutFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertBatchData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithoutFilter.length; i++) {
        String queryStatement = queryStatementsWithoutFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithoutFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
      statement.close();
    }
  }

  @Test
  public void testRemoteQueryWithFilterByBatch() throws Exception {

    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    initCorrectResultsWithFilter();
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertBatchData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      for(int i =0 ; i < queryStatementsWithFilter.length; i++) {
        String queryStatement = queryStatementsWithFilter[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResultsWithFilter.get(i);
        int count = 0;
        while (resultSet.next()) {
          String correctRow = correctResult.get(count++);
          StringBuilder rowRecordBuilder = new StringBuilder();
          for (int j = 1; j < columnCount; j++) {
            rowRecordBuilder.append(resultSet.getString(j)).append(",");
          }
          rowRecordBuilder.append(resultSet.getString(columnCount));
          assertEquals(correctRow, rowRecordBuilder.toString());
        }
      }
      QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
      statement.close();
    }
  }

}
