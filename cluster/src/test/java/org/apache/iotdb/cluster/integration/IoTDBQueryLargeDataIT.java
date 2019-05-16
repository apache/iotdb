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
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBResultMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBQueryLargeDataIT {


  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  private static final String URL = "127.0.0.1:6667/";

  private static final String[] createSQLs1 = {
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
  private static final String[] insertSQLs1 = {
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
      "insert into root.test.d1.g0(timestamp,s0) values(10,1100)"};
  private static final String[] insertSqls2 = {
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
      "insert into root.test.d1.g0(timestamp,s0) values(4,330)"};
  private static final String[] createSQLs3 = {
      "SET STORAGE GROUP TO root.iotdb",
      "SET STORAGE GROUP TO root.cluster",
      "CREATE TIMESERIES root.iotdb.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.cluster.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.cluster.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.cluster.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE"
  };
  private static final String[] insertSQLs3 = {
      "DELETE FROM root.vehicle WHERE time < 20",
      "DELETE FROM root.test WHERE time < 20",
      "insert into root.iotdb.d0(timestamp,s0) values(3,100)",
      "insert into root.iotdb.d0(timestamp,s0,s1) values(22,101,'102')",
      "insert into root.iotdb.d0(timestamp,s1) values(24,'103')",
      "insert into root.iotdb.d1(timestamp,s2) values(21,104.0)",
      "insert into root.iotdb.d1(timestamp,s2,s3) values(25,105.0,true)",
      "insert into root.iotdb.d1(timestamp,s3) values(27,false)",
      "insert into root.iotdb.d0(timestamp,s0) values(30,1000)",
      "insert into root.iotdb.d0(timestamp,s0,s1) values(202,101,'102')",
      "insert into root.iotdb.d0(timestamp,s1) values(44,'103')",
      "insert into root.iotdb.d1(timestamp,s2) values(1,404.0)",
      "insert into root.iotdb.d1(timestamp,s2,s3) values(250,10.0,true)",
      "insert into root.iotdb.d1(timestamp,s3) values(207,false)",
      "insert into root.cluster.d0(timestamp,s0) values(20,106)",
      "insert into root.cluster.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.cluster.d1.g0(timestamp,s0) values(1,110)",
      "insert into root.cluster.d0(timestamp,s0) values(200,1006)",
      "insert into root.cluster.d0(timestamp,s0,s1) values(1004,1007,'1080')",
      "insert into root.cluster.d1.g0(timestamp,s0) values(1000,910)",
      "insert into root.vehicle.d0(timestamp,s0) values(209,130)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(206,131,'132')",
      "insert into root.vehicle.d0(timestamp,s1) values(70,'33')",
      "insert into root.vehicle.d1(timestamp,s2) values(204,14.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(29,135.0,false)",
      "insert into root.vehicle.d1(timestamp,s3) values(14,false)",
      "insert into root.test.d0(timestamp,s0) values(19,136)",
      "insert into root.test.d0(timestamp,s0,s1) values(7,137,'138')",
      "insert into root.test.d0(timestamp,s1) values(30,'139')",
      "insert into root.test.d1.g0(timestamp,s0) values(4,150)",
      "insert into root.test.d0(timestamp,s0) values(1900,1316)",
      "insert into root.test.d0(timestamp,s0,s1) values(700,1307,'1038')",
      "insert into root.test.d0(timestamp,s1) values(3000,'1309')",
      "insert into root.test.d1.g0(timestamp,s0) values(400,1050)"
  };

  private static final String[] querys1  ={
      "select * from root.vehicle",
      "select * from root.test",
      "select * from root.vehicle,root.test where time = 11 or time = 12",
      "select * from root.vehicle,root.test where d0.s0 > 10 and d0.s1 < 301 or time = 12",
      "select * from root"
  };
  private static final String[] querys2  ={
      "select * from root.vehicle",
      "select * from root.test",
      "select * from root.vehicle,root.test where time = 11 or time = 16",
      "select * from root.vehicle,root.test where d0.s0 > 10 and d0.s1 < 301 or time = 20",
      "select * from root"
  };
  private static final String[] querys3  ={
      "select * from root.vehicle",
      "select * from root.test",
      "select * from root.cluster",
      "select * from root.vehicle,root.test where time = 11 or time = 14",
      "select * from root.vehicle,root.test where d0.s0 > 0 and d0.s1 < 1001 or time = 14",
      "select * from root"
  };

  private Map<Integer, List<String>> queryCorrentResults = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
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

  private void initCorrectResults1(){
    queryCorrentResults.clear();
    queryCorrentResults.put(0, new ArrayList<>());
    queryCorrentResults.put(1, new ArrayList<>());
    queryCorrentResults.put(2, new ArrayList<>());
    queryCorrentResults.put(3, new ArrayList<>());
    queryCorrentResults.put(4, new ArrayList<>());
    List<String> firstQueryRes = queryCorrentResults.get(0);
    firstQueryRes.add("10,100,null,null,null");
    firstQueryRes.add("11,null,null,104.0,null");
    firstQueryRes.add("12,101,102,null,null");
    firstQueryRes.add("15,null,null,105.0,true");
    firstQueryRes.add("17,null,null,null,false");
    firstQueryRes.add("19,null,103,null,null");
    firstQueryRes.add("20,1000,null,null,null");
    firstQueryRes.add("21,null,null,1004.0,null");
    firstQueryRes.add("22,1001,1002,null,null");
    firstQueryRes.add("25,null,null,1005.0,true");
    firstQueryRes.add("27,null,null,null,true");
    firstQueryRes.add("29,null,1003,null,null");

    List<String> secondQueryRes = queryCorrentResults.get(1);
    secondQueryRes.add("1,null,null,110");
    secondQueryRes.add("10,106,null,1100");
    secondQueryRes.add("14,107,108,null");
    secondQueryRes.add("16,null,109,null");
    secondQueryRes.add("30,1006,null,null");
    secondQueryRes.add("34,1007,1008,null");
    secondQueryRes.add("36,null,1090,null");

    List<String> thirdQueryRes = queryCorrentResults.get(2);
    thirdQueryRes.add("11,null,null,104.0,null,null,null,null");
    thirdQueryRes.add("12,101,102,null,null,null,null,null");

    List<String> forthQueryRes = queryCorrentResults.get(3);
    forthQueryRes.add("12,101,102,null,null,null,null,null");

    List<String> fifthQueryRes = queryCorrentResults.get(4);
    fifthQueryRes.add("1,null,null,null,null,null,null,110");
    fifthQueryRes.add("10,100,null,null,null,106,null,1100");
    fifthQueryRes.add("11,null,null,104.0,null,null,null,null");
    fifthQueryRes.add("12,101,102,null,null,null,null,null");
    fifthQueryRes.add("14,null,null,null,null,107,108,null");
    fifthQueryRes.add("15,null,null,105.0,true,null,null,null");
    fifthQueryRes.add("16,null,null,null,null,null,109,null");
    fifthQueryRes.add("17,null,null,null,false,null,null,null");
    fifthQueryRes.add("19,null,103,null,null,null,null,null");
    fifthQueryRes.add("20,1000,null,null,null,null,null,null");
    fifthQueryRes.add("21,null,null,1004.0,null,null,null,null");
    fifthQueryRes.add("22,1001,1002,null,null,null,null,null");
    fifthQueryRes.add("25,null,null,1005.0,true,null,null,null");
    fifthQueryRes.add("27,null,null,null,true,null,null,null");
    fifthQueryRes.add("29,null,1003,null,null,null,null,null");
    fifthQueryRes.add("30,null,null,null,null,1006,null,null");
    fifthQueryRes.add("34,null,null,null,null,1007,1008,null");
    fifthQueryRes.add("36,null,null,null,null,null,1090,null");
  }

  private void initCorrectResults2(){
    queryCorrentResults.clear();
    queryCorrentResults.put(0, new ArrayList<>());
    queryCorrentResults.put(1, new ArrayList<>());
    queryCorrentResults.put(2, new ArrayList<>());
    queryCorrentResults.put(3, new ArrayList<>());
    queryCorrentResults.put(4, new ArrayList<>());
    List<String> firstQueryRes = queryCorrentResults.get(0);
    firstQueryRes.add("6,120,null,null,null");
    firstQueryRes.add("9,null,123,null,null");
    firstQueryRes.add("10,100,null,null,null");
    firstQueryRes.add("11,null,null,104.0,null");
    firstQueryRes.add("12,101,102,null,null");
    firstQueryRes.add("14,null,null,1024.0,null");
    firstQueryRes.add("15,null,null,105.0,true");
    firstQueryRes.add("16,128,null,null,null");
    firstQueryRes.add("17,null,null,null,false");
    firstQueryRes.add("18,189,198,null,null");
    firstQueryRes.add("19,null,103,null,null");
    firstQueryRes.add("20,1000,null,null,null");
    firstQueryRes.add("21,null,null,1004.0,null");
    firstQueryRes.add("22,1001,1002,null,null");
    firstQueryRes.add("25,null,null,1005.0,true");
    firstQueryRes.add("27,null,null,null,true");
    firstQueryRes.add("29,null,1003,1205.0,true");
    firstQueryRes.add("33,null,null,null,true");
    firstQueryRes.add("38,121,122,null,null");
    firstQueryRes.add("99,null,1234,null,null");

    List<String> secondQueryRes = queryCorrentResults.get(1);
    secondQueryRes.add("1,null,null,110");
    secondQueryRes.add("2,null,1209,null");
    secondQueryRes.add("4,null,null,330");
    secondQueryRes.add("8,127,128,null");
    secondQueryRes.add("10,106,null,1100");
    secondQueryRes.add("14,107,108,430");
    secondQueryRes.add("15,126,null,null");
    secondQueryRes.add("16,null,109,null");
    secondQueryRes.add("20,null,129,null");
    secondQueryRes.add("30,1006,null,null");
    secondQueryRes.add("34,1007,1008,null");
    secondQueryRes.add("36,null,1090,null");
    secondQueryRes.add("80,427,528,null");
    secondQueryRes.add("150,426,null,null");

    List<String> thirdQueryRes = queryCorrentResults.get(2);
    thirdQueryRes.add("11,null,null,104.0,null,null,null,null");
    thirdQueryRes.add("16,128,null,null,null,null,109,null");

    List<String> forthQueryRes = queryCorrentResults.get(3);
    forthQueryRes.add("20,1000,null,null,null,null,129,null");

    List<String> fifthQueryRes = queryCorrentResults.get(4);
    fifthQueryRes.add("1,null,null,null,null,null,null,110");
    fifthQueryRes.add("2,null,null,null,null,null,1209,null");
    fifthQueryRes.add("4,null,null,null,null,null,null,330");
    fifthQueryRes.add("6,120,null,null,null,null,null,null");
    fifthQueryRes.add("8,null,null,null,null,127,128,null");
    fifthQueryRes.add("9,null,123,null,null,null,null,null");
    fifthQueryRes.add("10,100,null,null,null,106,null,1100");
    fifthQueryRes.add("11,null,null,104.0,null,null,null,null");
    fifthQueryRes.add("12,101,102,null,null,null,null,null");
    fifthQueryRes.add("14,null,null,1024.0,null,107,108,430");
    fifthQueryRes.add("15,null,null,105.0,true,126,null,null");
    fifthQueryRes.add("16,128,null,null,null,null,109,null");
    fifthQueryRes.add("17,null,null,null,false,null,null,null");
    fifthQueryRes.add("18,189,198,null,null,null,null,null");
    fifthQueryRes.add("19,null,103,null,null,null,null,null");
    fifthQueryRes.add("20,1000,null,null,null,null,129,null");
    fifthQueryRes.add("21,null,null,1004.0,null,null,null,null");
    fifthQueryRes.add("22,1001,1002,null,null,null,null,null");
    fifthQueryRes.add("25,null,null,1005.0,true,null,null,null");
    fifthQueryRes.add("27,null,null,null,true,null,null,null");
    fifthQueryRes.add("29,null,1003,1205.0,true,null,null,null");
    fifthQueryRes.add("30,null,null,null,null,1006,null,null");
    fifthQueryRes.add("33,null,null,null,true,null,null,null");
    fifthQueryRes.add("34,null,null,null,null,1007,1008,null");
    fifthQueryRes.add("36,null,null,null,null,null,1090,null");
    fifthQueryRes.add("38,121,122,null,null,null,null,null");
    fifthQueryRes.add("80,null,null,null,null,427,528,null");
    fifthQueryRes.add("99,null,1234,null,null,null,null,null");
    fifthQueryRes.add("150,null,null,null,null,426,null,null");
  }

  private void initCorrectResults3(){
    queryCorrentResults.clear();
    queryCorrentResults.put(0, new ArrayList<>());
    queryCorrentResults.put(1, new ArrayList<>());
    queryCorrentResults.put(2, new ArrayList<>());
    queryCorrentResults.put(3, new ArrayList<>());
    queryCorrentResults.put(4, new ArrayList<>());
    queryCorrentResults.put(5, new ArrayList<>());
    List<String> zeroQueryRes = queryCorrentResults.get(0);
    zeroQueryRes.add("14,null,null,null,false");
    zeroQueryRes.add("20,1000,null,null,null");
    zeroQueryRes.add("21,null,null,1004.0,null");
    zeroQueryRes.add("22,1001,1002,null,null");
    zeroQueryRes.add("25,null,null,1005.0,true");
    zeroQueryRes.add("27,null,null,null,true");
    zeroQueryRes.add("29,null,1003,135.0,false");
    zeroQueryRes.add("33,null,null,null,true");
    zeroQueryRes.add("38,121,122,null,null");
    zeroQueryRes.add("70,null,33,null,null");
    zeroQueryRes.add("99,null,1234,null,null");
    zeroQueryRes.add("204,null,null,14.0,null");
    zeroQueryRes.add("206,131,132,null,null");
    zeroQueryRes.add("209,130,null,null,null");

    List<String> firstQueryRes = queryCorrentResults.get(1);
    firstQueryRes.add("4,null,null,150");
    firstQueryRes.add("7,137,138,null");
    firstQueryRes.add("19,136,null,null");
    firstQueryRes.add("20,null,129,null");
    firstQueryRes.add("30,1006,139,null");
    firstQueryRes.add("34,1007,1008,null");
    firstQueryRes.add("36,null,1090,null");
    firstQueryRes.add("80,427,528,null");
    firstQueryRes.add("150,426,null,null");
    firstQueryRes.add("400,null,null,1050");
    firstQueryRes.add("700,1307,1038,null");
    firstQueryRes.add("1900,1316,null,null");
    firstQueryRes.add("3000,null,1309,null");

    List<String> secondQueryRes = queryCorrentResults.get(2);
    secondQueryRes.add("1,null,null,110");
    secondQueryRes.add("14,107,108,null");
    secondQueryRes.add("20,106,null,null");
    secondQueryRes.add("200,1006,null,null");
    secondQueryRes.add("1000,null,null,910");
    secondQueryRes.add("1004,1007,1080,null");

    List<String> thirdQueryRes = queryCorrentResults.get(3);
    thirdQueryRes.add("14,null,null,null,false,null,null,null");

    List<String> forthQueryRes = queryCorrentResults.get(4);
    forthQueryRes.add("14,null,null,null,false,null,null,null");

    List<String> fifthQueryRes = queryCorrentResults.get(5);
    fifthQueryRes.add("1,null,null,404.0,null,null,null,null,null,null,null,null,null,null,110");
    fifthQueryRes.add("3,100,null,null,null,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("4,null,null,null,null,null,null,null,null,null,null,150,null,null,null");
    fifthQueryRes.add("7,null,null,null,null,null,null,null,null,137,138,null,null,null,null");
    fifthQueryRes.add("14,null,null,null,null,null,null,null,false,null,null,null,107,108,null");
    fifthQueryRes.add("19,null,null,null,null,null,null,null,null,136,null,null,null,null,null");
    fifthQueryRes.add("20,null,null,null,null,1000,null,null,null,null,129,null,106,null,null");
    fifthQueryRes.add("21,null,null,104.0,null,null,null,1004.0,null,null,null,null,null,null,null");
    fifthQueryRes.add("22,101,102,null,null,1001,1002,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("24,null,103,null,null,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("25,null,null,105.0,true,null,null,1005.0,true,null,null,null,null,null,null");
    fifthQueryRes.add("27,null,null,null,false,null,null,null,true,null,null,null,null,null,null");
    fifthQueryRes.add("29,null,null,null,null,null,1003,135.0,false,null,null,null,null,null,null");
    fifthQueryRes.add("30,1000,null,null,null,null,null,null,null,1006,139,null,null,null,null");
    fifthQueryRes.add("33,null,null,null,null,null,null,null,true,null,null,null,null,null,null");
    fifthQueryRes.add("34,null,null,null,null,null,null,null,null,1007,1008,null,null,null,null");
    fifthQueryRes.add("36,null,null,null,null,null,null,null,null,null,1090,null,null,null,null");
    fifthQueryRes.add("38,null,null,null,null,121,122,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("44,null,103,null,null,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("70,null,null,null,null,null,33,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("80,null,null,null,null,null,null,null,null,427,528,null,null,null,null");
    fifthQueryRes.add("99,null,null,null,null,null,1234,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("150,null,null,null,null,null,null,null,null,426,null,null,null,null,null");
    fifthQueryRes.add("200,null,null,null,null,null,null,null,null,null,null,null,1006,null,null");
    fifthQueryRes.add("202,101,102,null,null,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("204,null,null,null,null,null,null,14.0,null,null,null,null,null,null,null");
    fifthQueryRes.add("206,null,null,null,null,131,132,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("207,null,null,null,false,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("209,null,null,null,null,130,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("250,null,null,10.0,true,null,null,null,null,null,null,null,null,null,null");
    fifthQueryRes.add("400,null,null,null,null,null,null,null,null,null,null,1050,null,null,null");
    fifthQueryRes.add("700,null,null,null,null,null,null,null,null,1307,1038,null,null,null,null");
    fifthQueryRes.add("1000,null,null,null,null,null,null,null,null,null,null,null,null,null,910");
    fifthQueryRes.add("1004,null,null,null,null,null,null,null,null,null,null,null,1007,1080,null");
    fifthQueryRes.add("1900,null,null,null,null,null,null,null,null,1316,null,null,null,null,null");
    fifthQueryRes.add("3000,null,null,null,null,null,null,null,null,null,1309,null,null,null,null");
  }

  @Test
  public void testClusterQueryWithLargeData() throws Exception {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      Statement statement = connection.createStatement();

       //first round
      insertData(connection, createSQLs1, insertSQLs1);
      initCorrectResults1();
      for(int i =0 ; i < querys1.length; i++) {
        String queryStatement = querys1[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResults.get(i);
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

      // second round
      insertData(connection, new String[]{}, insertSqls2);
      initCorrectResults2();
      for(int i =0 ; i < querys2.length; i++) {
        String queryStatement = querys2[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResults.get(i);
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

      // third round
      insertData(connection, createSQLs3, insertSQLs3);
      initCorrectResults3();
      for(int i =0 ; i < querys3.length; i++) {
        String queryStatement = querys3[i];
        boolean hasResultSet = statement.execute(queryStatement);
        assertTrue(hasResultSet);
        ResultSet resultSet = statement.getResultSet();
        IoTDBResultMetadata resultSetMetaData = (IoTDBResultMetadata) resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<String> correctResult = queryCorrentResults.get(i);
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
}
