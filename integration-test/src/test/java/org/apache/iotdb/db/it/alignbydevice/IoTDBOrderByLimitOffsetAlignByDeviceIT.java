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
package org.apache.iotdb.db.it.alignbydevice;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.checkHeader;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData2;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.startPrecipitation;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.startTemperature;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBOrderByLimitOffsetAlignByDeviceIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertData2();
    insertData3();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ORDER BY TIME
  @Test
  public void orderByTimeTest1() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME LIMIT 20 ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
        long lastTimeStamp = -1;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp >= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(20, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByCanNotPushLimitTest() {
    // 1. value filter, can not push down LIMIT
    String[] expectedHeader = new String[] {"Time,Device,s1"};
    String[] retArray = new String[] {"3,root.db.d1,111,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** WHERE s1>40 ORDER BY TIME LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by expression, can not push down LIMIT
    retArray = new String[] {"3,root.db.d3,333,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** ORDER BY s1 DESC LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. time filter, can push down LIMIT
    retArray = new String[] {"3,root.db.d3,33,", "2,root.db.d2,22,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** WHERE time>1 and time<3 ORDER BY device DESC LIMIT 2 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 4. both exist OFFSET and LIMIT, can push down LIMIT as OFFSET+LIMIT
    retArray = new String[] {"3,root.db.d2,222,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** ORDER BY time DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  private static final String[] SQL_LIST =
      new String[] {
        "CREATE DATABASE root.db;",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.db.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "INSERT INTO root.db.d1(timestamp,s1) VALUES(1, 1), (2, 11), (3, 111);",
        "INSERT INTO root.db.d2(timestamp,s1) VALUES(1, 2), (2, 22), (3, 222);",
        "INSERT INTO root.db.d3(timestamp,s1) VALUES(1, 3), (2, 33), (3, 333);",
      };

  protected static void insertData3() {
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection();
        Statement statement = iotDBConnection.createStatement()) {
      for (String sql : SQL_LIST) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
