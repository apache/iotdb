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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBOrderByWithLimitAlignByDeviceIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertData2();
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
}
