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

package org.apache.iotdb.db.integration;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBPreparedInsertionStatement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class IoTDBPreparedStmtIT {

  private static IoTDB daemon;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareSeries();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void prepareSeries() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("SET STORAGE GROUP TO root.device1");
      statement.execute("SET STORAGE GROUP TO root.device2");
      statement.execute("SET STORAGE GROUP TO root.device3");

      for (int i = 0; i < 5; i++) {
        statement.execute(String.format("CREATE TIMESERIES root.device1.sensor%d WITH "
            + "DATATYPE=DOUBLE,ENCODING=PLAIN", i));
        statement.execute(String.format("CREATE TIMESERIES root.device2.sensor%d WITH "
            + "DATATYPE=DOUBLE,ENCODING=PLAIN", i));
        statement.execute(String.format("CREATE TIMESERIES root.device3.sensor%d WITH "
            + "DATATYPE=DOUBLE,ENCODING=PLAIN", i));
      }
      // to create processors
      statement.execute("INSERT INTO root.device1(timestamp,sensor0,sensor1,sensor2,sensor3,"
          + "sensor4) VALUES (1,1,1,1,1,1)");
      statement.execute("INSERT INTO root.device2(timestamp,sensor0,sensor1,sensor2,sensor3,"
          + "sensor4) VALUES (1,1,1,1,1,1)");
      statement.execute("INSERT INTO root.device3(timestamp,sensor0,sensor1,sensor2,sensor3,"
          + "sensor4) VALUES (1,1,1,1,1,1)");
    }
  }

  @Test
  public void testPreparedInsertion() throws SQLException {


    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        IoTDBPreparedInsertionStatement statement =
            (IoTDBPreparedInsertionStatement) connection.prepareStatement("INSERT");
        Statement queryStmt = connection.createStatement()){
      statement.setDeviceId("root.device1");
      String[] measurements = new String[5];
      for (int i = 0; i < 5; i++) {
        measurements[i] = "sensor" + i;
      }
      statement.setMeasurements(Arrays.asList(measurements));
      String[] values = new String[5];
      for (int i = 1; i <= 100; i++) {
        statement.setTimestamp(i);
        for (int j = 0; j < 5; j ++) {
          values[j] = String.valueOf(j);
        }
        statement.setValues(Arrays.asList(values));
        statement.execute();
      }


      ResultSet resultSet = queryStmt.executeQuery("SELECT * FROM root.device1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
        assertEquals(cnt, resultSet.getLong(1));
        for (int i = 0; i < 5; i++) {
          assertEquals((double) i, resultSet.getDouble(i+2), 0.0001);
        }
      }
      assertEquals(100, cnt);
      resultSet.close();
    }
  }

  @Ignore
  @Test
  public void testPreparedInsertionPerf() throws SQLException {
    long preparedConsumption;
    long preparedIdealConsumption;
    long normalConsumption;

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        IoTDBPreparedInsertionStatement statement = (IoTDBPreparedInsertionStatement) connection
            .prepareStatement("INSERT")) {
      // normal usage, all parameters are updated

      long startTime = System.currentTimeMillis();
      String[] measurements = new String[5];
      for (int i = 0; i < 5; i++) {
        measurements[i] = "sensor" + i;
      }
      String[] values = new String[5];
      for (int i = 1000000; i <= 2000000; i++) {
        statement.setDeviceId("root.device1");
        statement.setMeasurements(Arrays.asList(measurements));
        statement.setTimestamp(i);
        for (int j = 0; j < 5; j++) {
          values[j] = String.valueOf(j);
        }
        statement.setValues(Arrays.asList(values));
        statement.execute();
      }
      preparedConsumption = System.currentTimeMillis() - startTime;

    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        IoTDBPreparedInsertionStatement statement = (IoTDBPreparedInsertionStatement) connection.prepareStatement("INSERT")) {
      // ideal usage, only value and time are updated
      long startTime = System.currentTimeMillis();
      statement.setDeviceId("root.device2");
      String[] measurements = new String[5];
      for (int i = 0; i < 5; i++) {
        measurements[i] = "sensor" + i;
      }
      statement.setMeasurements(Arrays.asList(measurements));
      String[] values = new String[5];
      for (int i = 1000000; i <= 2000000; i++) {
        statement.setTimestamp(i);
        for (int j = 0; j < 5; j ++) {
          values[j] = String.valueOf(j);
        }
        statement.setValues(Arrays.asList(values));
        statement.execute();
      }
      preparedIdealConsumption = System.currentTimeMillis() - startTime;

    }
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      long startTime = System.currentTimeMillis();
      String insertionTemplate = "INSERT INTO root.device3(timestamp,sensor0,sensor1,sensor2,"
          + "sensor3,sensor4) VALUES (%d,%d,%d,%d,%d,%d)";
      Object[] args = new Object[6];

      for (int i = 1000000; i <= 2000000; i++) {
        args[0] = i;
        for (int j = 0; j < 5; j ++) {
          args[j+1] = j;
        }
        statement.execute(String.format(insertionTemplate, args));
      }
      normalConsumption = System.currentTimeMillis() - startTime;
    }
    System.out.printf("Prepared statement costs %dms, ideal prepared statement costs %dms, normal "
            + "statement costs %dms \n",
        preparedConsumption, preparedIdealConsumption, normalConsumption);
  }
}
