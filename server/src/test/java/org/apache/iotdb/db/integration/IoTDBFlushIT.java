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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IoTDBFlushIT {

  private static int partitionInterval = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    StorageEngine.setEnablePartition(true);
    StorageEngine.setTimePartitionInterval(partitionInterval);
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    StorageEngine.setEnablePartition(false);
    StorageEngine.setTimePartitionInterval(-1);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSetEncodingRegularSimpleTimeseriesFlush() {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("flush");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularMultiTimeseriesFlush() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("create timeseries root.db_0.tab0.two with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,two) values(1,200)");
      statement.execute("insert into root.db_0.tab0(time ,two) values(2,300)");
      statement.execute("flush");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(2, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularMultiTimeseriesFlushAndInsertRegularData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("create timeseries root.db_0.tab0.two with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,two) values(1,200)");
      statement.execute("insert into root.db_0.tab0(time ,two) values(2,300)");
      statement.execute("flush");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndReviseData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1400)");
      statement.execute("flush");
      int[] result = new int[] {1000, 1100, 1200, 1300, 1400};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndReviseDataAndInsertRegularData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1400)");
      statement.execute("flush");

      statement.execute("insert into root.db_0.tab0(time ,salary) values(6,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(7,1600)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(8,1700)");
      statement.execute("flush");
      int[] result = new int[] {1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndLongReviseDataAndMissData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1400)");
      statement.execute("flush");

      statement.execute("insert into root.db_0.tab0(time ,salary) values(6,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(7,1600)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(8,1700)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(9,2000)");
      statement.execute("flush");
      int[] result = new int[] {1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 2000};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndInsertLongDescDataReviseData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1400)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1000)");
      statement.execute("flush");

      int[] result = new int[] {1400, 1300, 1200, 1100, 1000};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndInsertIntDescDataReviseData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT32,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1400)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1000)");
      statement.execute("flush");

      int[] result = new int[] {1400, 1300, 1200, 1100, 1000};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularFlushAndIntReviseDataAndMissData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT32,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(5,1400)");
      statement.execute("flush");

      statement.execute("insert into root.db_0.tab0(time ,salary) values(6,1500)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(7,1600)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(8,1700)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(9,2000)");
      statement.execute("flush");
      int[] result = new int[] {1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 2000};

      try (ResultSet resultSet = statement.executeQuery("SELECT salary FROM root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          int salary = resultSet.getInt("root.db_0.tab0.salary");
          assertEquals(result[index], salary);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void insertData() throws ClassNotFoundException {
    List<String> sqls = new ArrayList<>(Arrays.asList(
            "SET STORAGE GROUP TO root.test1",
            "SET STORAGE GROUP TO root.test2",
            "CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=PLAIN",
            "CREATE TIMESERIES root.test2.s0 WITH DATATYPE=INT64,ENCODING=PLAIN"
    ));
    // 10 partitions, each one with one seq file and one unseq file
    for (int i = 0; i < 10; i++) {
      // seq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(String.format("INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)", j,
                i * partitionInterval + 50, i * partitionInterval + 50));
      }
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
      // unseq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(String.format("INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)", j,
                i * partitionInterval, i * partitionInterval));
      }
      sqls.add("MERGE");
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
    }
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
