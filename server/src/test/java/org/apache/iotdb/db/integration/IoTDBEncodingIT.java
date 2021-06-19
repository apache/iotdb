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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IoTDBEncodingIT {

  private static int partitionInterval = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("REGULAR");
    StorageEngine.setEnablePartition(true);
    StorageEngine.setTimePartitionInterval(partitionInterval);
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    StorageEngine.setEnablePartition(false);
    StorageEngine.setTimePartitionInterval(-1);
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSetEncodingRegularFailed() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=REGULAR");

    } catch (SQLException e) {
      assertEquals(303, e.getErrorCode());
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderTS_2DIFF() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=TS_2DIFF");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");

      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderTS_2DIFFOutofOrder() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=TS_2DIFF");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderRLE() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=RLE");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");
      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderRLEOutofOrder() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=RLE");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderGORILLA() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=GORILLA");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(3,1300)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,1400)");
      statement.execute("flush");

      int[] result = new int[] {1100, 1200, 1300, 1400};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderGORILLAOutofOrder() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.salary WITH DATATYPE=INT64,ENCODING=GORILLA");
      statement.execute("insert into root.db_0.tab0(time,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(7,1000)");
      statement.execute("insert into root.db_0.tab0(time,salary) values(4,2200)");
      statement.execute("flush");

      int[] result = new int[] {1200, 1100, 2200, 1000};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
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
  public void testSetTimeEncoderRegularAndValueEncoderDictionary() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.city WITH DATATYPE=TEXT,ENCODING=DICTIONARY");
      statement.execute("insert into root.db_0.tab0(time,city) values(1,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(2,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(3,\"Beijing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(4,\"Shanghai\")");
      statement.execute("flush");

      String[] result = new String[] {"Nanjing", "Nanjing", "Beijing", "Shanghai"};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          String city = resultSet.getString("root.db_0.tab0.city");
          assertEquals(result[index], city);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetTimeEncoderRegularAndValueEncoderDictionaryOutOfOrder() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.db_0.tab0.city WITH DATATYPE=TEXT,ENCODING=DICTIONARY");
      statement.execute("insert into root.db_0.tab0(time,city) values(1,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(2,\"Nanjing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(4,\"Beijing\")");
      statement.execute("insert into root.db_0.tab0(time,city) values(3,\"Shanghai\")");
      statement.execute("flush");

      String[] result = new String[] {"Nanjing", "Nanjing", "Shanghai", "Beijing"};
      try (ResultSet resultSet = statement.executeQuery("select * from root.db_0.tab0")) {
        int index = 0;
        while (resultSet.next()) {
          String city = resultSet.getString("root.db_0.tab0.city");
          assertEquals(result[index], city);
          index++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void insertData() throws ClassNotFoundException {
    List<String> sqls =
        new ArrayList<>(
            Arrays.asList(
                "SET STORAGE GROUP TO root.test1",
                "SET STORAGE GROUP TO root.test2",
                "CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=PLAIN",
                "CREATE TIMESERIES root.test2.s0 WITH DATATYPE=INT64,ENCODING=PLAIN"));
    // 10 partitions, each one with one seq file and one unseq file
    for (int i = 0; i < 10; i++) {
      // seq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(
            String.format(
                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
                j, i * partitionInterval + 50, i * partitionInterval + 50));
      }
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
      // unseq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(
            String.format(
                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
                j, i * partitionInterval, i * partitionInterval));
      }
      sqls.add("MERGE");
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
    }
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
