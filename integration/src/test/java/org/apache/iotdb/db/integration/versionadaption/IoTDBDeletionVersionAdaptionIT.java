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

package org.apache.iotdb.db.integration.versionadaption;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
@Ignore // No longer forward compatible since v0.14
public class IoTDBDeletionVersionAdaptionIT {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      };

  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4" + ") VALUES(%d,%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DELETE FROM root.vehicle.d0.* WHERE time <= 10000";
  private long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeTest();
    prepareSeries();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void test() throws SQLException {
    prepareData();
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.s0  WHERE time <= 300");
      statement.execute(
          "DELETE FROM root.vehicle.d0.s1,root.vehicle.d0.s2,root.vehicle.d0.s3"
              + " WHERE time <= 350");
      statement.execute("DELETE FROM root.vehicle.d0 WHERE time <= 150");

      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(50, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testMerge() throws SQLException {
    prepareMerge();

    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      statement.execute("merge");
      statement.execute("DELETE FROM root.vehicle.d0 WHERE time <= 15000");

      // before merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(5000, cnt);
      }

      // after merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(5000, cnt);
      }
      cleanData();
    }
  }

  @Test
  public void testRangeDelete() throws SQLException {
    prepareData();
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time <= 300");
      statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }

      statement.execute("DELETE FROM root.vehicle.d0 WHERE time > 50 and time <= 250");
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testPartialPathRangeDelete() throws SQLException {
    prepareData();
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.* WHERE time <= 300 and time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      statement.execute("DELETE FROM root.vehicle.*.s0 WHERE time <= 100");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testDeleteAll() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.lz.dev.GPS(time, latitude, longitude) values(9,3.2,9.8)");
      statement.execute("insert into root.lz.dev.GPS(time, latitude) values(11,4.5)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("delete from root.lz");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  private static void prepareSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite file
      for (int i = 201; i <= 300; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("merge");
      // prepare Unseq-File
      for (int i = 1; i <= 100; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("merge");
      // prepare BufferWrite cache
      for (int i = 301; i <= 400; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow cache
      for (int i = 101; i <= 200; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
    }
  }

  private void cleanData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      statement.execute(deleteAllTemplate);
    }
  }

  public void prepareMerge() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite data
      for (int i = 10001; i <= 20000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow data
      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
    }
  }
}
