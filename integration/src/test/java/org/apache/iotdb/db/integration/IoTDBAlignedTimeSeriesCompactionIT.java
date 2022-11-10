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

import org.apache.iotdb.integration.env.EnvFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class IoTDBAlignedTimeSeriesCompactionIT {
  private static final String storageGroup = "root.compactionTestForAligned";
  private final double E = 0.001;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + storageGroup);
    }
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete database " + storageGroup);
    }
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testSimpleAlignedCompaction() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        while (time < nextTime) {
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time + 1, time + 2, time + 3, time + 4));
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time * 2, time * 3, time * 4, time * 5));
          time++;
        }
        statement.execute("FLUSH");
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        Assert.assertEquals(resultTime + 1, d1s1, E);
        Assert.assertEquals(resultTime + 2, d1s2, E);
        Assert.assertEquals(resultTime + 3, d1s3, E);
        Assert.assertEquals(resultTime + 4, d1s4, E);

        Assert.assertEquals(resultTime * 2, d2s1);
        Assert.assertEquals(resultTime * 3, d2s2);
        Assert.assertEquals(resultTime * 4, d2s3);
        Assert.assertEquals(resultTime * 5, d2s4);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }

  @Test
  public void testAlignedTsFileWithNullValue() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      Map<String, Map<Long, Long>> valueMap = new HashMap<>();
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        while (time < nextTime) {
          if (time % 2 == 0) {
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                    time, time + 1, time + 2, time + 3, time + 4));
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                    time, time * 2, time * 3, time * 4, time * 5));
            valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, time + 1);
            valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
            valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
            valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, time + 4);
            valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, time * 2);
            valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
            valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, time * 4);
            valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
          } else {
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, null)",
                    time, time + 1, time + 2, time + 3));
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, null, %d, %d, %d)",
                    time, time * 3, time * 4, time * 5));
            valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, time + 1);
            valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
            valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
            valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
            valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, time * 4);
            valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
          }
          time++;
        }
        statement.execute("FLUSH");
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        Assert.assertEquals(valueMap.get("d1s1").get(resultTime).doubleValue(), d1s1, E);
        Assert.assertEquals(valueMap.get("d1s2").get(resultTime).doubleValue(), d1s2, E);
        Assert.assertEquals(valueMap.get("d1s3").get(resultTime).doubleValue(), d1s3, E);
        Assert.assertEquals(valueMap.get("d1s4").get(resultTime).doubleValue(), d1s4, E);

        Assert.assertEquals((long) valueMap.get("d2s1").get(resultTime), d2s1);
        Assert.assertEquals((long) valueMap.get("d2s2").get(resultTime), d2s2);
        Assert.assertEquals((long) valueMap.get("d2s3").get(resultTime), d2s3);
        Assert.assertEquals((long) valueMap.get("d2s4").get(resultTime), d2s4);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }

  @Test
  public void testAlignedTsFileWithDifferentSeriesInDifferentTsFile() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      Map<String, Map<Long, Long>> valueMap = new HashMap<>();
      Random random = new Random(10);
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        if (i % 2 == 0) {
          while (time < nextTime) {
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                    time, time + 1, time + 2, time + 3, time + 4));
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                    time, time * 2, time * 3, time * 4, time * 5));
            valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, time + 1);
            valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
            valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
            valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, time + 4);
            valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, time * 2);
            valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
            valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, time * 4);
            valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
            time++;
          }
        } else {
          while (time < nextTime) {
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, null, %d, %d, null)",
                    time, time + 2, time + 3));
            statement.execute(
                String.format(
                    "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, null, %d, null, %d)",
                    time, time * 3, time * 5));
            valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
            valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
            valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
            valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, 0L);
            valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
            time++;
          }
        }
        statement.execute("FLUSH");
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        Assert.assertEquals(valueMap.get("d1s1").get(resultTime).doubleValue(), d1s1, E);
        Assert.assertEquals(valueMap.get("d1s2").get(resultTime).doubleValue(), d1s2, E);
        Assert.assertEquals(valueMap.get("d1s3").get(resultTime).doubleValue(), d1s3, E);
        Assert.assertEquals(valueMap.get("d1s4").get(resultTime).doubleValue(), d1s4, E);

        Assert.assertEquals((long) valueMap.get("d2s1").get(resultTime), d2s1);
        Assert.assertEquals((long) valueMap.get("d2s2").get(resultTime), d2s2);
        Assert.assertEquals((long) valueMap.get("d2s3").get(resultTime), d2s3);
        Assert.assertEquals((long) valueMap.get("d2s4").get(resultTime), d2s4);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }

  @Test
  public void testAlignedTsFileWithDifferentDevicesInDifferentTsFile() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      Map<String, Map<Long, Long>> valueMap = new HashMap<>();
      Random random = new Random(10);
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        long tempTime = time;
        while (tempTime < nextTime) {
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  tempTime, tempTime + 1, tempTime + 2, tempTime + 3, tempTime + 4));
          valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(tempTime, tempTime + 1);
          valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(tempTime, tempTime + 2);
          valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(tempTime, tempTime + 3);
          valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(tempTime, tempTime + 4);
          tempTime++;
        }
        statement.execute("FLUSH");
        while (time < nextTime) {
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, null, %d, null, %d)",
                  time, time * 3, time * 5));
          valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, 0L);
          valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
          valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, 0L);
          valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
          time++;
        }
        statement.execute("FLUSH");
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        Assert.assertEquals(valueMap.get("d1s1").get(resultTime).doubleValue(), d1s1, E);
        Assert.assertEquals(valueMap.get("d1s2").get(resultTime).doubleValue(), d1s2, E);
        Assert.assertEquals(valueMap.get("d1s3").get(resultTime).doubleValue(), d1s3, E);
        Assert.assertEquals(valueMap.get("d1s4").get(resultTime).doubleValue(), d1s4, E);

        Assert.assertEquals((long) valueMap.get("d2s1").get(resultTime), d2s1);
        Assert.assertEquals((long) valueMap.get("d2s2").get(resultTime), d2s2);
        Assert.assertEquals((long) valueMap.get("d2s3").get(resultTime), d2s3);
        Assert.assertEquals((long) valueMap.get("d2s4").get(resultTime), d2s4);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }

  @Test
  public void testAlignedTsFileWithDifferentDataType() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
          "create timeseries root.compactionTestForAligned.d3.s1 DOUBLE encoding=PLAIN",
          "create timeseries root.compactionTestForAligned.d3.s2 DOUBLE encoding=PLAIN",
          "create timeseries root.compactionTestForAligned.d3.s3 DOUBLE encoding=PLAIN",
          "create timeseries root.compactionTestForAligned.d3.s4 DOUBLE encoding=PLAIN",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      Map<String, Map<Long, Long>> valueMap = new HashMap<>();
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        while (time < nextTime) {
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time + 1, time + 2, time + 3, time + 4));
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time * 2, time * 3, time * 4, time * 5));
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d3(time, s1, s2, s3, s4) values (%d, %d, %d, %d, %d)",
                  time, time * 20, time * 30, time * 40, time * 50));
          valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, time + 1);
          valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
          valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
          valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, time + 4);
          valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, time * 2);
          valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
          valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, time * 4);
          valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
          valueMap.computeIfAbsent("d3s1", x -> new TreeMap<>()).put(time, time * 20);
          valueMap.computeIfAbsent("d3s2", x -> new TreeMap<>()).put(time, time * 30);
          valueMap.computeIfAbsent("d3s3", x -> new TreeMap<>()).put(time, time * 40);
          valueMap.computeIfAbsent("d3s4", x -> new TreeMap<>()).put(time, time * 50);
          time++;
        }
        statement.execute("FLUSH");
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        double d3s1 = resultSet.getDouble("root.compactionTestForAligned.d3.s1");
        double d3s2 = resultSet.getDouble("root.compactionTestForAligned.d3.s2");
        double d3s3 = resultSet.getDouble("root.compactionTestForAligned.d3.s3");
        double d3s4 = resultSet.getDouble("root.compactionTestForAligned.d3.s4");

        Assert.assertEquals(valueMap.get("d1s1").get(resultTime).doubleValue(), d1s1, E);
        Assert.assertEquals(valueMap.get("d1s2").get(resultTime).doubleValue(), d1s2, E);
        Assert.assertEquals(valueMap.get("d1s3").get(resultTime).doubleValue(), d1s3, E);
        Assert.assertEquals(valueMap.get("d1s4").get(resultTime).doubleValue(), d1s4, E);

        Assert.assertEquals((long) valueMap.get("d2s1").get(resultTime), d2s1);
        Assert.assertEquals((long) valueMap.get("d2s2").get(resultTime), d2s2);
        Assert.assertEquals((long) valueMap.get("d2s3").get(resultTime), d2s3);
        Assert.assertEquals((long) valueMap.get("d2s4").get(resultTime), d2s4);

        Assert.assertEquals(valueMap.get("d3s1").get(resultTime).doubleValue(), d3s1, E);
        Assert.assertEquals(valueMap.get("d3s2").get(resultTime).doubleValue(), d3s2, E);
        Assert.assertEquals(valueMap.get("d3s3").get(resultTime).doubleValue(), d3s3, E);
        Assert.assertEquals(valueMap.get("d3s4").get(resultTime).doubleValue(), d3s4, E);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }

  @Test
  public void testAlignedTsFileWithModification() throws Exception {
    String[] prepareSql =
        new String[] {
          "create aligned timeseries root.compactionTestForAligned.d1(s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE)",
          "create aligned timeseries root.compactionTestForAligned.d2(s1 INT64, s2 INT64, s3 INT64, s4 INT64)",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : prepareSql) {
        statement.execute(sql);
      }
      long time = 1;
      Map<String, Map<Long, Long>> valueMap = new HashMap<>();
      for (int i = 0; i < 30; ++i) {
        long nextTime = time + 100;
        while (time < nextTime) {
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d1(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time + 1, time + 2, time + 3, time + 4));
          statement.execute(
              String.format(
                  "insert into root.compactionTestForAligned.d2(time, s1, s2, s3, s4) aligned values (%d, %d, %d, %d, %d)",
                  time, time * 2, time * 3, time * 4, time * 5));
          valueMap.computeIfAbsent("d1s1", x -> new TreeMap<>()).put(time, time + 1);
          valueMap.computeIfAbsent("d1s2", x -> new TreeMap<>()).put(time, time + 2);
          valueMap.computeIfAbsent("d1s3", x -> new TreeMap<>()).put(time, time + 3);
          valueMap.computeIfAbsent("d1s4", x -> new TreeMap<>()).put(time, time + 4);
          valueMap.computeIfAbsent("d2s1", x -> new TreeMap<>()).put(time, time * 2);
          valueMap.computeIfAbsent("d2s2", x -> new TreeMap<>()).put(time, time * 3);
          valueMap.computeIfAbsent("d2s3", x -> new TreeMap<>()).put(time, time * 4);
          valueMap.computeIfAbsent("d2s4", x -> new TreeMap<>()).put(time, time * 5);
          time++;
        }
        statement.execute("FLUSH");
      }
      Random random = new Random(10);
      for (int i = 0; i < 10; i++) {
        long deleteTime = random.nextLong() % (30 * 100);
        statement.execute(
            "delete from root.compactionTestForAligned.d1.s1 where time = " + deleteTime);
        valueMap.get("d1s1").put(deleteTime, 0L);
      }
      for (int i = 0; i < 10; i++) {
        long deleteTime = random.nextLong() % (30 * 100);
        statement.execute(
            "delete from root.compactionTestForAligned.d2.s1 where time = " + deleteTime);
        valueMap.get("d2s1").put(deleteTime, 0L);
      }

      statement.execute("MERGE");

      Thread.sleep(500);
      ResultSet resultSet =
          statement.executeQuery("select * from root.compactionTestForAligned.**");

      int count = 0;
      while (resultSet.next()) {
        count++;
        long resultTime = resultSet.getLong("Time");
        double d1s1 = resultSet.getDouble("root.compactionTestForAligned.d1.s1");
        double d1s2 = resultSet.getDouble("root.compactionTestForAligned.d1.s2");
        double d1s3 = resultSet.getDouble("root.compactionTestForAligned.d1.s3");
        double d1s4 = resultSet.getDouble("root.compactionTestForAligned.d1.s4");

        long d2s1 = resultSet.getLong("root.compactionTestForAligned.d2.s1");
        long d2s2 = resultSet.getLong("root.compactionTestForAligned.d2.s2");
        long d2s3 = resultSet.getLong("root.compactionTestForAligned.d2.s3");
        long d2s4 = resultSet.getLong("root.compactionTestForAligned.d2.s4");

        Assert.assertEquals(valueMap.get("d1s1").get(resultTime).doubleValue(), d1s1, E);
        Assert.assertEquals(valueMap.get("d1s2").get(resultTime).doubleValue(), d1s2, E);
        Assert.assertEquals(valueMap.get("d1s3").get(resultTime).doubleValue(), d1s3, E);
        Assert.assertEquals(valueMap.get("d1s4").get(resultTime).doubleValue(), d1s4, E);

        Assert.assertEquals((long) valueMap.get("d2s1").get(resultTime), d2s1);
        Assert.assertEquals((long) valueMap.get("d2s2").get(resultTime), d2s2);
        Assert.assertEquals((long) valueMap.get("d2s3").get(resultTime), d2s3);
        Assert.assertEquals((long) valueMap.get("d2s4").get(resultTime), d2s4);
      }
      Assert.assertEquals(30 * 100, count);
    }
  }
}
