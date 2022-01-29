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
package org.apache.iotdb.db.integration.m4;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

public class MyTestM4 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData1();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
  }

  /**
   * MAC: merge all chunks. Use UDTF to mimic the process of merging all chunks to calculate
   * aggregation points.
   */
  @Test
  public void testMAC() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function M4 as \"org.apache.iotdb.db.query.udf.builtin.UDTFM4MAC\"");

      long tqs = 0L;
      long tqe = 100L;
      int w = 4;
      boolean hasResultSet =
          statement.execute(
              String.format(
                  "select M4(s0,'tqs'='%1$d','tqe'='%2$d','w'='%3$d') from root.vehicle.d0 where "
                      + "time>=%1$d and time<%2$d",
                  tqs, tqe, w));

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      "M4(root.vehicle.d0.s0, \"tqs\"=\"0\", \"tqe\"=\"100\", \"w\"=\"4\")");
          System.out.println(ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** MOC: merge overlapping chunks. This is what IoTDB does. */
  @Test
  public void testMOC() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0),first_value(s0),"
                  + "max_time(s0), last_value(s0),"
                  + "max_value(s0), min_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0));
          System.out.println(ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * The data written is shown in figure:
   * https://user-images.githubusercontent.com/33376433/151664843-6afcb40d-fe6e-4ea8-bdd6-efe467f40c1c.png
   */
  private static void prepareData1() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      // seq1
      for (int i = 0; i <= 99; i++) {
        statement.addBatch(String.format(Locale.ENGLISH, insertTemplate, i, i));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("FLUSH");

      // unseq2
      for (int i = 10; i <= 28; i++) {
        statement.addBatch(String.format(Locale.ENGLISH, insertTemplate, i, i));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("FLUSH");

      // unseq3
      for (int i = 29; i <= 36; i++) {
        statement.addBatch(String.format(Locale.ENGLISH, insertTemplate, i, i));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("FLUSH");

      // unseq4
      for (int i = 37; i <= 60; i++) {
        statement.addBatch(String.format(Locale.ENGLISH, insertTemplate, i, i));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=26 and time<=27");
      statement.execute("delete from root.vehicle.d0.s0 where time>=35 and time<=40");
      statement.execute("delete from root.vehicle.d0.s0 where time>=48 and time<=75");
      statement.execute("delete from root.vehicle.d0.s0 where time>=85");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
