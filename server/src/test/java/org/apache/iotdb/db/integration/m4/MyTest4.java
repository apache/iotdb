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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

public class MyTest4 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT64, ENCODING=PLAIN",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;

  private static boolean originalUseChunkIndex;

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    originalCompactionStrategy = config.getCompactionStrategy();
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    originalEnableCPV = config.isEnableCPV();
    config.setEnableCPV(true); // CPV

    originalUseChunkIndex = TSFileDescriptor.getInstance().getConfig().isUseTimeIndex();
    TSFileDescriptor.getInstance().getConfig().setUseTimeIndex(false);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    config.setTimestampPrecision("ms");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setEnableCPV(originalEnableCPV);
    TSFileDescriptor.getInstance().getConfig().setUseTimeIndex(originalUseChunkIndex);
  }

  @Test
  public void test1() {
    prepareData1();

    String[] res = new String[] {"0,1,14,7,6,2[6],8[2]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,25),25ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/176371221-1ecd5cdf-155b-4a00-a8b9-c793464cf289.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10000, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 13, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 12, 2));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=8 and time<=10");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2() {
    prepareData2();

    String[] res = new String[] {"0,1,12,7,2,2[6],8[10]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,13),13ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/176371428-b7b04db1-827b-4324-82b9-7e409f1a5b2e.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10000, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 12, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 13, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 6));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=8 and time<=8");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3() {
    prepareData3();

    String[] res = new String[] {"0,3,14,6,6,0[10],7[11]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,25),25ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3() {
    // data:
    // https://user-images.githubusercontent.com/33376433/176371648-b101be5d-d3cc-4673-9af8-50574d22a864.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10000, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 13, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 12, 2));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=1 and time<=2");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test4() {
    prepareData4();

    String[] res = new String[] {"0,4,14,2,6,0[10],7[11]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,25),25ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData4() {
    // data:
    // https://user-images.githubusercontent.com/33376433/176371811-62d8c175-fb8c-4a5e-84ab-1088ba6779d7.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10000, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 13, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 12, 2));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=1 and time<=2");
      statement.execute("delete from root.vehicle.d0.s0 where time>=2 and time<=3");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test5() {
    prepareData5();

    String[] res = new String[] {"0,1,13,5,3,0[10],8[2]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,25),25ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData5() {
    // data:
    // https://user-images.githubusercontent.com/33376433/176371991-207e306a-5a0c-443b-9ada-527309f3c42a.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10000, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 6));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 2));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 13, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 6));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 7));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 14, 2));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=14 and time<=14");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
