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

package org.apache.iotdb.db.integration.tri;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBStatement;
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

public class MyTest_M4 {

  /*
   * Requirements:
   * (1) Don't change the sequence of the above two aggregates
   * (2) Assume each chunk has only one page.
   * (3) Assume all chunks are sequential and no deletes.
   * (4) Assume plain encoding, UNCOMPRESSED, Double data type, no compaction
   */
  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        // IoTDB int data type does not support plain encoding, so use long data type
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    config.setTimestampPrecision("ms");
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    config.setEnableTri("M4");
    //    config.setEnableTri("SC");
    //    config.setEpsilon(0.1);

    config.setP1t(0);
    config.setP1v(0);
    config.setPnt(200);
    config.setPnv(200);

    config.setEnableCPV(false);
    TSFileDescriptor.getInstance().getConfig().setEnableMinMaxLSM(false);
    TSFileDescriptor.getInstance().getConfig().setUseStatistics(false);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test1() throws Exception {
    prepareData1();
    String res =
        "0.0[0],1.0[20],15.0[2],5.0[1],1.0[20],8.0[25],8.0[25],8.0[25],8.0[25],3.0[54],"
            + "3.0[54],3.0[54],3.0[54],null[null],null[null],null[null],null[null],200.0[200],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    // only first chunk
    // no overlap, no delete
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8.0));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3() {
    prepareData3();

    String res =
        "0.0[0],1.0[10],10.0[2],5.0[1],4.0[22],2.0[40],8.0[30],8.0[30],2.0[40],"
            + "4.0[72],20.0[62],5.0[55],4.0[72],1.0[90],11.0[80],11.0[80],1.0[90],200.0[200],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "SELECT min_value(s0), max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      int i = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(2);
        System.out.println(ans);
        Assert.assertEquals(res, ans);
      }
      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5.0));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5.0));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18.0));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7.0));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3_2() {
    prepareData3_2();

    String res =
        "0.0[0],1.0[10],10.0[2],5.0[1],5.0[20],null[null],null[null],null[null],null[null],4.0[72],"
            + "20.0[62],15.0[60],4.0[72],1.0[90],11.0[80],11.0[80],1.0[90],200.0[200],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3_2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    // remove chunk 2 so that bucket 2 is empty
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5.0));
      statement.execute("FLUSH");

      //            statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4.0));
      //            statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8.0));
      //            statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2.0));
      //            statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5.0));
      //            statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18.0));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7.0));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
