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

public class MyTest_ILTS {

  /*
   * Requirements:
   * (1) Don't change the sequence of the above two aggregates
   * (2) Assume each chunk has only one page.
   * (3) Assume all chunks are sequential and no deletes.
   * (4) Assume plain encoding, UNCOMPRESSED, Long or Double data type, no compaction
   * (5) Assume no empty bucket
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

    config.setEnableTri("ILTS");
    config.setAcc_avg(true);
    config.setAcc_rectangle(true);
    config.setAcc_convex(false);
    config.setAcc_iterRepeat(true);
    TSFileDescriptor.getInstance().getConfig().setWriteConvexHull(false);

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
  public void test1() {
    prepareData1();
    config.setNumIterations(4);
    String res = "5.0[1],10.0[2],2.0[40],5.0[55],20.0[62],1.0[90],7.0[102],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0)"
                  // TODO not real min_value here, actually controlled by enableTri
                  + ",max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([2,102),20ms)");
      // (102-2)/(7-2)=20ms
      // note keep no empty buckets

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test1_2() {
    prepareData1();
    config.setNumIterations(1); // result equals LTTB
    String res = "5.0[1],10.0[2],2.0[40],15.0[60],18.0[70],1.0[90],7.0[102],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0)"
                  // TODO not real min_value here, actually controlled by enableTri
                  + ",max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([2,102),20ms)");
      // (102-2)/(7-2)=20ms
      // note keep no empty buckets

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

  @Test
  public void test1_3() {
    prepareData1();
    config.setNumIterations(4);
    String res = "5.0[1],2.0[40],20.0[62],7.0[102],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0)"
                  // TODO not real min_value here, actually controlled by enableTri
                  + ",max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([2,102),40ms)");
      // (102-2)/(7-2)=20ms
      // note keep no empty buckets

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    // slightly modified
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      long[] t = new long[] {1, 2, 10, 20, 22, 30, 40, 55, 60, 62, 65, 70, 72, 80, 90, 102};
      double[] v = new double[] {5, 10, 1, 5, 4, 8, 2, 5, 15, 20, 8, 18, 4, 11, 1, 7};
      config.setP1t(t[0]);
      config.setP1v(v[0]);
      config.setPnt(t[t.length - 1]);
      config.setPnv(v[v.length - 1]);

      for (int i = 0; i < t.length; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, t[i], v[i]));
        if ((i + 1) % 4 == 0) {
          statement.execute("FLUSH");
        }
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2() {
    prepareData2();
    config.setNumIterations(8);
    String res =
        "-1.2079272[0],1.101946[200],-0.523204[300],0.145359[500],-1.014322[700],"
            + "0.532565[900],-0.122525[1200],-0.676077[1300],0.809559[1500],0.315869[1800],"
            + "-0.413534[1900],-0.0211206[2100],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0)"
                  // TODO not real min_value here, actually controlled by enableTri
                  + ",max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([100,2100),200ms)");
      // (tn-t2)/(nout-2)=(2100-100)/(12-2)=2000/10=200

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

  @Test
  public void test2_2() {
    prepareData2();
    config.setNumIterations(1); // result equals LTTB
    String res =
        "-1.2079272[0],1.101946[200],-0.523204[300],0.145359[500],-1.014322[700],0.532565[900],"
            + "-0.146934[1100],-0.676077[1300],0.809559[1500],0.032211[1700],"
            + "-0.413534[1900],-0.0211206[2100],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0)"
                  // TODO not real min_value here, actually controlled by enableTri
                  + ",max_value(s0),min_time(s0), max_time(s0), first_value(s0), last_value(s0)"
                  + " FROM root.vehicle.d0 group by ([100,2100),200ms)");
      // (tn-t2)/(nout-2)=(2100-100)/(12-2)=2000/10=200

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

  private static void prepareData2() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      int[] t =
          new int[] {
            0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500,
            1600, 1700, 1800, 1900, 2000, 2100
          };
      double[] v =
          new double[] {
            -1.2079272,
            -0.01120245,
            1.1019456,
            -0.52320362,
            -0.35970289,
            0.1453591,
            -0.45947892,
            -1.0143219,
            0.81760821,
            0.5325646,
            -0.29532424,
            -0.1469335,
            -0.12252526,
            -0.67607713,
            -0.16967308,
            0.8095585,
            -0.78541944,
            0.03221141,
            0.31586886,
            -0.41353356,
            -0.21019539,
            -0.0211206
          };
      config.setP1t(t[0]);
      config.setP1v(v[0]);
      config.setPnt(t[t.length - 1]);
      config.setPnv(v[v.length - 1]);

      for (int i = 0; i < t.length; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, t[i], v[i]));
        if ((i + 1) % 4 == 0) {
          statement.execute("FLUSH");
        }
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
