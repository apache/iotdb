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

public class MyTest_ILTS_2 {

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
    config.setAcc_convex(true);
    config.setAcc_iterRepeat(true);
    TSFileDescriptor.getInstance().getConfig().setWriteConvexHull(true);

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
    config.setNumIterations(8);
    String res = "10.0[2],1.0[10],8.0[30],5.0[55],20.0[62],1.0[90],7.0[102],";
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
}
