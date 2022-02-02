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

public class MyTest3 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;

  @Before
  public void setUp() throws Exception {
    originalCompactionStrategy = config.getCompactionStrategy();
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    originalEnableCPV = config.isEnableCPV();
    //    config.setEnableCPV(false); // MOC
    config.setEnableCPV(true); // CPV

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setEnableCPV(originalEnableCPV);
  }

  @Test
  public void test1() {
    prepareData1();

    String[] res =
        new String[] {
          "0,1,20,5.0,20.0,5.0[1],30.0[10]",
          "25,25,45,8.0,30.0,8.0[25],40.0[30]",
          "50,52,54,8.0,18.0,8.0[52],18.0[54]",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");

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
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test1_2() { // test UDF MAC
    prepareData1();

    String[] res =
        new String[] {
          "0,FirstPoint=(1,5.0), LastPoint=(20,20.0), BottomPoint=(1,5.0), TopPoint=(10,30.0)",
          "25,FirstPoint=(25,8.0), LastPoint=(45,30.0), BottomPoint=(25,8.0), TopPoint=(30,40.0)",
          "50,FirstPoint=(52,8.0), LastPoint=(54,18.0), BottomPoint=(52,8.0), TopPoint=(54,18.0)",
          "75,empty"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      long tqs = 0L;
      long tqe = 100L;
      int w = 4;
      boolean hasResultSet =
          statement.execute(
              String.format(
                  "select M4(s0,'tqs'='%1$d','tqe'='%2$d','w'='%3$d') from root.vehicle.d0 where "
                      + "time>=%1$d and time<%2$d",
                  tqs, tqe, w));

      String columnName = "M4(root.vehicle.d0.s0, \"tqs\"=\"%d\", \"tqe\"=\"%d\", \"w\"=\"%d\")";
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format(columnName, tqs, tqe, w));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
