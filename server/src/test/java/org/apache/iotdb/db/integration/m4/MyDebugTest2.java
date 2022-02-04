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

import java.util.Random;
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

public class MyDebugTest2 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
          "SET STORAGE GROUP TO root.vehicle.d0",
          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;
  private static int originalAvgSeriesPointNumberThreshold;
  private static long originalSeqTsFileSize;
  private static long originalUnSeqTsFileSize;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    originalEnableCPV = config.isEnableCPV();
    originalCompactionStrategy = config.getCompactionStrategy();
    originalAvgSeriesPointNumberThreshold = config.getAvgSeriesPointNumberThreshold();
    originalSeqTsFileSize = config.getSeqTsFileSize();
    originalUnSeqTsFileSize = config.getUnSeqTsFileSize();

    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    config.setSeqTsFileSize(1024 * 1024 * 1024); // 1G
    config.setUnSeqTsFileSize(1024 * 1024 * 1024); // 1G
    config.setAvgSeriesPointNumberThreshold(10); // this step cannot be omitted

//    config.setEnableCPV(false);
    config.setEnableCPV(true);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setAvgSeriesPointNumberThreshold(originalAvgSeriesPointNumberThreshold);
    config.setEnableCPV(originalEnableCPV);
    config.setSeqTsFileSize(originalSeqTsFileSize);
    config.setUnSeqTsFileSize(originalUnSeqTsFileSize);
  }

  @Test
  public void test1() {
    // 现像：fullGame实验中，CPV第一个interval结果还是对的，第二个Interval开始结果不对了
    prepareData1();

    String[] res =
        new String[] {
            "0,1,20,5,20,5[1],30[10]",
            "25,25,45,8,30,8[25],40[30]",
            "50,52,54,8,18,8[52],18[54]",
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
          //          Assert.assertEquals(res[i++], ans);
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
      Random random = new Random(100);

      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, i, random.nextDouble()));
      }

      for (int i = 4; i < 75; i=i+4) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, i, random.nextDouble()));
      }

      for (int i = 8; i < 90; i=i+3) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, i, random.nextDouble()));
      }

//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 15, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 16, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 17, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 18, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 24, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 26, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 31, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 32, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 41, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 42, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 43, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 49, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 50, 5));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 51, 1));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 4));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 25));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 3));
//
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 4));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 11, 25));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 32, 2));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 3));

      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
