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

public class MyDebugTest1 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE, ENCODING=RLE",
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

    config.setEnableCPV(false);
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
    // 现象：为什么synData1写成[0,10000000)反而更奇怪了？想象中的MOC在m=1k的时候每个interval刚好一个chunk从而不需要load任何chunk的画面没有出现
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
                  + " FROM root.vehicle.d0 group by ([0,100),10ms)");

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

      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, i, Math.random()));
      }

      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
