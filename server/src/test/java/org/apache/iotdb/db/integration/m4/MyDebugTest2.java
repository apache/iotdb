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

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MyDebugTest2 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[]{
          "SET STORAGE GROUP TO root.game",
          "CREATE TIMESERIES root.game.s6 WITH DATATYPE=INT64",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

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
    config.setAvgSeriesPointNumberThreshold(10000); // this step cannot be omitted
    config.setTimestampPrecision("ns");

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

    String[] res = new String[]{};
    try (Connection connection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_time(s6), max_time(s6), first_value(s6), last_value(s6), min_value(s6), "
                  + "max_value(s6) from root.game group by ([426460081333,852920162666), 426460081333ns)");

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
    try (Connection connection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      String path = "";
      File f = new File(path);
      String line = null;
      BufferedReader reader = new BufferedReader(new FileReader(f));
      while ((line = reader.readLine()) != null) {
        String[] split = line.split(",");
        long timestamp = Long.valueOf(split[0]);
        long value = Long.valueOf(split[1]);
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, timestamp, value));
      }

      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
