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

package org.apache.iotdb.db.integration.eBUG;

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

public class MyTest {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;
  private static boolean originalUseChunkIndex;

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    originalCompactionStrategy = config.getCompactionStrategy();
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    //    config.setTimestampPrecision("ms");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
  }

  @Test
  public void test1() {
    prepareData1();

    //        String[] res =
    //                new String[]{
    //                        "0,1,20,5.0,20.0,5.0[1],30.0[10]",
    //                        "25,25,45,8.0,30.0,8.0[25],40.0[30]",
    //                        "50,52,54,8.0,18.0,8.0[52],18.0[54]",
    //                        "75,null,null,null,null,null,null"
    //                };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("SELECT EBUG(s0,'m'='10','e'='0') FROM root.vehicle.d0");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(1) + "," + resultSet.getString(2);
          System.out.println(ans);
          //                    Assert.assertEquals(res[i++], ans);
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

      for (int i = 0; i < 1000; i++) {
        double x = 2 * Math.PI * i / 1000.0;
        double sineValue = Math.sin(x);
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, i, sineValue));
      }

      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
