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

package org.apache.iotdb.db.integration.aggregation;

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

import static org.apache.iotdb.db.constant.TestConstant.first_value;
import static org.apache.iotdb.db.constant.TestConstant.last_value;
import static org.junit.Assert.fail;

public class IoTDBAggregationScanOrderIT {

  private static final String[] sqls =
      new String[] {
        "insert into root.sg1.d1(time, s1) values (12, 12);",
        "flush;",
        "insert into root.sg1.d1(time, s2) values (30, 30);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (0, 0);",
        "insert into root.sg1.d1(time, s1) values (8, 8);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (0, 0);",
        "insert into root.sg1.d1(time, s1) values (10, 10);",
        "flush;",
        "insert into root.sg1.d1(time, s1) values (17, 17);",
        "insert into root.sg1.d1(time, s1) values (20, 20);",
        "flush;"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    insertSQL();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
  }

  @Test
  public void test() throws ClassNotFoundException {
    String expectedRet = "0.0,20.0";
    String d1s1 = "root.sg1.d1.s1";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select first_value(s1), last_value(s1) from root.sg1.d1;");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        String ans =
            resultSet.getString(first_value(d1s1)) + "," + resultSet.getString(last_value(d1s1));
        Assert.assertEquals(expectedRet, ans);
        Assert.assertFalse(resultSet.next());
      }

      hasResultSet =
          statement.execute(
              "select first_value(s1), last_value(s1) from root.sg1.d1 order by time desc;");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        String ans =
            resultSet.getString(first_value(d1s1)) + "," + resultSet.getString(last_value(d1s1));
        Assert.assertEquals(expectedRet, ans);
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
