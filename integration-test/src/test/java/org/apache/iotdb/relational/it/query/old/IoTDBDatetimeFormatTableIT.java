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

package org.apache.iotdb.relational.it.query.old;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDatetimeFormatTableIT {

  private static final String DATABASE_NAME = "db";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecisionCheckEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE_NAME);
      statement.execute("use " + DATABASE_NAME);
      statement.execute(
          "create table table1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 DOUBLE MEASUREMENT)");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDatetimeInputFormat() {
    String[] datetimeStrings = {
      "2022-01-01 01:02:03", // yyyy-MM-dd HH:mm:ss
      "2022/01/02 01:02:03", // yyyy/MM/dd HH:mm:ss
      "2022.01.03 01:02:03", // yyyy.MM.dd HH:mm:ss
      "2022-01-04 01:02:03+01:00", // yyyy-MM-dd HH:mm:ssZZ
      "2022/01/05 01:02:03+01:00", // yyyy/MM/dd HH:mm:ssZZ
      "2022.01.06 01:02:03+01:00", // yyyy.MM.dd HH:mm:ssZZ
      "2022-01-07 01:02:03.400", // yyyy-MM-dd HH:mm:ss.SSS
      "2022/01/08 01:02:03.400", // yyyy/MM/dd HH:mm:ss.SSS
      "2022.01.09 01:02:03.400", // yyyy.MM.dd HH:mm:ss.SSS
      "2022-01-10 01:02:03.400+01:00", // yyyy-MM-dd HH:mm:ss.SSSZZ
      "2022-01-11 01:02:03.400+01:00", // yyyy/MM/dd HH:mm:ss.SSSZZ
      "2022-01-12 01:02:03.400+01:00", // yyyy.MM.dd HH:mm:ss.SSSZZ
      "2022-01-13T01:02:03.400+01:00" // ISO8601 standard time format
    };
    long[] timestamps = {
      1640970123000L,
      1641056523000L,
      1641142923000L,
      1641254523000L,
      1641340923000L,
      1641427323000L,
      1641488523400L,
      1641574923400L,
      1641661323400L,
      1641772923400L,
      1641859323400L,
      1641945723400L,
      1642032123400L
    };
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      connection.setClientInfo("time_zone", "+08:00");
      statement.execute("use " + DATABASE_NAME);

      for (int i = 0; i < datetimeStrings.length; i++) {
        String insertSql =
            String.format(
                "INSERT INTO table1(time, device_id, s1) values (%s, %s, %d)",
                datetimeStrings[i], "'d1'", i);
        statement.execute(insertSql);
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT time, s1 FROM table1 where device_id='d1'")) {
        Assert.assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          Assert.assertEquals(timestamps[cnt], resultSet.getLong(1));
          cnt++;
        }
        Assert.assertEquals(timestamps.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testBigDateTime() {

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);

      statement.execute(
          "insert into table1(time,device_id,s2) values (1618283005586000, 'd2',8.76)");

      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, s2 from table1 where device_id='d2' and time=53251-05-07T17:06:26.000+08:00")) {
        Assert.assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          Assert.assertEquals(1618283005586000L, resultSet.getLong(1));
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
