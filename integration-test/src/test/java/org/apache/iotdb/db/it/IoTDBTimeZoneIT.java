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
package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBTimeZoneIT {

  private static String[] insertSqls =
      new String[] {
        "CREATE DATABASE root.timezone",
        "CREATE TIMESERIES root.timezone.tz1 WITH DATATYPE = INT32, ENCODING = PLAIN",
      };
  private final String TIMESTAMP_STR = "Time";
  private final String tz1 = "root.timezone.tz1";

  // private boolean testFlag = TestUtils.testFlag;
  private String[] retArray =
      new String[] {
        "1514775603000,4",
        "1514779200000,1",
        "1514779201000,2",
        "1514779202000,3",
        "1514779203000,8",
        "1514782804000,5",
        "1514782805000,7",
        "1514782806000,9",
        "1514782807000,10",
        "1514782808000,11",
        "1514782809000,12",
        "1514782810000,13",
        "1514789200000,6",
      };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeseries();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * // execute in cli-tool CREATE DATABASE root.timezone CREATE TIMESERIES root.timezone.tz1 WITH
   * DATATYPE = INT32, ENCODING = PLAIN set time_zone=+08:00 insert into
   * root.timezone(timestamp,tz1) values(1514779200000,1) insert into root.timezone(timestamp,tz1)
   * values(2018-1-1T12:00:01,2) insert into root.timezone(timestamp,tz1)
   * values(2018-1-1T12:00:02+08:00,3) insert into root.timezone(timestamp,tz1)
   * values(2018-1-1T12:00:03+09:00,4) insert into root.timezone(timestamp,tz1)
   * values(2018-1-1T12:00:04+07:00,5)
   *
   * <p>set time_zone=+09:00 insert into root.timezone(timestamp,tz1) values(1514789200000,6) insert
   * into root.timezone(timestamp,tz1) values(2018-1-1T14:00:05,7) insert into
   * root.timezone(timestamp,tz1) values(2018-1-1T12:00:03+08:00,8) insert into
   * root.timezone(timestamp,tz1) values(2018-1-1T12:00:04+07:00,9) set time_zone=Asia/Almaty insert
   * into root.timezone(timestamp,tz1) values(1514782807000,10) insert into
   * root.timezone(timestamp,tz1) values(2018-1-1T11:00:08,11) insert into
   * root.timezone(timestamp,tz1) values(2018-1-1T13:00:09+08:00,12) insert into
   * root.timezone(timestamp,tz1) values(2018-1-1T12:00:10+07:00,13)
   *
   * <p>select * from root.**
   */
  @Test
  public void timezoneTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String insertSQLTemplate = "insert into root.timezone(timestamp,tz1) values(%s,%s)";
      connection.setClientInfo("time_zone", "+08:00");
      // 1514779200000 = 2018-1-1T12:00:00+08:00
      statement.execute(String.format(insertSQLTemplate, "1514779200000", "1"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:01", "2"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:02+08:00", "3"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:03+09:00", "4"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:04+07:00", "5"));

      connection.setClientInfo("time_zone", "+09:00");
      statement.execute(String.format(insertSQLTemplate, "1514789200000", "6"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T14:00:05", "7"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:03+08:00", "8"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:06+07:00", "9"));

      // Asia/Almaty +06:00
      connection.setClientInfo("time_zone", "Asia/Almaty");
      statement.execute(String.format(insertSQLTemplate, "1514782807000", "10"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T11:00:08", "11"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T13:00:09+08:00", "12"));
      statement.execute(String.format(insertSQLTemplate, "2018-1-1T12:00:10+07:00", "13"));

      ResultSet resultSet = statement.executeQuery("select * from root.**");
      int cnt = 0;
      try {
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(tz1);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      } finally {
        resultSet.close();
      }
      Assert.assertEquals(13, cnt);
    }
  }

  private void createTimeseries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
