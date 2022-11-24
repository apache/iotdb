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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBLargeDataIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;

  @BeforeClass
  public static void setUp() throws Exception {

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(1000);
    ConfigFactory.getConfig().setPageSizeInByte(1024 * 150);
    ConfigFactory.getConfig().setGroupSizeInByte(1024 * 1000);
    ConfigFactory.getConfig().setMemtableSizeThreshold(1024 * 1000);

    EnvFactory.getEnv().initBeforeClass();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {

    // recovery value
    ConfigFactory.getConfig()
        .setMaxNumberOfPointsInPage(maxNumberOfPointsInPage)
        .setPageSizeInByte(pageSizeInByte)
        .setGroupSizeInByte(groupSizeInByte)
        .setMemtableSizeThreshold(groupSizeInByte);

    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.createSql) {
        statement.execute(sql);
      }

      // insert large amount of data time range : 13700 ~ 24000
      for (int time = 13700; time < 24000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        statement.execute(sql);
      }

      // insert large amount of data time range : 3000 ~ 13600
      for (int time = 3000; time < 13600; time++) {
        // System.out.println("===" + time);
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
        statement.execute(sql);
      }

      statement.execute("flush");
      statement.execute("compact");

      // buffwrite data, unsealed file
      for (int time = 100000; time < 101000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        statement.execute(sql);
      }

      statement.execute("flush");

      // sequential data, memory data
      for (int time = 200000; time < 201000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        statement.execute(sql);
      }

      // unseq insert, time < 3000
      for (int time = 2000; time < 2500; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
      }

      // seq insert, time > 200000
      for (int time = 200900; time < 201000; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select * from root.vehicle.**" : test select wild data
  @Test
  public void selectAllTest() {
    String selectSql = "select * from root.vehicle.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s1)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s2)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s3)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s4)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s5);
          cnt++;
        }

        assertEquals(23400, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s0 from root.vehicle.d0 where s0 >= 20" : test select same series with same series
  // filter
  @Test
  public void selectOneSeriesWithValueFilterTest() {

    String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0);
          // System.out.println("===" + ans);
          cnt++;
        }
        assertEquals(16440, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s0 from root.vehicle.d0 where time > 22987 " : test select clause with only global time
  // filter
  @Test
  public void seriesGlobalTimeFilterTest() {

    boolean hasResultSet;

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      hasResultSet = statement.execute("select s0 from root.vehicle.d0 where time > 22987");
      assertTrue(hasResultSet);

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0);
          //           System.out.println(ans);
          cnt++;
        }

        assertEquals(3012, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s1 from root.vehicle.d0 where s0 < 111" : test select clause with different series
  // filter
  @Test
  public void crossSeriesReadUpdateTest() {

    boolean hasResultSet;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      hasResultSet = statement.execute("select s1 from root.vehicle.d0 where s0 < 111");
      assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          long time = Long.parseLong(resultSet.getString(TestConstant.TIMESTAMP_STR));
          String value =
              resultSet.getString(TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s1);
          if (time > 200900) {
            assertEquals("7777", value);
          }
          // String ans = resultSet.getString(d0s1);
          cnt++;
        }
        assertEquals(22800, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
