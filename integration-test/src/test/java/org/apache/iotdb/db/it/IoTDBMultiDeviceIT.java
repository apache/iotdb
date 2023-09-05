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
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMultiDeviceIT {

  @Before
  public void setUp() throws Exception {
    // use small page
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxNumberOfPointsInPage(100)
        .setPageSizeInByte(1024 * 15)
        .setGroupSizeInByte(1024 * 100)
        .setMemtableSizeThreshold(1024 * 100)
        .setPartitionInterval(100)
        .setQueryThreadCount(2)
        .setCompressor("LZ4");

    EnvFactory.getEnv().initClusterEnvironment();

    insertData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.createSql) {
        statement.addBatch(sql);
      }

      statement.addBatch("CREATE DATABASE root.fans");
      statement.addBatch("CREATE TIMESERIES root.fans.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.fans.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.fans.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.fans.d3.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.car.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.car.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.addBatch("CREATE TIMESERIES root.car.d2.s1 WITH DATATYPE=INT64, ENCODING=RLE");

      // insert of data time range :0-100 into fans
      for (int time = 0; time < 100; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // insert large amount of data time range : 1370 ~ 2400
      for (int time = 1370; time < 2400; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // insert large amount of data time range : 300 ~ 1360
      for (int time = 300; time < 1360; time++) {
        // System.out.println("===" + time);
        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      statement.addBatch("flush");
      statement.addBatch("merge");

      // unsequential data, memory data
      for (int time = 1000; time < 1100; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // sequential data, memory data
      for (int time = 20000; time < 20100; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }
      statement.executeBatch();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMultiDeviceQueryAndDelete() {
    testSelectAll();
    testSelectAfterDelete();
  }

  private void testSelectAll() {
    String selectSql = "select * from root.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        long before = -1;
        while (resultSet.next()) {
          long cur = Long.parseLong(resultSet.getString(TestConstant.TIMESTAMP_STR));
          if (cur <= before) {
            fail("time order wrong!");
          }
          before = cur;
          cnt++;
        }
        assertEquals(2290, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void testSelectAfterDelete() {
    String selectSql = "select * from root.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.fans.** WHERE time <= 100");
      statement.execute("DELETE FROM root.car.** WHERE time <= 100");
      statement.execute("DELETE FROM root.fans.** WHERE time >= 20050 and time < 20100");
      statement.execute("DELETE FROM root.car.** WHERE time >= 20050 and time < 20100");

      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        long before = -1;
        while (resultSet.next()) {
          long cur = Long.parseLong(resultSet.getString(TestConstant.TIMESTAMP_STR));
          if (cur <= before) {
            fail("time order wrong!");
          }
          before = cur;
          cnt++;
        }
        assertEquals(2140, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
