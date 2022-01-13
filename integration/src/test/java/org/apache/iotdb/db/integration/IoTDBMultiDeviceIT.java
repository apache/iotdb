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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBMultiDeviceIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;
  private static long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(1000);
    tsFileConfig.setPageSizeInByte(1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1000);
    ConfigFactory.getConfig().setMemtableSizeThreshold(1024 * 1000);
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(100);
    ConfigFactory.getConfig().setCompressor("LZ4");

    EnvFactory.getEnv().initBeforeTest();

    insertData();
  }

  @After
  public void tearDown() throws Exception {
    // recovery value
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    ConfigFactory.getConfig().setPageSizeInByte(pageSizeInByte);
    ConfigFactory.getConfig().setGroupSizeInByte(groupSizeInByte);

    EnvFactory.getEnv().cleanAfterTest();

    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
    ConfigFactory.getConfig().setMemtableSizeThreshold(groupSizeInByte);
    ConfigFactory.getConfig().setCompressor("SNAPPY");
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.createSql) {
        statement.execute(sql);
      }

      statement.execute("SET STORAGE GROUP TO root.fans");
      statement.execute("CREATE TIMESERIES root.fans.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d2.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d3.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.car.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.car.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.car.d2.s1 WITH DATATYPE=INT64, ENCODING=RLE");

      // insert of data time range :0-1000 into fans
      for (int time = 0; time < 1000; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
      }
      statement.executeBatch();

      // insert large amount of data time range : 13700 ~ 24000
      for (int time = 13700; time < 24000; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
      }
      statement.executeBatch();

      // insert large amount of data time range : 3000 ~ 13600
      for (int time = 3000; time < 13600; time++) {
        // System.out.println("===" + time);
        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
      }
      statement.executeBatch();

      statement.execute("flush");
      statement.execute("merge");

      // unsequential data, memory data
      for (int time = 10000; time < 11000; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
      }
      statement.executeBatch();

      // sequential data, memory data
      for (int time = 200000; time < 201000; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d2(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql =
            String.format("insert into root.fans.d3(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d1(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
        sql = String.format("insert into root.car.d2(timestamp,s0) values(%s,%s)", time, time % 40);
        statement.addBatch(sql);
      }
      statement.executeBatch();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllTest() {
    String selectSql = "select * from root.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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
        assertEquals(22900, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAfterDeleteTest() {
    String selectSql = "select * from root.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.fans.** WHERE time <= 1000");
      statement.execute("DELETE FROM root.car.** WHERE time <= 1000");
      statement.execute("DELETE FROM root.fans.** WHERE time >= 200500 and time < 201000");
      statement.execute("DELETE FROM root.car.** WHERE time >= 200500 and time < 201000");

      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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
        assertEquals(21400, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
