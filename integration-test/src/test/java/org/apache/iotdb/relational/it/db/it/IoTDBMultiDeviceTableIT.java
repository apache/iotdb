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
package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBMultiDeviceTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
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

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.addBatch("CREATE DATABASE test");
      statement.addBatch("USE \"test\"");
      statement.addBatch(
          "create table t (id1 string id, id2 string id, s0 int32 measurement, s1 int32 measurement)");

      // insert of data time range :0-100 into fans
      for (int time = 0; time < 100; time++) {

        String sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d2',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d3',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d2',%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // insert large amount of data time range : 1370 ~ 2400
      for (int time = 1370; time < 2400; time++) {

        String sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d2',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d3',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d2',%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // insert large amount of data time range : 300 ~ 1360
      for (int time = 300; time < 1360; time++) {
        // System.out.println("===" + time);
        String sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d2',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d3',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d2',%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      statement.addBatch("flush");

      // unsequential data, memory data
      for (int time = 1000; time < 1100; time++) {

        String sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d2',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d3',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d2',%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }

      // sequential data, memory data
      for (int time = 20000; time < 20100; time++) {

        String sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d2',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1,id2,time,s0) values('fans','d3',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d0',%s,%s)", time, time % 7);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d1',%s,%s)", time, time % 4);
        statement.addBatch(sql);
        sql =
            String.format(
                "insert into t(id1, id2,time,s0) values('car','d2',%s,%s)", time, time % 4);
        statement.addBatch(sql);
      }
      assertTrue(Arrays.stream(statement.executeBatch()).allMatch(i -> i == 200));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMultiDeviceQueryAndDelete() {
    testSelectAll();
    // TODO: add support
    // testSelectAfterDelete();
  }

  private void testSelectAll() {
    String selectSql = "select * from t";

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      Map<String, Long> lastTimeMap = new HashMap<>();
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        while (resultSet.next()) {
          String id = resultSet.getString("id1") + "_" + resultSet.getString("id2");
          long before = lastTimeMap.getOrDefault(id, -1L);
          long cur = resultSet.getTimestamp("time").getTime();
          if (cur <= before) {
            fail("time order wrong!");
          }
          lastTimeMap.put(id, cur);
          cnt++;
        }
        assertEquals(16030, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void testSelectAfterDelete() {
    String selectSql = "select * from t";

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");

      statement.execute("DELETE FROM t WHERE id1='fans' and time <= 100");
      statement.execute("DELETE FROM t WHERE id1='car' and time <= 100");
      statement.execute("DELETE FROM t WHERE id1='fans' and time >= 20050 and time < 20100");
      statement.execute("DELETE FROM t WHERE id1='car' and time >= 20050 and time < 20100");

      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        long before = -1;
        while (resultSet.next()) {
          long cur = Long.parseLong(resultSet.getString("time"));
          if (cur <= before) {
            fail("time order wrong!");
          }
          before = cur;
          cnt++;
        }
        assertEquals(2140, cnt);
      }

      statement.execute("DELETE FROM t WHERE id1 = 'fans' and time <= 20000");
      statement.execute("DELETE FROM t WHERE id1 = 'car' and time <= 20000");

      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        long before = -1;
        while (resultSet.next()) {
          long cur = resultSet.getTimestamp("time").getTime();
          if (cur <= before) {
            fail("time order wrong!");
          }
          before = cur;
          cnt++;
        }
        assertEquals(49, cnt);
      }

      statement.execute("DELETE FROM t WHERE time >= 20000");

      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        int cnt = 0;
        long before = -1;
        while (resultSet.next()) {
          long cur = resultSet.getTimestamp("time").getTime();
          if (cur <= before) {
            fail("time order wrong!");
          }
          before = cur;
          cnt++;
        }
        assertEquals(0, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
