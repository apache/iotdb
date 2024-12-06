/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFilterBetweenIT {
  protected static final int ITERATION_TIMES = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%s)", i, i, i, i));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testBetweenExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int start = 1, end = 5;
      String query = "SELECT * FROM root.vehicle.d1 WHERE s1 BETWEEN " + start + " AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      query =
          "SELECT * FROM root.vehicle.d1 WHERE s1 NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      query = "SELECT * FROM root.vehicle.d1 WHERE time BETWEEN " + start + " AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      query =
          "SELECT * FROM root.vehicle.d1 WHERE time NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      query = "SELECT * FROM root.vehicle.d1 WHERE " + start + " BETWEEN time AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals("1", rs.getString(ColumnHeaderConstant.TIME));
        Assert.assertEquals("1", rs.getString("root.vehicle.d1.s1"));
        Assert.assertEquals("1", rs.getString("root.vehicle.d1.s2"));
        Assert.assertEquals("1", rs.getString("root.vehicle.d1.s3"));
      }

      query = "SELECT * FROM root.vehicle.d1 WHERE " + start + " NOT BETWEEN time AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start + 1; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      query = "SELECT * FROM root.vehicle.d1 WHERE " + start + " BETWEEN " + end + " AND time";
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertFalse(rs.next());
      }

      query = "SELECT * FROM root.vehicle.d1 WHERE " + start + " NOT BETWEEN " + end + " AND time";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(String.valueOf(i), rs.getString(ColumnHeaderConstant.TIME));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("root.vehicle.d1.s3"));
        }
      }

      String[] expectedHeader =
          new String[] {
            TIMESTAMP_STR, "root.vehicle.d1.s1 BETWEEN 1 AND 2", "Time BETWEEN 0 AND 1"
          };
      String[] retArray = new String[] {"1,true,true,", "2,true,false,", "3,false,false,"};
      resultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from root.vehicle.d1 where time between 1 and 3",
          expectedHeader,
          retArray);

      expectedHeader = new String[] {TIMESTAMP_STR, "r1", "r2"};
      retArray = new String[] {"1,true,true,", "2,true,false,", "3,false,false,"};
      resultSetEqualTest(
          "select s1 between 1 and 2 as r1, time between 0 and 1 as r2 from root.vehicle.d1 where time between 1 and 3",
          expectedHeader,
          retArray);

      expectedHeader =
          new String[] {TIMESTAMP_STR, "Device", "s1 BETWEEN 1 AND 2", "Time BETWEEN 0 AND 1"};
      retArray =
          new String[] {
            "1,root.vehicle.d1,true,true,",
            "2,root.vehicle.d1,true,false,",
            "3,root.vehicle.d1,false,false,"
          };
      resultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from root.vehicle.* where time between 1 and 3 align by device",
          expectedHeader,
          retArray);
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
