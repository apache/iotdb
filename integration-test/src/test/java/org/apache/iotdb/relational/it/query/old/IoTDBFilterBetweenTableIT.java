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
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFilterBetweenTableIT {
  protected static final int ITERATION_TIMES = 10;
  private static final String DATABASE_NAME = "test";
  private static final List<String> SQLs = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    generateData(SQLs);
    prepareTableData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void generateData(List<String> SQLs) {
    SQLs.add("CREATE DATABASE " + DATABASE_NAME);
    SQLs.add("USE " + DATABASE_NAME);
    SQLs.add(
        "CREATE TABLE table1 (device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 TEXT MEASUREMENT)");
    for (int i = 1; i <= ITERATION_TIMES; ++i) {
      SQLs.add(
          String.format(
              "insert into table1(device,time,s1,s2,s3) values('d1',%d,%d,%d,%s)", i, i, i, i));
    }
  }

  @Test
  public void testBetweenExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      int start = 1, end = 5;
      String query = "SELECT * FROM table1 WHERE s1 BETWEEN " + start + " AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query =
          "SELECT * FROM table1 WHERE s1 NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query = "SELECT * FROM table1 WHERE time BETWEEN " + start + " AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query =
          "SELECT * FROM table1 WHERE time NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query = "SELECT * FROM table1 WHERE " + start + " BETWEEN time AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(defaultFormatDataTime(1), rs.getString("time"));
        Assert.assertEquals("1", rs.getString("s1"));
        Assert.assertEquals("1", rs.getString("s2"));
        Assert.assertEquals("1", rs.getString("s3"));
      }

      query = "SELECT * FROM table1 WHERE " + start + " NOT BETWEEN time AND " + end;
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start + 1; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query = "SELECT * FROM table1 WHERE " + start + " BETWEEN " + end + " AND time";
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertFalse(rs.next());
      }

      query = "SELECT * FROM table1 WHERE " + start + " NOT BETWEEN " + end + " AND time";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(defaultFormatDataTime(i), rs.getString("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      String[] expectedHeader = new String[] {"_col0", "_col1"};
      String[] retArray = new String[] {"true,true,", "true,false,", "false,false,"};
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from table1 where time between 1 and 3",
          expectedHeader,
          retArray,
          DATABASE_NAME);

      expectedHeader = new String[] {"r1", "r2"};
      retArray = new String[] {"true,true,", "true,false,", "false,false,"};
      tableResultSetEqualTest(
          "select s1 between 1 and 2 as r1, time between 0 and 1 as r2 from table1 where time between 1 and 3",
          expectedHeader,
          retArray,
          DATABASE_NAME);

      expectedHeader = new String[] {"_col0", "_col1"};
      retArray = new String[] {"true,true,", "true,false,", "false,false,"};
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from table1 where time between 1 and 3 order by device",
          expectedHeader,
          retArray,
          DATABASE_NAME);
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
