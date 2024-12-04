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

package org.apache.iotdb.relational.it.db.it.udf.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBScalarFunctionIT {
  private static String[] sqls =
      new String[] {
        "CREATE DATABASE test",
        "USE test",
        "CREATE TABLE vehicle (device_id string id, s1 INT32 measurement, s2 INT64 measurement, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT)",
        "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (1, 'd0', 1, 1, 1.1, 1.1, true)",
        "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (2, 'd0', null, 2, 2.2, 2.2, true)",
        "insert into vehicle(time, device_id, s1, s2, s3, s4, s5) values (3, 'd0', 3, 3, null, null, false)",
        "insert into vehicle(time, device_id, s5) values (5, 'd0', true)",
        "CREATE FUNCTION contain_null as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'",
        "CREATE FUNCTION all_sum as 'org.apache.iotdb.db.query.udf.example.relational.AllSum'",
        "CREATE TABLE t2 (device_id string id, s1 DATE measurement)",
        "insert into t2(time, device_id, s1) values (1, 'd0', '2024-02-28')",
        "insert into t2(time, device_id, s1) values (2, 'd0', '2024-02-29')",
        "insert into t2(time, device_id, s1) values (3, 'd0', '2024-03-01')",
        "CREATE FUNCTION date_plus as 'org.apache.iotdb.db.query.udf.example.relational.DatePlusOne'"
      };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        System.out.println(sql);
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @Test
  public void testIllegalInput() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      try {
        statement.execute("select contain_null() from vehicle");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("At least one parameter is required"));
      }
      try {
        statement.execute("select all_sum(s1,s2,s3,s4,s5) from vehicle");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("Only support inputs of INT32,INT64,DOUBLE,FLOAT type"));
      }

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testNormalQuery() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      List<String> expectedResult =
          Arrays.asList("1,false,false", "2,true,true", "3,true,false", "5,true,true");
      int row = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, contain_null(s1,s2,s3,s4,s5) as contain_null, contain_null(s1) as s1_null from vehicle")) {
        while (resultSet.next()) {
          Assert.assertEquals(
              expectedResult.get(row),
              resultSet.getLong(1) + "," + resultSet.getBoolean(2) + "," + resultSet.getBoolean(3));
          row++;
        }
        assertEquals(4, row);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testPolymorphicQuery() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      List<String> expectedResult =
          Arrays.asList(
              "1,1,1,1.1,1.1,2,3.1,4.2,2.1",
              "2,0,2,2.2,2.2,2,4.2,6.4,4.2",
              "3,3,3,.0,.0,6,6.0,6.0,3.0",
              "5,0,0,.0,.0,0,.0,.0,.0");
      int row = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, all_sum(s1) as s1, all_sum(s2) as s2,  all_sum(s3) as s3,  all_sum(s4) as s4, all_sum(s1,s2) as s12,  all_sum(s1,s2,s3) as s123, all_sum(s1,s2,s3,s4) as s1234, all_sum(s2,s3) as s23 from vehicle")) {
        Assert.assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(1));
        Assert.assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(2));
        Assert.assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(3));
        Assert.assertEquals(Types.FLOAT, resultSet.getMetaData().getColumnType(4));
        Assert.assertEquals(Types.DOUBLE, resultSet.getMetaData().getColumnType(5));
        Assert.assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(6));
        Assert.assertEquals(Types.FLOAT, resultSet.getMetaData().getColumnType(7));
        Assert.assertEquals(Types.DOUBLE, resultSet.getMetaData().getColumnType(8));
        Assert.assertEquals(Types.FLOAT, resultSet.getMetaData().getColumnType(9));
        DecimalFormat df = new DecimalFormat("#.0");
        while (resultSet.next()) {
          Assert.assertEquals(
              expectedResult.get(row),
              resultSet.getLong(1)
                  + ","
                  + resultSet.getInt(2)
                  + ","
                  + resultSet.getLong(3)
                  + ","
                  + df.format(resultSet.getFloat(4))
                  + ","
                  + df.format(resultSet.getDouble(5))
                  + ","
                  + resultSet.getLong(6)
                  + ","
                  + df.format(resultSet.getFloat(7))
                  + ","
                  + df.format(resultSet.getDouble(8))
                  + ","
                  + df.format(resultSet.getFloat(9)));
          row++;
        }
        assertEquals(4, row);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDateFunction() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      List<LocalDate> expectedResult =
          Arrays.asList(
              LocalDate.of(2024, 2, 29), LocalDate.of(2024, 3, 1), LocalDate.of(2024, 3, 2));
      int row = 0;
      try (ResultSet resultSet = statement.executeQuery("select date_plus(s1, 1) from t2")) {
        while (resultSet.next()) {
          Assert.assertEquals(expectedResult.get(row), resultSet.getDate(1).toLocalDate());
          row++;
        }
        assertEquals(3, row);
      }
      expectedResult =
          Arrays.asList(
              LocalDate.of(2024, 3, 1), LocalDate.of(2024, 3, 2), LocalDate.of(2024, 3, 3));
      row = 0;
      try (ResultSet resultSet = statement.executeQuery("select date_plus(s1, 2) from t2")) {
        while (resultSet.next()) {
          Assert.assertEquals(expectedResult.get(row), resultSet.getDate(1).toLocalDate());
          row++;
        }
        assertEquals(3, row);
      }

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
