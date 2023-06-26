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
package org.apache.iotdb.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.BuiltinAggregationFunctionEnum;
import org.apache.iotdb.itbase.constant.BuiltinScalarFunctionEnum;
import org.apache.iotdb.itbase.constant.BuiltinTimeSeriesGeneratingFunctionEnum;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDFManagementIT {

  private static final int NATIVE_FUNCTIONS_COUNT =
      BuiltinAggregationFunctionEnum.getNativeFunctionNames().size();
  private static final int BUILTIN_FUNCTIONS_COUNT =
      BuiltinTimeSeriesGeneratingFunctionEnum.values().length;

  private static final int BUILTIN_SCALAR_FUNCTIONS_COUNT =
      BuiltinScalarFunctionEnum.values().length;

  private static final String FUNCTION_TYPE_NATIVE = "native";
  private static final String FUNCTION_TYPE_BUILTIN_UDTF = "built-in UDTF";
  private static final String FUNCTION_TYPE_EXTERNAL_UDTF = "external UDTF";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateReflectShowDrop() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.executeQuery("select udf(*, *) from root.vehicle");

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_NATIVE)) {
            continue;
          }
          ++count;
        }
        Assert.assertEquals(1 + BUILTIN_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT, count);
        statement.execute("drop function udf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateAndDropSeveralTimes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.executeQuery("select udf(*, *) from root.vehicle");

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(
            1 + NATIVE_FUNCTIONS_COUNT + BUILTIN_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT,
            count);
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        statement.execute("drop function udf");

        statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
        statement.executeQuery("select udf(*, *) from root.vehicle");
      }

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(
            1 + NATIVE_FUNCTIONS_COUNT + BUILTIN_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT,
            count);
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        statement.execute("drop function udf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testReflectBeforeCreate() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select udf(*, *) from root.vehicle");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void testReflectAfterDrop() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("drop function udf");
      statement.executeQuery("select udf(*, *) from root.vehicle");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void testCreateFunctionWithBuiltinFunctionName1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function aVg as 'org.apache.iotdb.db.query.udf.example.Adder'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the given function name conflicts with the built-in function name"));
    }
  }

  @Test
  public void testCreateFunctionWithBuiltinFunctionName2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function MAX_VALUE as 'org.apache.iotdb.db.query.udf.example.Adder'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the given function name conflicts with the built-in function name"));
    }
  }

  @Test
  public void testCreateFunction1() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      try {
        statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("Failed to create"));
      }
    }
  }

  @Test
  public void testCreateFunction3() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      try {
        statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("the same name UDF has been created"));
      }
    }
  }

  @Test
  public void testCreateFunctionWithInvalidURI() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "create stateless trigger %s before insert on root.test.stateless.* as '%s' using URI '%s' with (\"name\"=\"%s\")",
                "a", "org.apache.iotdb.test", "", "test"));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }

      try {
        statement.execute(
            String.format(
                "create stateless trigger %s before insert on root.test.stateless.* as '%s' using URI '%s' with (\"name\"=\"%s\")",
                "a", "org.apache.iotdb.test", "file:///data/udf/upload-test.jar", "test"));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }
    } catch (SQLException throwable) {
      fail();
    }
  }

  @Test
  public void testDropFunction1() throws SQLException { // create + drop twice
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("drop function udf");

      try {
        // drop UDF that does not exist will not throw exception now.
        statement.execute("drop function udf");
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("this UDF has not been created"));
      }
    }
  }

  @Test
  public void testDropFunction2() { // drop
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // drop UDF that does not exist will not throw exception now.
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("this UDF has not been created"));
    }
  }

  @Test
  public void testDropBuiltInFunction() throws SQLException { // drop
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("drop function abs");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable.getMessage().contains("Built-in function ABS can not be deregistered"));
      }
      // ensure that abs is not dropped
      statement.execute("INSERT INTO root.vehicle.d1(time, s1) VALUES(1, -10.0)");
      try (ResultSet rs = statement.executeQuery("SELECT ABS(s1) FROM root.vehicle.d1")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(10.0F, rs.getFloat(2), 0.00001);
        Assert.assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testDropBuiltinFunction1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("drop function sin");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Built-in function SIN can not be deregistered"));
    }
  }

  @Test
  public void testCreateBuiltinFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function sin as 'org.apache.iotdb.db.query.udf.example.Adder'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("the same name UDF has been created"));
    }
  }

  @Test
  public void testReflectBuiltinFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function adder as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.executeQuery("select adder(*, *) from root.vehicle");
      statement.execute("drop function adder");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testShowBuiltinFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_NATIVE)) {
            continue;
          }

          if (result.contains(FUNCTION_TYPE_EXTERNAL_UDTF)) {
            Assert.assertEquals(
                String.format(
                    "UDF,%s,org.apache.iotdb.db.query.udf.example.Adder,",
                    FUNCTION_TYPE_EXTERNAL_UDTF),
                result);
            ++count;
          } else if (result.contains(FUNCTION_TYPE_BUILTIN_UDTF)) {
            ++count;
          }
        }
        Assert.assertEquals(1 + BUILTIN_FUNCTIONS_COUNT, count);
        statement.execute("drop function udf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
