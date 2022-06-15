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

import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.it.aligned.AlignedWriteUtil;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDFManagementIT {

  private static final int NATIVE_FUNCTIONS_COUNT =
      BuiltinAggregationFunction.getNativeFunctionNames().size();
  private static final int BUILTIN_FUNCTIONS_COUNT =
      BuiltinTimeSeriesGeneratingFunction.values().length;

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @Before
  public void setUp() throws Exception {
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
  }

  @Test
  public void testCreateReflectShowDrop() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("select udf(*, *) from root.vehicle");

      ResultSet resultSet = statement.executeQuery("show functions");
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
      Assert.assertEquals(1 + BUILTIN_FUNCTIONS_COUNT, count);
      resultSet.close();
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateAndDropSeveralTimes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("select udf(*, *) from root.vehicle");

      ResultSet resultSet = statement.executeQuery("show functions");
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }
      Assert.assertEquals(1 + NATIVE_FUNCTIONS_COUNT + BUILTIN_FUNCTIONS_COUNT, count);
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet.close();
      statement.execute("drop function udf");

      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("select udf(*, *) from root.vehicle");

      resultSet = statement.executeQuery("show functions");
      count = 0;
      while (resultSet.next()) {
        ++count;
      }
      Assert.assertEquals(1 + NATIVE_FUNCTIONS_COUNT + BUILTIN_FUNCTIONS_COUNT, count);
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet.close();
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testReflectBeforeCreate() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("select udf(*, *) from root.vehicle");
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
      statement.execute("select udf(*, *) from root.vehicle");
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
        assertTrue(throwable.getMessage().contains("Failed to register"));
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
        assertTrue(
            throwable
                .getMessage()
                .contains(
                    "with the same function name and the class name has already been registered"));
      }
    }
  }

  @Test
  public void testDropFunction1() throws SQLException { // create + drop twice
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute("drop function udf");

      try {
        statement.execute("drop function udf");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("does not exist"));
      }
    }
  }

  @Test
  public void testDropFunction2() { // drop
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("drop function udf");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("does not exist"));
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
        assertTrue(throwable.getMessage().contains("Built-in function"));
      }
      statement.execute("INSERT INTO root.vehicle.d1(time, s1) VALUES(1, -10.0)");
      ResultSet rs = statement.executeQuery("SELECT ABS(s1) FROM root.vehicle.d1");
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1));
      Assert.assertEquals(10.0F, rs.getFloat(2), 0.00001);
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testCreateBuiltinFunction() throws ClassNotFoundException {
    UDFRegistrationService.getInstance()
        .registerBuiltinFunction("adder", "org.apache.iotdb.db.query.udf.example.Adder");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function adder as 'org.apache.iotdb.db.query.udf.example.Adder'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the given function name is the same as a built-in UDF function name"));
    } finally {
      UDFRegistrationService.getInstance().deregisterBuiltinFunction("adder");
    }
  }

  @Test
  public void testDropBuiltinFunction() throws ClassNotFoundException {
    UDFRegistrationService.getInstance()
        .registerBuiltinFunction("adder", "org.apache.iotdb.db.query.udf.example.Adder");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("drop function adder");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("Built-in function ADDER can not be deregistered"));
    } finally {
      UDFRegistrationService.getInstance().deregisterBuiltinFunction("adder");
    }
  }

  @Test
  public void testReflectBuiltinFunction() throws ClassNotFoundException {
    UDFRegistrationService.getInstance()
        .registerBuiltinFunction("adder", "org.apache.iotdb.db.query.udf.example.Adder");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("select adder(*, *) from root.vehicle");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    } finally {
      UDFRegistrationService.getInstance().deregisterBuiltinFunction("adder");
    }
  }

  @Test
  public void testShowBuiltinFunction() throws ClassNotFoundException {
    UDFRegistrationService.getInstance()
        .registerBuiltinFunction("adder", "org.apache.iotdb.db.query.udf.example.Adder");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      ResultSet resultSet = statement.executeQuery("show functions");
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
      Assert.assertEquals(2 + BUILTIN_FUNCTIONS_COUNT, count);
      resultSet.close();
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    } finally {
      UDFRegistrationService.getInstance().deregisterBuiltinFunction("adder");
    }
  }
}
