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

import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBSyntaxConventionStringLiteralIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  /** Legal cases of using StringLiteral with single quote in insert and select clause. */
  @Test
  public void testStringLiteralWithSingleQuote() {
    String[] insertData = {
      "'string'",
      "'`string`'",
      "'``string'",
      "'\"string\"'",
      "'\"\"string'",
      "'\\\"string\\\"'",
      "'''string'",
      "'\\'string'",
      "'\\nstring'",
      "'\\rstring'",
      "'@#$%^&*()string'",
    };

    String[] resultData = {
      "string",
      "`string`",
      "``string",
      "\"string\"",
      "\"\"string",
      "\"string\"",
      "'string",
      "'string",
      "\\nstring",
      "\\rstring",
      "@#$%^&*()string",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      for (int i = 0; i < insertData.length; i++) {
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, s1) values (%d, %s)", i, insertData[i]);
        System.out.println("INSERT STATEMENT: " + insertSql);
        statement.execute(insertSql);
      }

      boolean hasResult = statement.execute("SELECT s1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      int cnt = 0;
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertEquals(resultData[cnt], resultSet.getString("root.sg1.d1.s1"));
        cnt++;
      }
      Assert.assertEquals(insertData.length, cnt);

      for (int i = 0; i < insertData.length; i++) {
        String querySql = String.format("SELECT s1 FROM root.sg1.d1 WHERE s1 = %s", insertData[i]);
        System.out.println("QUERY STATEMENT: " + querySql);
        hasResult = statement.execute(querySql);
        Assert.assertTrue(hasResult);

        resultSet = statement.getResultSet();
        Assert.assertTrue(resultSet.next());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  /** Legal cases of using StringLiteral with double quote in insert and select clause. */
  @Test
  public void testStringLiteralWithDoubleQuote() {
    String[] insertData = {
      "\"string\"",
      "\"`string`\"",
      "\"``string\"",
      "\"'string'\"",
      "\"''string\"",
      "\"\\'string\\'\"",
      "\"\"\"string\"",
      "\"\\\"string\"",
      "\"\\nstring\"",
      "\"\\rstring\"",
      "\"@#$%^&*()string\"",
    };

    String[] resultData = {
      "string",
      "`string`",
      "``string",
      "'string'",
      "''string",
      "'string'",
      "\"string",
      "\"string",
      "\\nstring",
      "\\rstring",
      "@#$%^&*()string",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      for (int i = 0; i < insertData.length; i++) {
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, s1) values (%d, %s)", i, insertData[i]);
        System.out.println("INSERT STATEMENT: " + insertSql);
        statement.execute(insertSql);
      }

      boolean hasResult = statement.execute("SELECT s1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      int cnt = 0;
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertEquals(resultData[cnt], resultSet.getString("root.sg1.d1.s1"));
        cnt++;
      }
      Assert.assertEquals(insertData.length, cnt);

      for (int i = 0; i < insertData.length; i++) {
        String querySql = String.format("SELECT s1 FROM root.sg1.d1 WHERE s1 = %s", insertData[i]);
        System.out.println("QUERY STATEMENT: " + querySql);
        hasResult = statement.execute(querySql);
        Assert.assertTrue(hasResult);

        resultSet = statement.getResultSet();
        Assert.assertTrue(resultSet.next());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testStringLiteralIllegalCase() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:45 no viable alternative at input '(1, string'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      // without ' or "
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, string)");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testStringLiteralIllegalCase1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:45 no viable alternative at input '(1, `string`'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      // wrap STRING_LITERAL with ``
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, `string`)");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testStringLiteralIllegalCase2() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:53 token recognition error at: '')'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      // single ' in ''
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, ''string')");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testStringLiteralIllegalCase3() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:53 token recognition error at: '\")'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      // single " in ""
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, \"\"string\")");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  /**
   * LOAD/REMOVE/SETTLE use STRING_LITERAL to represent file path legal cases are in
   * IoTDBLoadExternalTsfileIT
   */
  @Test
  public void testFilePath() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD 'path'");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testFilePath1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:5 no viable alternative at input 'LOAD path'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testFilePath2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("REMOVE 'path'");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testFilePath3() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:7 mismatched input 'path' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("REMOVE path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testFilePath4() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SETTLE 'path'");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testFilePath5() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:7 mismatched input 'path' expecting {ROOT, STRING_LITERAL}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SETTLE path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testUserPassword() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:17 mismatched input 'test' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER test 'test'");
      // password should be STRING_LITERAL
      statement.execute("CREATE USER test test");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testUserPassword1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:17 mismatched input '`test`' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER test \"test\"");
      // password should be STRING_LITERAL
      statement.execute("CREATE USER test `test`");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testTriggerClassName() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:64 mismatched input 'org' expecting {AS, '.'}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());

      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 FLOAT");
      // create trigger, trigger class name should be STRING_LITERAL
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s3 "
              + "as org.apache.iotdb.db.engine.trigger.example.Accumulator");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testTriggerClassName1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:64 mismatched input '`org.apache.iotdb.db.engine.trigger.example.Accumulator`' "
            + "expecting {AS, '.'}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
              + "as `org.apache.iotdb.db.engine.trigger.example.Accumulator`");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testUDFClassName() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:23 mismatched input 'org' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // udf class name should be STRING_LITERAL
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      // executed correctly
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
      Assert.assertEquals(1 + BuiltinTimeSeriesGeneratingFunction.values().length, count);
      resultSet.close();
      statement.execute("drop function udf");

      // without '' or ""
      statement.execute("create function udf as org.apache.iotdb.db.query.udf.example.Adder");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testUDFClassName1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:23 mismatched input '`org.apache.iotdb.db.query.udf.example.Adder`' "
            + "expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // udf class name should be STRING_LITERAL
      statement.execute("create function udf as `org.apache.iotdb.db.query.udf.example.Adder`");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  // attribute can be constant | identifier
  @Test
  public void testTriggerAttribute() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s4 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s5 FLOAT");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s6 FLOAT");
      // trigger attribute should be STRING_LITERAL
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k1'='v1')");

      statement.execute(
          "create trigger trigger_2 before insert on root.vehicle.d1.s2 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with (k1='v1')");

      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k1'=v1)");

      statement.execute(
          "create trigger trigger_4 before insert on root.vehicle.d1.s4 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with (k1=v1)");

      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with (`k1`=`v1`)");

      statement.execute(
          "create trigger trigger_6 before insert on root.vehicle.d1.s6 "
              + "as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with (`k1`=v1)");

      boolean hasResult = statement.execute("show triggers");
      assertTrue(hasResult);
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUDFAttribute() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
      statement.execute("INSERT INTO root.vehicle.d1(time,s1) values (1,2.0),(2,3.0)");

      ResultSet resultSet =
          statement.executeQuery("select bottom_k(s1,'k' = '1') from root.vehicle.d1");
      assertTrue(resultSet.next());
      Assert.assertEquals("2.0", resultSet.getString(2));

      //      resultSet = statement.executeQuery("select bottom_k(s1,k = 1) from root.vehicle.d1");
      //      assertTrue(resultSet.next());
      //      Assert.assertEquals("2.0", resultSet.getString(2));
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUDFAttribute1() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:21 extraneous input 'k' expecting {',', ')'}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // UDF attribute should be STRING_LITERAL
      statement.executeQuery("select bottom_k(s1,``k` = 1) from root.vehicle.d1");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testCreateTimeSeriesAttribute() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // besides datatype,encoding,compression,compressor, attributes in create time series clause
      // should be STRING_LITERAL
      statement.execute(
          "create timeseries root.vehicle.d1.s1 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.vehicle.d1.s2 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY, max_point_number = 5");
      statement.execute(
          "create timeseries root.vehicle.d1.s3 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY, 'max_point_number' = '5'");
      statement.execute(
          "create timeseries root.vehicle.d1.s4 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY, max_point_number = '5'");
      statement.execute(
          "create timeseries root.vehicle.d1.s5 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY, `max_point_number` = 5");
      statement.execute(
          "create timeseries root.vehicle.d1.s6 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY, `max_point_number` = `5`");
      ResultSet resultSet = statement.executeQuery("show timeseries");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(6, cnt);
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateTimeSeriesTags() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.vehicle.d1.s1 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(tag1=v1)");
      statement.execute(
          "create timeseries root.vehicle.d1.s2 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(`tag1`=v1)");
      statement.execute(
          "create timeseries root.vehicle.d1.s3 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags('tag1'=v1)");
      statement.execute(
          "create timeseries root.vehicle.d1.s4 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(\"tag1\"=v1)");
      statement.execute(
          "create timeseries root.vehicle.d1.s5 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(tag1=`v1`)");
      statement.execute(
          "create timeseries root.vehicle.d1.s6 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(tag1='v1')");
      statement.execute(
          "create timeseries root.vehicle.d1.s7 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(tag1=\"v1\")");
      statement.execute(
          "create timeseries root.vehicle.d1.s8 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "tags(tag1=v1)");
      boolean hasResult = statement.execute("show timeseries");
      Assert.assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(8, cnt);
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateTimeSeriesAttributeClause() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.vehicle.d1.s1 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "attributes('attr1'='v1', 'attr2'='v2')");
      statement.execute(
          "create timeseries root.vehicle.d1.s2 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "attributes(attr1=v1, attr2=v2)");
      statement.execute(
          "create timeseries root.vehicle.d1.s3 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "attributes(`attr1`=`v1`, `attr2`=v2)");
      statement.execute(
          "create timeseries root.vehicle.d1.s4 "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY "
              + "attributes('attr1'=v1, attr2=v2)");
      ResultSet resultSet = statement.executeQuery("show timeseries");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(4, cnt);
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testPipeSinkAttribute() {
    String errorMsg =
        "401: Error occurred while parsing SQL to physical plan: "
            + "line 1:40 token recognition error at: '` = '127.0.0.1')'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE PIPESINK `test.*1` AS IoTDB (``ip` = '127.0.0.1')");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  // alias can be identifier or STRING_LITERAL
  @Test
  public void testAliasInResultColumn() {
    String[] alias = {
      "b", "test", "`test.1`", "`1``1`", "'test'", "\"test\"", "\"\\\\test\"",
    };

    String[] res = {
      "b", "test", "test.1", "1`1", "test", "test", "\\\\test",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.sg.a "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY ");
      statement.execute("insert into root.sg(time, a) values (1,1)");

      String selectSql = "select a as %s from root.sg";
      for (int i = 0; i < alias.length; i++) {
        boolean hasResult = statement.execute(String.format(selectSql, alias[i]));
        assertTrue(hasResult);
        ResultSet resultSet = statement.getResultSet();
        Assert.assertEquals(res[i], resultSet.getMetaData().getColumnName(2));
      }

      try {
        statement.execute("select a as test.b from root.sg");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testAliasInAlterClause() {
    String[] alias = {
      "b", "test", "`test.1`", "`1``1`", "'test'", "\"test\"", "\"\\\\test\"",
    };

    String[] res = {
      "b", "test", "test.1", "1`1", "test", "test", "\\\\test",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.sg.a "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY ");

      String alterSql = "ALTER timeseries root.sg.a UPSERT alias = %s";
      for (int i = 0; i < alias.length; i++) {
        statement.execute(String.format(alterSql, alias[i]));
        statement.execute("show timeseries");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        Assert.assertEquals(res[i], resultSet.getString("alias"));
      }

      try {
        statement.execute("ALTER timeseries root.sg.a UPSERT alias = test.a");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
