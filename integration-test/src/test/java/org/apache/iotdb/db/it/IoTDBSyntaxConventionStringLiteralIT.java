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
import org.apache.iotdb.itbase.constant.BuiltinScalarFunctionEnum;
import org.apache.iotdb.itbase.constant.BuiltinTimeSeriesGeneratingFunctionEnum;
import org.apache.iotdb.itbase.constant.TestConstant;
import org.apache.iotdb.rpc.TSStatusCode;

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
public class IoTDBSyntaxConventionStringLiteralIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
      "'''string'",
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
        statement.execute(insertSql);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          Assert.assertEquals(resultData[cnt], resultSet.getString("root.sg1.d1.s1"));
          cnt++;
        }
        Assert.assertEquals(insertData.length, cnt);
      }

      for (String insertDatum : insertData) {
        String querySql = String.format("SELECT s1 FROM root.sg1.d1 WHERE s1 = %s", insertDatum);
        try (ResultSet resultSet = statement.executeQuery(querySql)) {
          Assert.assertTrue(resultSet.next());
        }
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
      "\"\"\"string\"",
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
        statement.execute(insertSql);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          Assert.assertEquals(resultData[cnt], resultSet.getString("root.sg1.d1.s1"));
          cnt++;
        }
        Assert.assertEquals(insertData.length, cnt);
      }

      for (String insertDatum : insertData) {
        String querySql = String.format("SELECT s1 FROM root.sg1.d1 WHERE s1 = %s", insertDatum);
        try (ResultSet resultSet = statement.executeQuery(querySql)) {
          Assert.assertTrue(resultSet.next());
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testStringLiteralIllegalCase() {
    String errorMsg =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:45 mismatched input 'string' expecting {FALSE, NAN, NOW, NULL, TRUE, '-', '+', '/', '.', STRING_LITERAL, BINARY_LITERAL, DATETIME_LITERAL, INTEGER_LITERAL, EXPONENT_NUM_PART}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
    } catch (SQLException e) {
      fail(e.getMessage());
    }
    // without ' or "
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, string)");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }

    String errorMsg1 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:45 mismatched input '`string`' expecting {FALSE, NAN, NOW, NULL, TRUE, '-', '+', '/', '.', STRING_LITERAL, BINARY_LITERAL, DATETIME_LITERAL, INTEGER_LITERAL, EXPONENT_NUM_PART}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // wrap STRING_LITERAL with ``
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, `string`)");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg1, e.getMessage());
    }

    String errorMsg2 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:47 extraneous input 'string' expecting {',', ')'}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // single ' in ''
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, ''string')");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg2, e.getMessage());
    }

    String errorMsg3 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:47 extraneous input 'string' expecting {',', ')'}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // single " in ""
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, \"\"string\")");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg3, e.getMessage());
    }
  }

  /**
   * LOAD/REMOVE/SETTLE use STRING_LITERAL to represent file path legal cases are in
   * IoTDBLoadExternalTsfileIT
   */
  @Test
  public void testIllegalFilePath() {
    String errorMsg =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:5 no viable alternative at input 'LOAD path'";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }

    String errorMsg1 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:7 mismatched input 'path' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("REMOVE path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg1, e.getMessage());
    }

    String errorMsg2 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:7 mismatched input 'path' expecting {ROOT, STRING_LITERAL}";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SETTLE path");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg2, e.getMessage());
    }
  }

  @Test
  public void testUserPassword() {
    String errorMsg =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:18 mismatched input 'test' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER test1 'test'");
      // password should be STRING_LITERAL
      statement.execute("CREATE USER test1 test");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }

    String errorMsg1 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:17 mismatched input '`test`' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER test \"test\"");
      // password should be STRING_LITERAL
      statement.execute("CREATE USER test `test`");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg1, e.getMessage());
    }
  }

  @Test
  public void testUDFClassName() {
    String errorMsg =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:23 mismatched input 'org' expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // udf class name should be STRING_LITERAL
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");

      // executed correctly
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(TestConstant.FUNCTION_TYPE_NATIVE)) {
            continue;
          }
          ++count;
        }
        Assert.assertEquals(
            1
                + BuiltinTimeSeriesGeneratingFunctionEnum.values().length
                + BuiltinScalarFunctionEnum.values().length,
            count);
      }
      statement.execute("drop function udf");

      // without '' or ""
      statement.execute("create function udf as org.apache.iotdb.db.query.udf.example.Adder");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }

    // Illegal name with back quote
    String errorMsg1 =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:23 mismatched input '`org.apache.iotdb.db.query.udf.example.Adder`' "
            + "expecting STRING_LITERAL";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // udf class name should be STRING_LITERAL
      statement.execute("create function udf as `org.apache.iotdb.db.query.udf.example.Adder`");
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(errorMsg1, e.getMessage());
    }
  }

  @Test
  public void testUDFAttribute() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
      statement.execute("INSERT INTO root.vehicle.d1(time,s1) values (1,2.0),(2,3.0)");

      try (ResultSet resultSet =
          statement.executeQuery("select bottom_k(s1,'k' = '1') from root.vehicle.d1")) {
        assertTrue(resultSet.next());
        Assert.assertEquals("2.0", resultSet.getString(2));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }

    // Illegal attribute
    String errorMsg =
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": Error occurred while parsing SQL to physical plan: "
            + "line 1:22 token recognition error at: '` = 1) from root.vehicle.d1'";
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
      // could be STRING_LITERAL
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
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(6, cnt);
      }
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
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(8, cnt);
      }
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
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(4, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
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
        try (ResultSet resultSet = statement.executeQuery(String.format(selectSql, alias[i]))) {
          Assert.assertEquals(res[i], resultSet.getMetaData().getColumnName(2));
        }
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
      "b", "test", "`test.1`", "`1``1`", "'test'", "\"test\"", "`\\\\test`",
    };

    String[] res = {
      "b", "test", "`test.1`", "`1``1`", "test", "test", "`\\\\test`",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.sg.a "
              + "with datatype=INT64, encoding=PLAIN, compression=SNAPPY ");

      String alterSql = "ALTER timeseries root.sg.a UPSERT alias = %s";
      for (int i = 0; i < alias.length; i++) {
        statement.execute(String.format(alterSql, alias[i]));
        try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
          resultSet.next();
          Assert.assertEquals(res[i], resultSet.getString(ColumnHeaderConstant.ALIAS));
        }
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

  // TODO: add this back when trigger is supported in new cluster

  //  @Test
  //  public void testTriggerClassName() {
  //    String errorMsg =
  //        TSStatusCode.SQL_PARSE_ERROR.getStatusCode() + ": Error occurred while parsing SQL to
  // physical plan: "
  //            + "line 1:64 mismatched input 'org' expecting {AS, '.'}";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      // show
  //      ResultSet resultSet = statement.executeQuery("show triggers");
  //      assertFalse(resultSet.next());
  //
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 FLOAT");
  //      // create trigger, trigger class name should be STRING_LITERAL
  //      statement.execute(
  //          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //      statement.execute(
  //          "create trigger trigger_2 after insert on root.vehicle.d1.s2 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Counter'");
  //
  //      // show
  //      resultSet = statement.executeQuery("show triggers");
  //      assertTrue(resultSet.next());
  //      assertTrue(resultSet.next());
  //      assertFalse(resultSet.next());
  //
  //      // show
  //      resultSet = statement.executeQuery("show triggers");
  //      assertTrue(resultSet.next());
  //      assertTrue(resultSet.next());
  //      assertFalse(resultSet.next());
  //
  //      statement.execute(
  //          "create trigger trigger_1 before insert on root.vehicle.d1.s3 "
  //              + "as org.apache.iotdb.db.storageengine.trigger.example.Accumulator");
  //      fail();
  //    } catch (SQLException e) {
  //      Assert.assertEquals(errorMsg, e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void testTriggerClassName1() {
  //    String errorMsg =
  //        TSStatusCode.SQL_PARSE_ERROR.getStatusCode() + ": Error occurred while parsing SQL to
  // physical plan: "
  //            + "line 1:64 mismatched input
  // '`org.apache.iotdb.db.storageengine.trigger.example.Accumulator`' "
  //            + "expecting {AS, '.'}";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
  //      statement.execute(
  //          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
  //              + "as `org.apache.iotdb.db.storageengine.trigger.example.Accumulator`");
  //      fail();
  //    } catch (SQLException e) {
  //      Assert.assertEquals(errorMsg, e.getMessage());
  //    }
  //  }
  //
  //  // attribute can be constant | identifier
  //  @Test
  //  public void testTriggerAttribute() {
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s4 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s5 FLOAT");
  //      statement.execute("CREATE TIMESERIES root.vehicle.d1.s6 FLOAT");
  //      // trigger attribute should be STRING_LITERAL
  //      statement.execute(
  //          "create trigger trigger_1 before insert on root.vehicle.d1.s1 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // ('k1'='v1')");
  //
  //      statement.execute(
  //          "create trigger trigger_2 before insert on root.vehicle.d1.s2 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // (k1='v1')");
  //
  //      statement.execute(
  //          "create trigger trigger_3 before insert on root.vehicle.d1.s3 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // ('k1'=v1)");
  //
  //      statement.execute(
  //          "create trigger trigger_4 before insert on root.vehicle.d1.s4 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // (k1=v1)");
  //
  //      statement.execute(
  //          "create trigger trigger_5 before insert on root.vehicle.d1.s5 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // (`k1`=`v1`)");
  //
  //      statement.execute(
  //          "create trigger trigger_6 before insert on root.vehicle.d1.s6 "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator' with
  // (`k1`=v1)");
  //
  //      boolean hasResult = statement.execute("show triggers");
  //      assertTrue(hasResult);
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail();
  //    }
  //  }

  // todo: add this back when supporting sync in new cluster

  //  @Test
  //  public void testPipeSinkAttribute() {
  //    String errorMsg =
  //        TSStatusCode.SQL_PARSE_ERROR.getStatusCode() + ": Error occurred while parsing SQL to
  // physical plan: "
  //            + "line 1:40 token recognition error at: '` = '127.0.0.1')'";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      statement.execute("CREATE PIPESINK `test.*1` AS IoTDB (``ip` = '127.0.0.1')");
  //      fail();
  //    } catch (SQLException e) {
  //      Assert.assertEquals(errorMsg, e.getMessage());
  //    }
  //  }
}
