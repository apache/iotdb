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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSyntaxConventionIdentifierIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testKeyWord() {
    String[] createNodeNames = {
      "add",
      "as",
      "select",
      "drop_trigger",
      "REVOKE_USER_ROLE",
      "pipesink",
      "boolean",
      "datatype",
      "device",
    };

    String[] resultTimeseries = {
      "root.sg1.d1.add",
      "root.sg1.d1.as",
      "root.sg1.d1.select",
      "root.sg1.d1.drop_trigger",
      "root.sg1.d1.REVOKE_USER_ROLE",
      "root.sg1.d1.pipesink",
      "root.sg1.d1.boolean",
      "root.sg1.d1.datatype",
      "root.sg1.d1.device",
    };

    String[] selectNodeNames = {
      "add",
      "as",
      "select",
      "drop_trigger",
      "REVOKE_USER_ROLE",
      "pipesink",
      "boolean",
      "datatype",
      "device",
    };

    String[] suffixInResultColumns = {
      "add",
      "as",
      "select",
      "drop_trigger",
      "REVOKE_USER_ROLE",
      "pipesink",
      "boolean",
      "datatype",
      "device",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String createNodeName : createNodeNames) {
        String createSql = String.format("CREATE TIMESERIES root.sg1.d1.%s INT32", createNodeName);
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, %s) VALUES(1, 1)", createNodeName);
        statement.execute(createSql);
        statement.execute(insertSql);
      }

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
        Set<String> expectedResult = new HashSet<>(Arrays.asList(resultTimeseries));

        while (resultSet.next()) {
          Assert.assertTrue(
              expectedResult.contains(resultSet.getString(ColumnHeaderConstant.TIMESERIES)));
          expectedResult.remove(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        Assert.assertEquals(0, expectedResult.size());
      }

      for (int i = 0; i < selectNodeNames.length; i++) {
        String selectSql =
            String.format("SELECT %s FROM root.sg1.d1 WHERE time = 1", selectNodeNames[i]);
        try (ResultSet resultSet = statement.executeQuery(selectSql)) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(1, resultSet.getInt("root.sg1.d1." + suffixInResultColumns[i]));
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNodeName() {
    String[] createNodeNames = {
      "a_1",
      "aaa",
      "in",
      "between",
      "is",
      "`select`",
      "`a.b`",
      "`111`",
      "`a``b`",
      "`a.\"b`",
      "`a.'b`",
      "````",
      "`c.d.```",
      "`abc`",
      "`+12`",
      "`1e3`",
      "`001`",
      "`-1.0`",
      "`01e-3`",
      "`+0001`",
      "`-0001`",
      "`++1`",
      "`+-1`",
      "`--1`",
      "123w",
      "123d",
      "123h"
    };

    String[] resultTimeseries = {
      "root.sg1.d1.a_1",
      "root.sg1.d1.aaa",
      "root.sg1.d1.in",
      "root.sg1.d1.between",
      "root.sg1.d1.is",
      "root.sg1.d1.select",
      "root.sg1.d1.`a.b`",
      "root.sg1.d1.`111`",
      "root.sg1.d1.`a``b`",
      "root.sg1.d1.`a.\"b`",
      "root.sg1.d1.`a.'b`",
      "root.sg1.d1.````",
      "root.sg1.d1.`c.d.```",
      "root.sg1.d1.abc",
      "root.sg1.d1.`+12`",
      "root.sg1.d1.`1e3`",
      "root.sg1.d1.`001`",
      "root.sg1.d1.`-1.0`",
      "root.sg1.d1.`01e-3`",
      "root.sg1.d1.`+0001`",
      "root.sg1.d1.`-0001`",
      "root.sg1.d1.`++1`",
      "root.sg1.d1.`+-1`",
      "root.sg1.d1.`--1`",
      "root.sg1.d1.123w",
      "root.sg1.d1.123d",
      "root.sg1.d1.123h"
    };

    String[] selectNodeNames = {
      "a_1",
      "aaa",
      "in",
      "between",
      "is",
      "`select`",
      "`a.b`",
      "`111`",
      "`a``b`",
      "`a.\"b`",
      "`a.'b`",
      "````",
      "`c.d.```",
      "abc",
      "`+12`",
      "`1e3`",
      "`001`",
      "`-1.0`",
      "`01e-3`",
      "`+0001`",
      "`-0001`",
      "`++1`",
      "`+-1`",
      "`--1`",
      "123w",
      "123d",
      "123h"
    };

    String[] suffixInResultColumns = {
      "a_1",
      "aaa",
      "in",
      "between",
      "is",
      "select",
      "`a.b`",
      "`111`",
      "`a``b`",
      "`a.\"b`",
      "`a.'b`",
      "````",
      "`c.d.```",
      "abc",
      "`+12`",
      "`1e3`",
      "`001`",
      "`-1.0`",
      "`01e-3`",
      "`+0001`",
      "`-0001`",
      "`++1`",
      "`+-1`",
      "`--1`",
      "123w",
      "123d",
      "123h"
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String createNodeName : createNodeNames) {
        String createSql = String.format("CREATE TIMESERIES root.sg1.d1.%s INT32", createNodeName);
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, %s) VALUES(1, 1)", createNodeName);
        statement.execute(createSql);
        statement.execute(insertSql);
      }

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
        Set<String> expectedResult = new HashSet<>(Arrays.asList(resultTimeseries));

        while (resultSet.next()) {
          Assert.assertTrue(
              expectedResult.contains(resultSet.getString(ColumnHeaderConstant.TIMESERIES)));
          expectedResult.remove(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        Assert.assertEquals(0, expectedResult.size());
      }

      for (int i = 0; i < selectNodeNames.length; i++) {
        String selectSql =
            String.format("SELECT %s FROM root.sg1.d1 WHERE time = 1", selectNodeNames[i]);
        try (ResultSet resultSet = statement.executeQuery(selectSql)) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(1, resultSet.getInt("root.sg1.d1." + suffixInResultColumns[i]));
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNodeNameIllegal() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // nodeName with special characters should be quoted with '`'
      try {
        statement.execute("create timeseries root.sg1.d1.`a INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.[a INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.a! INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.a\" INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.a' INT32");
        fail();
      } catch (Exception ignored) {
      }

      // nodeName consists of numbers should be quoted with '`'
      try {
        statement.execute("create timeseries root.sg1.d1.111 INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.012 INT32");
        fail();
      } catch (Exception ignored) {
      }

      // shouled use double '`' in a quoted nodeName
      try {
        statement.execute("create timeseries root.sg1.d1.`a`` INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.``a` INT32");
        fail();
      } catch (Exception ignored) {
      }

      // reserved words can not be identifier
      try {
        statement.execute("create timeseries root.sg1.d1.root INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.time INT32");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create timeseries root.sg1.d1.timestamp INT32");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateIllegalStorageGroup() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.execute("create database root.sg1.d1.");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.`a.b` INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.`a.``b` INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.text TEXT");
      int pointCnt = 3;
      for (int i = 0; i < pointCnt; i++) {
        statement.execute(
            String.format(
                "insert into root.sg1.d1(time,%s,%s,%s) values(%d,%d,%d,%d)",
                "`1`", "`a.b`", "`a.``b`", i, i, i, i));
      }

      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT `1` + 1 FROM root.sg1.d1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(pointCnt, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT (`1`*`1`)+1-`a.b` FROM root.sg1.d1 where `1` > 1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT (`1`*`1`)+1-`a.b` FROM root.sg1.d1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(pointCnt, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT (`1`*`1`)+1-`a.b` FROM root.sg1.d1 where `1`>0")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT avg(`1`)+1 FROM root.sg1.d1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT count(`1`)+1 FROM root.sg1.d1 where `1`>1")) {
        while (resultSet.next()) {
          Assert.assertEquals(2.0, resultSet.getDouble(1), 1e-7);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT sin(`1`) + 1 FROM root.sg1.d1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(pointCnt, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT sin(`1`) + 1 FROM root.sg1.d1 where `1`>1")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT text FROM root.sg1.d1 where text = '\'")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT text FROM root.sg1.d1 where text = '\' or text = 'asdf'")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT text FROM root.sg1.d1 where text = '\\'")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT text FROM root.sg1.d1 where text = '\\' and text = 'asdf'")) {
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUDFName() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] udfNames = {"udf", "`udf.test`", "`012`", "`udf```"};

      String[] resultNames = {"udf", "udf.test", "012", "udf`"};

      String createSql = "create function %s as 'org.apache.iotdb.db.query.udf.example.Adder'";
      for (String udfName : udfNames) {
        statement.execute(String.format(createSql, udfName));
      }
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        Set<String> expectedResult = new HashSet<>(Arrays.asList(resultNames));
        while (resultSet.next()) {
          if (resultSet.getString(2).equals("external UDTF")) {
            String udf = resultSet.getString(1).toLowerCase();
            Assert.assertTrue(expectedResult.contains(udf));
            expectedResult.remove(udf);
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }

    // Illegal Name
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create function udf` as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create function ``udf` as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create function 111 as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create function 'udf' as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create function \"udf\" as 'org.apache.iotdb.db.query.udf.example.Adder'");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testUserName() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] userNames = new String[] {"userid", "userid0", "user_id", "user0id", "`a22233`"};

      String[] resultNames =
          new String[] {"root", "userid", "userid0", "user_id", "user0id", "a22233"};

      String createUsersSql = "create user %s 'pwd123' ";
      for (String userName : userNames) {
        statement.execute(String.format(createUsersSql, userName));
      }
      Set<String> expectedResult = new HashSet<>(Arrays.asList(resultNames));
      try (ResultSet resultSet = statement.executeQuery("list user")) {
        while (resultSet.next()) {
          String user = resultSet.getString(ColumnHeaderConstant.USER).toLowerCase();
          Assert.assertTrue(expectedResult.contains(user));
          expectedResult.remove(user);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }

    // Illegal names
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create user `abcd`` 'pwd123'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create user `abcd 'pwd123'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create user 12345 'pwd123'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create user a.b.c 'pwd123'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create user a!@bc 'pwd123'");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testRoleName() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] roleNames = new String[] {"roleid", "roleid0", "role_id", "role0id", "`a22233`"};

      String[] resultNames = new String[] {"roleid", "roleid0", "role_id", "role0id", "a22233"};
      String createRolesSql = "create role %s";
      for (String roleName : roleNames) {
        statement.execute(String.format(createRolesSql, roleName));
      }
      Set<String> expectedResult = new HashSet<>(Arrays.asList(resultNames));
      try (ResultSet resultSet = statement.executeQuery("list role")) {
        while (resultSet.next()) {
          String role = resultSet.getString(ColumnHeaderConstant.ROLE).toLowerCase();
          Assert.assertTrue(expectedResult.contains(role));
          expectedResult.remove(role);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }

    // Illegal names
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create role `abcd``");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create role `abcd");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create role 123456");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create role a.b.c");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("create role a!b%c");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  // todo: add this back when supporting trigger in cluster

  //  @Test
  //  public void testTriggerName() {
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      String[] timeseries = {
  //        "root.vehicle.d1.s1",
  //        "root.vehicle.d1.s2",
  //        "root.vehicle.d1.s3",
  //        "root.vehicle.d1.s4",
  //        "root.vehicle.d1.s5",
  //      };
  //
  //      String[] triggerNames = {
  //        "`trigger`", "trigger1", "`test```", "`111`", "`[trigger]`",
  //      };
  //
  //      String[] resultNames = {
  //        "trigger", "trigger1", "test`", "111", "[trigger]",
  //      };
  //
  //      // show
  //      try (ResultSet resultSet = statement.executeQuery("show triggers")) {
  //        assertFalse(resultSet.next());
  //      }
  //
  //      String createTimeSereisSql = "CREATE TIMESERIES %s FLOAT";
  //      String createTriggerSql =
  //          "create trigger %s before insert on %s "
  //              + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'";
  //      for (int i = 0; i < timeseries.length; ++i) {
  //        statement.execute(String.format(createTimeSereisSql, timeseries[i]));
  //        statement.execute(String.format(createTriggerSql, triggerNames[i], timeseries[i]));
  //      }
  //      Set<String> expectedResult = new HashSet<>(Arrays.asList(resultNames));
  //      try (ResultSet resultSet = statement.executeQuery("show triggers")) {
  //        while (resultSet.next()) {
  //          String trigger = resultSet.getString(1).toLowerCase();
  //          Assert.assertTrue(expectedResult.contains(trigger));
  //          expectedResult.remove(trigger);
  //        }
  //      }
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail();
  //    }
  //  }

  //  @Test
  //  public void testTriggerNameIllegal() {
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      try {
  //        statement.execute(
  //            "create trigger trigger` before insert on root.sg1.d1  "
  //                + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "create trigger `trigger`` before insert on root.sg1.d1  "
  //                + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "create trigger 111 before insert on root.sg1.d1  "
  //                + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "create trigger 'tri' before insert on root.sg1.d1  "
  //                + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "create trigger \"tri\" before insert on root.sg1.d1  "
  //                + "as 'org.apache.iotdb.db.storageengine.trigger.example.Accumulator'");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail();
  //    }
  //  }
  //

  // todo: add this back when supporting cq in new cluster

  //  @Test
  //  public void testContinuousQueryNameIllegal() {
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      try {
  //        statement.execute(
  //            "CREATE CONTINUOUS QUERY `cq1 "
  //                + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
  //                + "GROUP BY time(1s) END");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "CREATE CONTINUOUS QUERY 111 "
  //                + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
  //                + "GROUP BY time(1s) END");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "CREATE CONTINUOUS QUERY ``cq1` "
  //                + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
  //                + "GROUP BY time(1s) END");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "CREATE CONTINUOUS QUERY 'cq1' "
  //                + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
  //                + "GROUP BY time(1s) END");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //      try {
  //        statement.execute(
  //            "CREATE CONTINUOUS QUERY \"cq1\" "
  //                + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
  //                + "GROUP BY time(1s) END");
  //        fail();
  //      } catch (Exception ignored) {
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail();
  //    }
  //  }

  @Test
  public void testPipeSinkNameIllegal() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE PIPESINK test` AS IoTDB (`ip` = '127.0.0.1')");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("CREATE PIPESINK ``test` AS IoTDB (`ip` = '127.0.0.1')");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("CREATE PIPESINK test.1 AS IoTDB (`ip` = '127.0.0.1')");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("CREATE PIPESINK 12345 AS IoTDB (`ip` = '127.0.0.1')");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("CREATE PIPESINK a!@cb AS IoTDB (`ip` = '127.0.0.1')");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTemplateName() {
    String[] templateNames = {
      "id", "ID", "id0", "_id", "0id", "`233`",
    };

    String[] resultNames = {
      "id", "ID", "id0", "_id", "0id", "233",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String templateName : templateNames) {
        String createTemplateSql =
            String.format(
                "create device template %s (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
                templateName);
        statement.execute(createTemplateSql);
      }

      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        Set<String> expectedResult = new HashSet<>(Arrays.asList(resultNames));
        while (resultSet.next()) {
          Assert.assertTrue(expectedResult.contains(resultSet.getString("TemplateName")));
          expectedResult.remove(resultSet.getString("TemplateName"));
        }
        Assert.assertEquals(0, expectedResult.size());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTemplateNameIllegal() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create device template `a`` "
                + "(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create device template 111 "
                + "(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create device template `a "
                + "(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create device template 'a' "
                + "(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "create device template \"a\" "
                + "(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNodeNameWithWildcard() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.device_123.s1 INT32");

      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.device_123")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.device_*")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.*_123")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.*123")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.*_12*")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.*12*")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg.*e*")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
