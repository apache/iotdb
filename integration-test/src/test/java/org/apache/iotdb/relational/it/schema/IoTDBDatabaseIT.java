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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDBColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDBDetailsColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDatabaseIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testManageDatabase() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {

      // create
      statement.execute("create database test with (ttl='INF')");

      // create duplicated database without IF NOT EXISTS
      try {
        statement.execute("create database test");
        fail("create database test shouldn't succeed because test already exists");
      } catch (final SQLException e) {
        assertEquals("501: Database test already exists", e.getMessage());
      }

      // create duplicated database with IF NOT EXISTS
      statement.execute("create database IF NOT EXISTS test");

      String[] databaseNames = new String[] {"test"};
      String[] TTLs = new String[] {"INF"};
      int[] schemaReplicaFactors = new int[] {1};
      int[] dataReplicaFactors = new int[] {1};
      int[] timePartitionInterval = new int[] {604800000};
      String[] model = new String[] {"TABLE"};

      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBDetailsColumnHeaders.size(); i++) {
          assertEquals(
              showDBDetailsColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          assertEquals(model[cnt], resultSet.getString(6));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      // use
      statement.execute("use test");

      // use nonexistent database
      try {
        statement.execute("use test1");
        fail("use test1 shouldn't succeed because test1 doesn't exist");
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      // drop
      statement.execute("drop database test");
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        assertFalse(resultSet.next());
      }

      // drop nonexistent database without IF EXISTS
      try {
        statement.execute("drop database test");
        fail("drop database test shouldn't succeed because test doesn't exist");
      } catch (final SQLException e) {
        assertEquals("500: Database test doesn't exist", e.getMessage());
      }

      // drop nonexistent database with IF EXISTS
      statement.execute("drop database IF EXISTS test");

      // Test create database with properties
      statement.execute(
          "create database test_prop with (ttl=300, schema_region_group_num=DEFAULT, time_partition_interval=100000)");
      databaseNames = new String[] {"test_prop"};
      TTLs = new String[] {"300"};
      timePartitionInterval = new int[] {100000};

      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      try {
        statement.execute("create database test_prop_2 with (non_exist_prop=DEFAULT)");
        fail(
            "create database test_prop_2 shouldn't succeed because the property key does not exist.");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("Unsupported database property key: non_exist_prop"));
      }

      // create with strange name
      try {
        statement.execute("create database 1test");
        fail(
            "create database 1test shouldn't succeed because 1test is not a legal identifier; identifiers must not start with a digit; surround the identifier with double quotes");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }

      statement.execute("create database \"1test\"");
      statement.execute("use \"1test\"");
      statement.execute("drop database \"1test\"");

      try {
        statement.execute("create database 1");
        fail("create database 1 shouldn't succeed because 1 is not a legal identifier");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }

      statement.execute("create database \"1\"");
      statement.execute("use \"1\"");
      statement.execute("drop database \"1\"");

      try {
        statement.execute("create database a.b");
        fail("create database a.b shouldn't succeed because a.b is not a legal identifier");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '.'"));
      }

      // Test length limitation
      statement.execute(
          "create database thisDatabaseLengthIsPreciselySixtyFourThusItCanBeNormallyCreated");

      try {
        statement.execute(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated");
        fail(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated shouldn't succeed because it's length has exceeded 64");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("the length of database name shall not exceed 64"));
      }

    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDatabaseWithSpecificCharacters() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database \"````x.\"");
        fail("create database ````x. shouldn't succeed because it contains '.'");
      } catch (final SQLException e) {
        assertEquals(
            "509: ````x. is not a legal path, because the database name can only contain english or chinese characters, numbers, backticks and underscores.",
            e.getMessage());
      }

      try {
        statement.execute("create database \"#\"");
        fail("create database # shouldn't succeed because it contains illegal character '#'");
      } catch (final SQLException e) {
        assertEquals(
            "509: # is not a legal path, because the database name can only contain english or chinese characters, numbers, backticks and underscores.",
            e.getMessage());
      }

      statement.execute("create database \"````x\"");

      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        assertTrue(resultSet.next());
        assertEquals("````x", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      statement.execute("use \"````x\"");

      statement.execute("create table table0 (a id, b attribute, c int32)");

      statement.execute("desc table0");
      statement.execute("desc \"````x\".table0");

      statement.execute("show tables");
      statement.execute("show tables from \"````x\"");

      statement.execute("insert into table0 (time, a, b, c) values(0, '1', '2', 3)");
      statement.execute("insert into \"````x\".table0 (time, a, b, c) values(1, '1', '2', 3)");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select a, b, c from \"````x\".table0 where time = 0"),
          "a,b,c,",
          Collections.singleton("1,2,3,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0"),
          "a,b,",
          Collections.singleton("1,2,"));

      statement.execute("update \"````x\".table0 set b = '4'");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0"),
          "a,b,",
          Collections.singleton("1,4,"));
    }
  }
}
