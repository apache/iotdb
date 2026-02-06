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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNullPredicateTableIT {

  private static final String DATABASE_NAME = "test_null_predicate";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    createTablesAndInsertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTablesAndInsertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);

      statement.execute(
          "CREATE TABLE test_table_no_tag("
              + "id INT32 FIELD, "
              + "name STRING FIELD, "
              + "value DOUBLE FIELD)");

      statement.execute(
          "INSERT INTO test_table_no_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:00:00, 1, 'Alice', 100.5)");
      statement.execute(
          "INSERT INTO test_table_no_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:01:00, 2, 'Bob', 200.3)");
      statement.execute(
          "INSERT INTO test_table_no_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:02:00, 3, null, 300.7)");

      statement.execute(
          "CREATE TABLE test_table_with_tag("
              + "id STRING TAG, "
              + "name STRING FIELD, "
              + "value DOUBLE FIELD)");

      statement.execute(
          "INSERT INTO test_table_with_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:00:00, '1', 'Alice', 100.5)");
      statement.execute(
          "INSERT INTO test_table_with_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:01:00, '2', 'Bob', 200.3)");
      statement.execute(
          "INSERT INTO test_table_with_tag(time, id, name, value) "
              + "VALUES (2025-01-01T00:02:00, '3', null, 300.7)");

    } catch (SQLException e) {
      fail("Failed to create tables and insert data: " + e.getMessage());
    }
  }

  @Test
  public void testNullPredicateOnTableWithoutTag() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet rs =
          statement.executeQuery("SELECT * FROM test_table_no_tag WHERE name = null")) {
        assertFalse("Expected no rows when using 'name = null'", rs.next());
      }

    } catch (SQLException e) {
      fail("testNullPredicateOnTableWithoutTag failed: " + e.getMessage());
    }
  }

  @Test
  public void testNullPredicateOnTableWithTag() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet rs =
          statement.executeQuery("SELECT * FROM test_table_with_tag WHERE name = null")) {
        assertFalse("Expected no rows when using 'name = null'", rs.next());
      }

    } catch (SQLException e) {
      fail("testNullPredicateOnTableWithTag failed: " + e.getMessage());
    }
  }

  @Test
  public void testIsNullPredicateOnTableWithoutTag() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet rs =
          statement.executeQuery("SELECT * FROM test_table_no_tag WHERE name IS NULL")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        Assert.assertEquals("Expected 1 row with NULL name", 1, count);
      }

    } catch (SQLException e) {
      fail("testIsNullPredicateOnTableWithoutTag failed: " + e.getMessage());
    }
  }

  @Test
  public void testIsNullPredicateOnTableWithTag() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet rs =
          statement.executeQuery("SELECT * FROM test_table_with_tag WHERE name IS NULL")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        Assert.assertEquals("Expected 1 row with NULL name", 1, count);
      }

    } catch (SQLException e) {
      fail("testIsNullPredicateOnTableWithTag failed: " + e.getMessage());
    }
  }
}
