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
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPreparedStatementIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE test_table(id INT64 FIELD, name STRING FIELD, value DOUBLE FIELD)",
        "INSERT INTO test_table VALUES (2025-01-01T00:00:00, 1, 'Alice', 100.5)",
        "INSERT INTO test_table VALUES (2025-01-01T00:01:00, 2, 'Bob', 200.3)",
        "INSERT INTO test_table VALUES (2025-01-01T00:02:00, 3, 'Charlie', 300.7)",
        "INSERT INTO test_table VALUES (2025-01-01T00:03:00, 4, 'David', 400.2)",
        "INSERT INTO test_table VALUES (2025-01-01T00:04:00, 5, 'Eve', 500.9)",
      };

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed: " + e.getMessage());
    }
  }

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testPrepareAndExecute() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray = new String[] {"2025-01-01T00:01:00.000Z,2,Bob,200.3,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement
      statement.execute("PREPARE stmt1 FROM SELECT * FROM test_table WHERE id = ?");
      // Execute with parameter
      tableResultSetEqualTest("EXECUTE stmt1 USING 2", expectedHeader, retArray, DATABASE_NAME);
      // Deallocate
      statement.execute("DEALLOCATE PREPARE stmt1");
    } catch (SQLException e) {
      fail("testPrepareAndExecute failed: " + e.getMessage());
    }
  }

  @Test
  public void testPrepareAndExecuteMultipleTimes() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray1 = new String[] {"2025-01-01T00:00:00.000Z,1,Alice,100.5,"};
    String[] retArray2 = new String[] {"2025-01-01T00:02:00.000Z,3,Charlie,300.7,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement
      statement.execute("PREPARE stmt2 FROM SELECT * FROM test_table WHERE id = ?");
      // Execute multiple times with different parameters
      tableResultSetEqualTest("EXECUTE stmt2 USING 1", expectedHeader, retArray1, DATABASE_NAME);
      tableResultSetEqualTest("EXECUTE stmt2 USING 3", expectedHeader, retArray2, DATABASE_NAME);
      // Deallocate
      statement.execute("DEALLOCATE PREPARE stmt2");
    } catch (SQLException e) {
      fail("testPrepareAndExecuteMultipleTimes failed: " + e.getMessage());
    }
  }

  @Test
  public void testPrepareWithMultipleParameters() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray = new String[] {"2025-01-01T00:01:00.000Z,2,Bob,200.3,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement with multiple parameters
      statement.execute("PREPARE stmt3 FROM SELECT * FROM test_table WHERE id = ? AND value > ?");
      // Execute with multiple parameters
      tableResultSetEqualTest(
          "EXECUTE stmt3 USING 2, 150.0", expectedHeader, retArray, DATABASE_NAME);
      // Deallocate
      statement.execute("DEALLOCATE PREPARE stmt3");
    } catch (SQLException e) {
      fail("testPrepareWithMultipleParameters failed: " + e.getMessage());
    }
  }

  @Test
  public void testExecuteImmediate() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray = new String[] {"2025-01-01T00:03:00.000Z,4,David,400.2,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Execute immediate with SQL string and parameters
      tableResultSetEqualTest(
          "EXECUTE IMMEDIATE 'SELECT * FROM test_table WHERE id = ?' USING 4",
          expectedHeader,
          retArray,
          DATABASE_NAME);
    } catch (SQLException e) {
      fail("testExecuteImmediate failed: " + e.getMessage());
    }
  }

  @Test
  public void testExecuteImmediateWithoutParameters() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray = new String[] {"5,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Execute immediate without parameters
      tableResultSetEqualTest(
          "EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM test_table'",
          expectedHeader,
          retArray,
          DATABASE_NAME);
    } catch (SQLException e) {
      fail("testExecuteImmediateWithoutParameters failed: " + e.getMessage());
    }
  }

  @Test
  public void testExecuteImmediateWithMultipleParameters() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray = new String[] {"2025-01-01T00:04:00.000Z,5,Eve,500.9,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Execute immediate with multiple parameters
      tableResultSetEqualTest(
          "EXECUTE IMMEDIATE 'SELECT * FROM test_table WHERE id = ? AND value > ?' USING 5, 450.0",
          expectedHeader,
          retArray,
          DATABASE_NAME);
    } catch (SQLException e) {
      fail("testExecuteImmediateWithMultipleParameters failed: " + e.getMessage());
    }
  }

  @Test
  public void testDeallocateNonExistentStatement() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Try to deallocate a non-existent statement
      SQLException exception =
          assertThrows(
              SQLException.class, () -> statement.execute("DEALLOCATE PREPARE non_existent_stmt"));
      assertTrue(
          exception.getMessage().contains("does not exist")
              || exception.getMessage().contains("Prepared statement"));
    } catch (SQLException e) {
      fail("testDeallocateNonExistentStatement failed: " + e.getMessage());
    }
  }

  @Test
  public void testExecuteNonExistentStatement() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Try to execute a non-existent statement
      SQLException exception =
          assertThrows(
              SQLException.class, () -> statement.execute("EXECUTE non_existent_stmt USING 1"));
      assertTrue(
          exception.getMessage().contains("does not exist")
              || exception.getMessage().contains("Prepared statement"));
    } catch (SQLException e) {
      fail("testExecuteNonExistentStatement failed: " + e.getMessage());
    }
  }

  @Test
  public void testMultiplePreparedStatements() {
    String[] expectedHeader1 = new String[] {"time", "id", "name", "value"};
    String[] retArray1 = new String[] {"2025-01-01T00:00:00.000Z,1,Alice,100.5,"};
    String[] expectedHeader2 = new String[] {"_col0"};
    String[] retArray2 = new String[] {"4,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare multiple statements
      statement.execute("PREPARE stmt4 FROM SELECT * FROM test_table WHERE id = ?");
      statement.execute("PREPARE stmt5 FROM SELECT COUNT(*) FROM test_table WHERE value > ?");
      // Execute both statements
      tableResultSetEqualTest("EXECUTE stmt4 USING 1", expectedHeader1, retArray1, DATABASE_NAME);
      tableResultSetEqualTest(
          "EXECUTE stmt5 USING 200.0", expectedHeader2, retArray2, DATABASE_NAME);
      // Deallocate both
      statement.execute("DEALLOCATE PREPARE stmt4");
      statement.execute("DEALLOCATE PREPARE stmt5");
    } catch (SQLException e) {
      fail("testMultiplePreparedStatements failed: " + e.getMessage());
    }
  }

  @Test
  public void testPrepareDuplicateName() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement
      statement.execute("PREPARE stmt6 FROM SELECT * FROM test_table WHERE id = ?");
      // Try to prepare another statement with the same name
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> statement.execute("PREPARE stmt6 FROM SELECT * FROM test_table WHERE id = ?"));
      assertTrue(
          exception.getMessage().contains("already exists")
              || exception.getMessage().contains("Prepared statement"));
      // Cleanup
      statement.execute("DEALLOCATE PREPARE stmt6");
    } catch (SQLException e) {
      fail("testPrepareDuplicateName failed: " + e.getMessage());
    }
  }

  @Test
  public void testPrepareAndExecuteWithAggregation() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray = new String[] {"300.40000000000003,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement with aggregation
      statement.execute(
          "PREPARE stmt7 FROM SELECT AVG(value) FROM test_table WHERE id >= ? AND id <= ?");
      // Execute with parameters
      tableResultSetEqualTest("EXECUTE stmt7 USING 2, 4", expectedHeader, retArray, DATABASE_NAME);
      // Deallocate
      statement.execute("DEALLOCATE PREPARE stmt7");
    } catch (SQLException e) {
      fail("testPrepareAndExecuteWithAggregation failed: " + e.getMessage());
    }
  }

  @Test
  public void testPrepareAndExecuteWithStringParameter() {
    String[] expectedHeader = new String[] {"time", "id", "name", "value"};
    String[] retArray = new String[] {"2025-01-01T00:02:00.000Z,3,Charlie,300.7,"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // Prepare a statement with string parameter
      statement.execute("PREPARE stmt8 FROM SELECT * FROM test_table WHERE name = ?");
      // Execute with string parameter
      tableResultSetEqualTest(
          "EXECUTE stmt8 USING 'Charlie'", expectedHeader, retArray, DATABASE_NAME);
      // Deallocate
      statement.execute("DEALLOCATE PREPARE stmt8");
    } catch (SQLException e) {
      fail("testPrepareAndExecuteWithStringParameter failed: " + e.getMessage());
    }
  }
}
