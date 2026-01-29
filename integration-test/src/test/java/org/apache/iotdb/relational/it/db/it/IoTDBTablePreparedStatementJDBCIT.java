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
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTablePreparedStatementJDBCIT {

  private static final String DATABASE_NAME = "test";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE test_table(id STRING TAG, name STRING FIELD, value DOUBLE FIELD, "
              + "int_value INT32 FIELD, long_value INT64 FIELD)");
      statement.execute(
          "INSERT INTO test_table VALUES (2025-01-01T00:00:00, '1', 'Alice', 100.5, 10, 1000)");
      statement.execute(
          "INSERT INTO test_table VALUES (2025-01-01T00:01:00, '2', 'Bob', 200.3, 20, 2000)");
      statement.execute(
          "INSERT INTO test_table VALUES (2025-01-01T00:02:00, '3', 'Charlie', 300.7, 30, 3000)");
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private Connection getConnection() throws SQLException {
    Connection connection = EnvFactory.getEnv().getTableConnection();
    if (connection instanceof ClusterTestConnection) {
      // Get the underlying real JDBC connection that supports prepareStatement
      return ((ClusterTestConnection) connection).writeConnection.getUnderlyingConnection();
    }
    return connection;
  }

  @Test
  public void testPreparedStatementWithIntParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE int_value = ?")) {
        ps.setInt(1, 20);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(20, rs.getInt("int_value"));
          assertEquals("Bob", rs.getString("name"));
          assertEquals(200.3, rs.getDouble("value"), 0.001);
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithStringParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE name = ?")) {
        ps.setString(1, "Charlie");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("3", rs.getString("id"));
          assertEquals("Charlie", rs.getString("name"));
          assertEquals(300.7, rs.getDouble("value"), 0.001);
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithMultipleParameters() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE id = ? AND value < ?")) {
        ps.setString(1, "2");
        ps.setDouble(2, 300.0);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("2", rs.getString("id"));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementExecuteMultipleTimes() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE id = ?")) {
        // First execution
        ps.setString(1, "1");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Alice", rs.getString("name"));
          assertFalse(rs.next());
        }

        // Second execution with different parameter
        ps.setString(1, "3");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Charlie", rs.getString("name"));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithDoubleParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE value > ?")) {
        ps.setDouble(1, 250.0);
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            assertTrue(rs.getDouble("value") > 250.0);
            count++;
          }
          // Only Charlie (300.7 > 250) satisfies the condition
          assertEquals(1, count);
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithLongParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE long_value = ?")) {
        ps.setLong(1, 1000L);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1000L, rs.getLong("long_value"));
          assertEquals("Alice", rs.getString("name"));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithFloatParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE value < ?")) {
        ps.setFloat(1, 150.0f);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Alice", rs.getString("name"));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithBooleanParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);
      // Create table with boolean column
      stmt.execute("CREATE TABLE bool_table(flag BOOLEAN FIELD)");
      stmt.execute("INSERT INTO bool_table VALUES (2025-01-01T00:00:00, true)");
      stmt.execute("INSERT INTO bool_table VALUES (2025-01-01T00:01:00, false)");

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM bool_table WHERE flag = ?")) {
        ps.setBoolean(1, true);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(rs.getBoolean("flag"));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithNullParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE name = ?")) {
        ps.setNull(1, java.sql.Types.VARCHAR);
        try (ResultSet rs = ps.executeQuery()) {
          // No rows should match null
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementWithBinaryParameter() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);
      stmt.execute("CREATE TABLE blob_table(data BLOB FIELD)");

      byte[] testData = new byte[] {0x01, 0x02, 0x03};
      stmt.execute("INSERT INTO blob_table VALUES (2025-01-01T00:00:00, X'010203')");

      try (PreparedStatement queryPs =
          connection.prepareStatement("SELECT data FROM blob_table WHERE data = ?")) {
        queryPs.setBytes(1, testData);

        try (ResultSet rs = queryPs.executeQuery()) {
          assertTrue(rs.next());
          assertArrayEquals(testData, rs.getBytes("data"));
        }
      }
    }
  }

  @Test
  public void testPreparedStatementResultSetMetaData() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement(
              "SELECT id, name, value, int_value, long_value FROM test_table WHERE id = ?")) {
        ps.setString(1, "1");
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData metaData = rs.getMetaData();
          assertEquals(5, metaData.getColumnCount());
          assertEquals("id", metaData.getColumnLabel(1).toLowerCase());
          assertEquals("name", metaData.getColumnLabel(2).toLowerCase());
          assertEquals("value", metaData.getColumnLabel(3).toLowerCase());
          assertEquals("int_value", metaData.getColumnLabel(4).toLowerCase());
          assertEquals("long_value", metaData.getColumnLabel(5).toLowerCase());
        }
      }
    }
  }

  @Test
  public void testPreparedStatementParameterMetaData() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement(
              "SELECT id, name, value FROM test_table WHERE id = ? AND value > ?")) {
        ParameterMetaData metaData = ps.getParameterMetaData();
        assertEquals(2, metaData.getParameterCount());
      }
    }
  }

  @Test
  public void testPreparedStatementInsert() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);
      stmt.execute("CREATE TABLE insert_test(id INT32 FIELD, name STRING FIELD)");

      try (PreparedStatement ps =
          connection.prepareStatement("INSERT INTO insert_test VALUES (?, ?, ?)")) {
        ps.setLong(1, System.currentTimeMillis());
        ps.setInt(2, 100);
        ps.setString(3, "TestName");
        int affected = ps.executeUpdate();
        assertTrue(affected >= 0);
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM insert_test WHERE id = 100")) {
        assertTrue(rs.next());
        assertEquals("TestName", rs.getString("name"));
      }
    }
  }

  @Test
  public void testPreparedStatementAggregation() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT COUNT(*) as cnt FROM test_table WHERE value > ?")) {
        ps.setDouble(1, 150.0);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt")); // Bob and Charlie
        }
      }
    }
  }

  @Test
  public void testPreparedStatementClearParameters() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps =
          connection.prepareStatement("SELECT * FROM test_table WHERE id = ?")) {
        ps.setString(1, "1");
        ps.clearParameters();
        // After clear, should be able to set new parameters
        ps.setString(1, "2");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Bob", rs.getString("name"));
        }
      }
    }
  }

  @Test
  public void testMultiplePreparedStatements() throws SQLException {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute("USE " + DATABASE_NAME);

      try (PreparedStatement ps1 =
              connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
          PreparedStatement ps2 =
              connection.prepareStatement(
                  "SELECT COUNT(*) as cnt FROM test_table WHERE value > ?")) {
        // Execute first prepared statement
        ps1.setString(1, "1");
        try (ResultSet rs = ps1.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Alice", rs.getString("name"));
        }

        // Execute second prepared statement
        ps2.setDouble(1, 200.0);
        try (ResultSet rs = ps2.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
      }
    }
  }
}
