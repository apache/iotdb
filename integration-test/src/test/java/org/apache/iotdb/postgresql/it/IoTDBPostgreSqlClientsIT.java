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

package org.apache.iotdb.postgresql.it;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** pgJDBC as DBeaver and other JDBC tools use it. */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlClientsIT {

  private static final long START = 1_767_225_600_000L; // 2026-01-01T00:00:00Z
  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnablePostgreSqlService(true)
        .setPostgreSqlServicePort(port);
    EnvFactory.getEnv().initClusterEnvironment();
    PgItSupport.awaitListening(port);
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE plant");
      session.executeNonQueryStatement("USE plant");
      session.executeNonQueryStatement(
          "CREATE TABLE sensors (device STRING TAG, ok BOOLEAN FIELD, n INT32 FIELD,"
              + " big INT64 FIELD, f FLOAT FIELD, temp DOUBLE FIELD, label STRING FIELD,"
              + " raw BLOB FIELD, day DATE FIELD, at TIMESTAMP FIELD)");
      for (int i = 0; i < 100; i++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO sensors (time, device, ok, n, big, f, temp, label, raw, day, at)"
                    + " VALUES (%d, 'd%d', %s, %d, %d, %s, %s, 'l%d', X'0%d', '2026-01-%02d', %d)",
                START + i * 1000L,
                i % 3,
                i % 2 == 0 ? "true" : "false",
                i,
                i * 10_000_000_000L,
                i / 2.0f,
                i / 4.0,
                i,
                i % 10,
                i % 28 + 1,
                START + i));
      }
      // ids that look like numbers, and one with an underscore (review of apache/iotdb#19350)
      // in a database of their own, so that the metadata test still sees only plant.sensors
      session.executeNonQueryStatement("CREATE DATABASE lab");
      session.executeNonQueryStatement("CREATE TABLE lab.ids (device STRING TAG, v INT64 FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO lab.ids (time, device, v) VALUES (1, '1001', 7), (2, '0012', 8),"
              + " (3, 'd_1', 9), (4, 'dx1', 10)");
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Connection connect(String mode) throws SQLException {
    return connect(mode, new Properties());
  }

  private static Connection connect(String mode, Properties extra) throws SQLException {
    Properties props = new Properties();
    props.putAll(extra);
    props.setProperty("user", SessionConfig.DEFAULT_USER);
    props.setProperty("password", SessionConfig.DEFAULT_PASSWORD);
    props.setProperty("sslmode", "disable");
    props.setProperty("preferQueryMode", mode);
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/any", props);
  }

  @Test
  public void databaseMetaData() throws Exception {
    try (Connection c = connect("extended")) {
      DatabaseMetaData meta = c.getMetaData();
      StringBuilder tables = new StringBuilder();
      try (ResultSet rs = meta.getTables(null, "plant", "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          tables.append(rs.getString("TABLE_SCHEM")).append('.').append(rs.getString("TABLE_NAME"));
        }
      }
      assertEquals("plant.sensors", tables.toString());
      StringBuilder columns = new StringBuilder();
      try (ResultSet rs = meta.getColumns(null, "plant", "sensors", "%")) {
        while (rs.next()) {
          columns.append(rs.getString("COLUMN_NAME")).append(':').append(rs.getString("TYPE_NAME"));
          columns.append(' ');
        }
      }
      assertEquals(
          "time:timestamptz device:text ok:bool n:int4 big:int8 f:float4 temp:float8 label:text"
              + " raw:bytea day:date at:timestamptz ",
          columns.toString());
      try (ResultSet rs = meta.getPrimaryKeys(null, "plant", "sensors")) {
        assertFalse("no keys in the table model", rs.next());
      }
    }
  }

  @Test
  public void preparedStatementsWithEveryParameterType() throws Exception {
    try (Connection c = connect("extended");
        PreparedStatement p =
            c.prepareStatement(
                "SELECT n, big, label, raw, day FROM plant.sensors WHERE device = ? AND ok = ?"
                    + " AND n >= ? AND big >= ? AND f >= ? AND temp >= ? AND day >= ?"
                    + " AND time >= ? ORDER BY time LIMIT 1")) {
      // executed repeatedly with different values (pgJDBC switches to a server-side statement)
      for (int round = 0; round < 8; round++) {
        int i = round * 6; // device d0, ok = true
        p.setString(1, "d0");
        p.setBoolean(2, true);
        p.setInt(3, i);
        p.setLong(4, i * 10_000_000_000L);
        p.setFloat(5, i / 2.0f);
        p.setDouble(6, i / 4.0);
        p.setObject(7, LocalDate.of(2026, 1, 1));
        p.setTimestamp(8, new Timestamp(START + i * 1000L));
        try (ResultSet rs = p.executeQuery()) {
          assertTrue("round " + round, rs.next());
          assertEquals(i, rs.getInt(1));
          assertEquals(i * 10_000_000_000L, rs.getLong(2));
          assertEquals("l" + i, rs.getString(3));
          assertArrayEquals(new byte[] {(byte) (i % 10)}, rs.getBytes(4));
          assertEquals(LocalDate.of(2026, 1, i % 28 + 1), rs.getObject(5, LocalDate.class));
        }
      }
    }
  }

  @Test
  public void fetchSizeStreamsInPortions() throws Exception {
    try (Connection c = connect("extended")) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        s.setFetchSize(7);
        int rows = 0;
        try (ResultSet rs = s.executeQuery("SELECT time FROM plant.sensors ORDER BY time")) {
          while (rs.next()) {
            assertEquals(START + rows * 1000L, rs.getTimestamp(1).getTime());
            rows++;
          }
        }
        assertEquals(100, rows);
      }
      c.commit();
    }
  }

  @Test
  public void simpleModeMultiStatementsTransactionsAndWrites() throws Exception {
    try (Connection c = connect("simple");
        Statement s = c.createStatement()) {
      // BEGIN, the query and COMMIT: the query's result is the second one
      s.execute("BEGIN; SELECT count(*) FROM plant.sensors; COMMIT");
      long count = -1;
      do {
        ResultSet rs = s.getResultSet();
        if (rs != null) {
          assertTrue(rs.next());
          count = rs.getLong(1);
        }
      } while (s.getMoreResults() || s.getUpdateCount() != -1);
      assertEquals(100L, count);
      try (ResultSet rs = s.executeQuery("SELECT count(*) FROM plant.sensors WHERE ok")) {
        assertTrue(rs.next());
        assertEquals(50L, rs.getLong(1));
      }
      try {
        s.execute("INSERT INTO plant.sensors (time, device, n) VALUES (1, 'x', 1)");
        fail("read-only");
      } catch (SQLException e) {
        assertEquals("25006", e.getSQLState());
      }
    }
  }

  @Test
  public void untypedStringParametersTakeTheirTypeFromTheContext() throws Exception {
    // stringtype=unspecified: pgJDBC sends setString values untyped, as psycopg 3 does
    Properties untyped = new Properties();
    untyped.setProperty("stringtype", "unspecified");
    try (Connection c = connect("extended", untyped);
        PreparedStatement p =
            c.prepareStatement("SELECT v FROM lab.ids WHERE device = ? AND v >= ?")) {
      for (int round = 0; round < 6; round++) {
        p.setString(1, round % 2 == 0 ? "1001" : "0012");
        p.setString(2, "5"); // compared with INT64: stays a number
        try (ResultSet rs = p.executeQuery()) {
          assertTrue("round " + round, rs.next());
          assertEquals(round % 2 == 0 ? 7L : 8L, rs.getLong(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void likeUsesBackslashAsItsEscapeCharacter() throws Exception {
    try (Connection c = connect("extended");
        Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery("SELECT device FROM lab.ids WHERE device LIKE 'd\\_1' ORDER BY time")) {
      assertTrue(rs.next());
      assertEquals("d_1", rs.getString(1));
      assertFalse("_ is escaped, dx1 does not match", rs.next());
    }
  }

  @Test
  public void escapedBytesAreUtf8() throws Exception {
    try (Connection c = connect("simple");
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT E'\\xC3\\xA9'")) {
      assertTrue(rs.next());
      assertEquals("\u00e9", rs.getString(1));
    }
  }

  @Test
  public void closedConnectionsAreNoLongerListed() throws Exception {
    long before = connections();
    try (Connection c = connect("extended");
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(*) FROM plant.sensors")) {
      assertTrue(rs.next());
      assertEquals(before + 1, connections());
    }
    // the release runs on the connection's worker after the socket is closed
    long deadline = System.currentTimeMillis() + 10_000;
    long now = connections();
    while (now != before && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
      now = connections();
    }
    assertEquals("the closed connection is gone", before, now);
  }

  private static long connections() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection();
        org.apache.iotdb.isession.SessionDataSet rows =
            session.executeQueryStatement("SELECT count(*) FROM information_schema.connections")) {
      return rows.next().getFields().get(0).getLongV();
    }
  }
}
