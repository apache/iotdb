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

import org.apache.iotdb.isession.ISession;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The PostgreSQL service on loopback against the engine: the recorded client corpus, psql's catalog
 * queries, Npgsql's type loading, date rules, pgJDBC metadata, cursors, cancel and parameters.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlEngineIT {

  private static final String PASSWORD = "Pg-Pass-2026a";
  private static final String ROOT_PASSWORD = SessionConfig.DEFAULT_PASSWORD;
  private static int port;

  @BeforeClass
  public static void start() throws Exception {
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
    try (ISession admin = EnvFactory.getEnv().getSessionConnection()) {
      admin.executeNonQueryStatement("CREATE USER pguser '" + PASSWORD + "'");
    }
    try (org.apache.iotdb.isession.ITableSession table =
        EnvFactory.getEnv().getTableSessionConnection()) {
      table.executeNonQueryStatement("CREATE DATABASE plant");
      table.executeNonQueryStatement("USE plant");
      table.executeNonQueryStatement(
          "CREATE TABLE sensors (device STRING TAG, temp DOUBLE FIELD, ok BOOLEAN FIELD,"
              + " n INT32 FIELD, label STRING FIELD)");
      table.executeNonQueryStatement(
          "INSERT INTO sensors (time, device, temp, ok, n, label) VALUES"
              + " (1000, 'd1', 20.5, true, 1, 'a'), (2000, 'd1', 21.5, false, 2, 'b'),"
              + " (3000, 'd2', 19.0, true, 3, null)");
      table.executeNonQueryStatement("GRANT SELECT ON TABLE plant.sensors TO USER pguser");
      // not readable for pguser: hidden from its catalog
      table.executeNonQueryStatement("CREATE TABLE secret (v INT32 FIELD)");
      table.executeNonQueryStatement("CREATE DATABASE other");
      table.executeNonQueryStatement("CREATE TABLE other.t (v INT32 FIELD)");
      // the reference schema of the client corpus
      // (external-service-impl/postgresql/src/test/corpus-capture)
      table.executeNonQueryStatement("CREATE DATABASE refdb");
      table.executeNonQueryStatement(
          "CREATE TABLE refdb.sensors (plant STRING TAG, line STRING TAG, machine STRING TAG,"
              + " model STRING ATTRIBUTE, temperature DOUBLE FIELD, pressure FLOAT FIELD,"
              + " \"count\" INT64 FIELD, level INT32 FIELD, running BOOLEAN FIELD,"
              + " state STRING FIELD, note STRING FIELD, blob BLOB FIELD, day DATE FIELD,"
              + " ts TIMESTAMP FIELD)");
      table.executeNonQueryStatement(
          "INSERT INTO refdb.sensors (time, plant, line, machine, model, temperature, level,"
              + " running, day) VALUES (1000, 'plant1', 'line1', 'm1', 'model1', 20.5, 3, true,"
              + " '2026-01-05')");
      // every parameter type
      table.executeNonQueryStatement(
          "CREATE TABLE refdb.ptypes (device STRING TAG, ok BOOLEAN FIELD, n INT32 FIELD,"
              + " big INT64 FIELD, f FLOAT FIELD, temp DOUBLE FIELD, label STRING FIELD,"
              + " raw BLOB FIELD, day DATE FIELD, at TIMESTAMP FIELD)");
      table.executeNonQueryStatement(
          "INSERT INTO refdb.ptypes (time, device, ok, n, big, f, temp, label, raw, day, at)"
              + " VALUES (1000, 'd0', true, 6, 60000000000, 3.0, 1.5, 'l6', X'06', '2026-01-07',"
              + " 1006)");
      // enough rows for a slow cross join
      table.executeNonQueryStatement("CREATE TABLE refdb.many (v INT32 FIELD)");
      for (int batch = 0; batch < 4; batch++) {
        StringBuilder insert = new StringBuilder("INSERT INTO refdb.many (time, v) VALUES ");
        for (int i = 0; i < 500; i++) {
          int n = batch * 500 + i;
          insert
              .append(i > 0 ? ", " : "")
              .append('(')
              .append(n + 1)
              .append(", ")
              .append(n)
              .append(')');
        }
        table.executeNonQueryStatement(insert.toString());
      }
      // instants around the DST changes in Europe/Berlin
      table.executeNonQueryStatement("CREATE TABLE refdb.dates (v INT32 FIELD, s STRING FIELD)");
      long[] dates = {
        java.time.Instant.parse("2026-01-15T09:20:30.250Z").toEpochMilli(),
        java.time.Instant.parse("2026-03-28T23:30:00Z").toEpochMilli(),
        java.time.Instant.parse("2026-03-29T10:00:00Z").toEpochMilli(),
        java.time.Instant.parse("2026-10-24T22:30:00Z").toEpochMilli(),
        java.time.Instant.parse("2026-10-25T11:00:00Z").toEpochMilli(),
        java.time.Instant.parse("2026-12-31T23:30:00Z").toEpochMilli()
      };
      for (int i = 0; i < dates.length; i++) {
        table.executeNonQueryStatement(
            "INSERT INTO refdb.dates (time, v, s) VALUES ("
                + dates[i]
                + ", "
                + i
                + ", "
                + (i == 2 ? "null" : "'s" + i + "'")
                + ")");
      }
      table.executeNonQueryStatement(
          "CREATE TABLE refdb.events (machine STRING TAG, code INT32 FIELD, message STRING FIELD)");
      table.executeNonQueryStatement(
          "INSERT INTO refdb.events (time, machine, code, message) VALUES (1000, 'm1', 1, 'e')");
    }
  }

  /** Npgsql 4.0.17's type-loading queries (PostgresDatabaseInfo.GenerateTypesQuery), verbatim. */
  private static final String[] NPGSQL_TYPE_LOADING = {
    "/*** Load all supported types ***/\n"
        + "SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\n"
        + "CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\n"
        + "CASE\n"
        + "  WHEN pg_proc.proname='array_recv' THEN a.typelem\n"
        + "  WHEN a.typtype='r' THEN rngsubtype\n"
        + "  ELSE 0\n"
        + "END AS elemoid,\n"
        + "CASE\n"
        + "  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n"
        + "  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n"
        + "  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n"
        + "  ELSE 0                                                           /* Base types first */\n"
        + "END AS ord\n"
        + "FROM pg_type AS a\n"
        + "JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\n"
        + "JOIN pg_proc ON pg_proc.oid = a.typreceive\n"
        + "LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\n"
        + "LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\n"
        + "LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\n"
        + "LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \n"
        + "WHERE\n"
        + "  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n"
        + "  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n"
        + "  (pg_proc.proname='array_recv' AND (\n"
        + "    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\n"
        + "    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\n"
        + "    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n"
        + "  )) OR\n"
        + "  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\n"
        + "ORDER BY ord",
    "/*** Load field definitions for (free-standing) composite types ***/\n"
        + "SELECT typ.oid, att.attname, att.atttypid\n"
        + "FROM pg_type AS typ\n"
        + "JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\n"
        + "JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
        + "JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\n"
        + "WHERE\n"
        + "  (typ.typtype = 'c' AND cls.relkind='c') AND\n"
        + "  attnum > 0 AND     /* Don't load system attributes */\n"
        + "  NOT attisdropped\n"
        + "ORDER BY typ.oid, att.attnum",
    "/*** Load enum fields ***/\n"
        + "SELECT pg_type.oid, enumlabel\n"
        + "FROM pg_enum\n"
        + "JOIN pg_type ON pg_type.oid=enumtypid\n"
        + "ORDER BY oid, enumsortorder"
  };

  @Test
  public void npgsqlLoadsTheTypes() throws Exception {
    try (Connection c = connect("pguser", PASSWORD, "extended");
        Statement s = c.createStatement()) {
      int rows = 0;
      int lastOrd = 0;
      Map<String, String> kinds = new HashMap<>();
      try (ResultSet rs = s.executeQuery(NPGSQL_TYPE_LOADING[0])) {
        while (rs.next()) {
          rows++;
          assertEquals("pg_catalog", rs.getString("nspname"));
          assertTrue("arrays last", rs.getInt("ord") >= lastOrd);
          lastOrd = rs.getInt("ord");
          kinds.put(rs.getString("typname"), rs.getString("type") + ":" + rs.getLong("elemoid"));
        }
      }
      // 18 base types, their 18 arrays, record, _record and void
      assertEquals(kinds.toString(), 39, rows);
      assertEquals("b:0", kinds.get("timestamptz"));
      assertEquals("a:1184", kinds.get("_timestamptz"));
      assertEquals("a:2249", kinds.get("_record"));
      assertEquals("p:0", kinds.get("void"));
      for (int i = 1; i < NPGSQL_TYPE_LOADING.length; i++) {
        try (ResultSet rs = s.executeQuery(NPGSQL_TYPE_LOADING[i])) {
          assertTrue("no composites or enums", !rs.next());
        }
      }
    }
  }

  /**
   * Every corpus statement that the golden files record as translated runs on the engine.
   * Statements with bound parameters are left to the extended-protocol tests.
   */
  @Test
  public void translatedCorpusStatementsRunOnTheEngine() throws Exception {
    java.nio.file.Path corpus =
        java.nio.file.Paths.get(
                IoTDBPostgreSqlEngineIT.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI())
            .getParent()
            .getParent()
            .resolve("../external-service-impl/postgresql/src/test/resources/corpus")
            .normalize();
    // translated, but the engine cannot run them yet; each with the reason
    Map<String, String> known = new HashMap<>();
    java.util.List<String> failures = new java.util.ArrayList<>();
    int ran = 0;
    try (Connection c = connect("root", ROOT_PASSWORD);
        Statement s = c.createStatement()) {
      // the recordings (and the golden files) were made in UTC
      s.execute("SET TimeZone = 'UTC'");
      java.io.File[] files = corpus.toFile().listFiles((dir, name) -> name.endsWith(".sql"));
      assertTrue("corpus at " + corpus, files != null && files.length > 0);
      java.util.Arrays.sort(files);
      for (java.io.File file : files) {
        String client = file.getName().replace(".sql", "");
        java.util.List<String> statements = new java.util.ArrayList<>();
        StringBuilder current = null;
        for (String line : java.nio.file.Files.readAllLines(file.toPath())) {
          if (line.equals("-- next")) {
            if (current != null) {
              statements.add(current.toString().trim());
            }
            current = new StringBuilder();
          } else if (current != null && !line.startsWith("--")) {
            current.append(line).append('\n');
          }
        }
        if (current != null) {
          statements.add(current.toString().trim());
        }
        for (int i = 0; i < statements.size(); i++) {
          java.util.List<String> expected =
              java.nio.file.Files.readAllLines(
                  corpus.resolve("expected").resolve(client).resolve((i + 1) + ".sql"));
          String outcome = expected.get(expected.size() - 1);
          String statement = statements.get(i);
          // cursor statements need a transaction: sqlCursorsStreamInPortions covers them
          if (outcome.startsWith("--")
              || expected.stream().anyMatch(l -> l.startsWith("-- cursor"))
              || statement.contains("$1")
              || known.containsKey(client + "/" + (i + 1))) {
            continue;
          }
          ran++;
          try (ResultSet rs = s.executeQuery(statement)) {
            while (rs.next()) {
              // drain
            }
          } catch (SQLException e) {
            failures.add(client + "/" + (i + 1) + ": " + e.getMessage());
          }
        }
      }
    }
    assertEquals(String.join("\n", failures), 0, failures.size());
    assertTrue(ran > 5);
  }

  /** psql's {@code \\dn} and the table lookup of {@code \\d refdb.sensors}, from the corpus. */
  @Test
  public void psqlCatalogQueriesReturnTheRightRows() throws Exception {
    java.util.List<String> psql = corpusStatements("psql");
    try (Connection c = connect("root", ROOT_PASSWORD);
        Statement s = c.createStatement()) {
      StringBuilder schemas = new StringBuilder();
      try (ResultSet rs = s.executeQuery(psql.get(1))) {
        while (rs.next()) {
          schemas.append(rs.getString("Name")).append(' ');
        }
      }
      assertEquals("\\dn", "other plant refdb ", schemas.toString());
      try (ResultSet rs = s.executeQuery(psql.get(3))) {
        assertTrue(rs.next());
        assertEquals("refdb", rs.getString("nspname"));
        assertEquals("sensors", rs.getString("relname"));
        long oid = rs.getLong("oid");
        assertTrue(oid >= 16384);
        assertTrue("one table", !rs.next());
        // \d then lists the columns by that OID (corpus statement 6 used PostgreSQL's OID)
        String columns = psql.get(5).replace("'16386'", "'" + oid + "'");
        StringBuilder names = new StringBuilder();
        try (ResultSet cols = s.executeQuery(columns)) {
          while (cols.next()) {
            names.append(cols.getString(1)).append(':').append(cols.getString(2)).append(' ');
          }
        }
        assertTrue(
            names.toString(),
            names
                .toString()
                .startsWith(
                    "time:timestamp with time zone plant:text line:text machine:text model:text"
                        + " temperature:double precision pressure:real count:bigint level:integer"
                        + " running:boolean "));
      }
    }
  }

  private static java.util.List<String> corpusStatements(String client) throws Exception {
    java.nio.file.Path file =
        java.nio.file.Paths.get(
                IoTDBPostgreSqlEngineIT.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI())
            .getParent()
            .getParent()
            .resolve(
                "../external-service-impl/postgresql/src/test/resources/corpus/" + client + ".sql")
            .normalize();
    java.util.List<String> statements = new java.util.ArrayList<>();
    StringBuilder current = null;
    for (String line : java.nio.file.Files.readAllLines(file)) {
      if (line.equals("-- next")) {
        if (current != null) {
          statements.add(current.toString().trim());
        }
        current = new StringBuilder();
      } else if (current != null && !line.startsWith("--")) {
        current.append(line).append('\n');
      }
    }
    if (current != null) {
      statements.add(current.toString().trim());
    }
    return statements;
  }

  /** date_trunc, EXTRACT and intervals give PostgreSQL's results in Europe/Berlin. */
  @Test
  public void dateRulesMatchPostgreSqlAcrossDaylightSavingTime() throws Exception {
    java.time.ZoneId berlin = java.time.ZoneId.of("Europe/Berlin");
    try (Connection c = connect("root", ROOT_PASSWORD);
        Statement s = c.createStatement()) {
      s.execute("SET TimeZone = 'Europe/Berlin'");
      try (ResultSet rs =
          s.executeQuery(
              "SELECT time, date_trunc('day', time), date_trunc('month', time),"
                  + " date_trunc('quarter', time), date_trunc('year', time),"
                  + " date_trunc('hour', time), extract(dow from time), extract(second from time),"
                  + " extract(epoch from time), date_part('doy', time), extract(hour from time)"
                  + " FROM refdb.dates ORDER BY time")) {
        int rows = 0;
        while (rs.next()) {
          rows++;
          java.time.ZonedDateTime t = rs.getTimestamp(1).toInstant().atZone(berlin);
          String at = t.toString();
          java.time.ZonedDateTime day = t.toLocalDate().atStartOfDay(berlin);
          assertEquals(at, day.toInstant(), rs.getTimestamp(2).toInstant());
          assertEquals(at, day.withDayOfMonth(1).toInstant(), rs.getTimestamp(3).toInstant());
          java.time.ZonedDateTime quarter =
              t.toLocalDate()
                  .withDayOfMonth(1)
                  .withMonth((t.getMonthValue() - 1) / 3 * 3 + 1)
                  .atStartOfDay(berlin);
          assertEquals(at, quarter.toInstant(), rs.getTimestamp(4).toInstant());
          assertEquals(
              at,
              t.toLocalDate().withDayOfYear(1).atStartOfDay(berlin).toInstant(),
              rs.getTimestamp(5).toInstant());
          assertEquals(
              at,
              t.truncatedTo(java.time.temporal.ChronoUnit.HOURS).toInstant(),
              rs.getTimestamp(6).toInstant());
          assertEquals(at, t.getDayOfWeek().getValue() % 7, rs.getLong(7));
          assertEquals(at, t.getSecond() + t.getNano() / 1e9, rs.getDouble(8), 1e-9);
          assertEquals(at, t.toInstant().toEpochMilli() / 1000.0, rs.getDouble(9), 1e-9);
          assertEquals(at, t.getDayOfYear(), rs.getDouble(10), 0);
          assertEquals(at, t.getHour(), rs.getLong(11));
        }
        assertEquals(6, rows);
      }
      // GROUP BY a truncated time, as Power BI's date hierarchy does
      try (ResultSet rs =
          s.executeQuery(
              "SELECT date_trunc('month', time) AS m, count(*) FROM refdb.dates"
                  + " GROUP BY date_trunc('month', time) ORDER BY m")) {
        StringBuilder months = new StringBuilder();
        while (rs.next()) {
          months
              .append(rs.getTimestamp(1).toInstant().atZone(berlin).toLocalDate())
              .append('=')
              .append(rs.getLong(2))
              .append(' ');
        }
        assertEquals("2026-01-01=1 2026-03-01=2 2026-10-01=2 2027-01-01=1 ", months.toString());
      }
      try {
        s.executeQuery("SELECT now() - interval '7 days'");
        fail("days have no fixed length in Europe/Berlin");
      } catch (SQLException e) {
        assertEquals("0A000", e.getSQLState());
      }
      s.execute("SET TimeZone = 'UTC'");
      try (ResultSet rs =
          s.executeQuery(
              "SELECT count(*) FROM refdb.dates WHERE time >= TIMESTAMP '2026-10-25 00:00:00'"
                  + " - interval '1 hour 30 minutes'")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
      }
      try (ResultSet rs =
          s.executeQuery(
              "SELECT v, s || '-x', concat(s, '-y') FROM refdb.dates WHERE v IN (1, 2)"
                  + " ORDER BY v")) {
        assertTrue(rs.next());
        assertEquals("s1-x", rs.getString(2));
        assertEquals("s1-y", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("NULL || text is NULL", null, rs.getString(2));
        assertEquals("concat skips NULL", "-y", rs.getString(3));
      }
    }
  }

  /** pgJDBC's DatabaseMetaData, as DBeaver and other JDBC tools use it. */
  @Test
  public void pgJdbcDatabaseMetaData() throws Exception {
    try (Connection c = connect("pguser", PASSWORD, "extended")) {
      java.sql.DatabaseMetaData meta = c.getMetaData();
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
          columns
              .append(rs.getString("COLUMN_NAME"))
              .append(':')
              .append(rs.getString("TYPE_NAME"))
              .append(':')
              .append(rs.getInt("DATA_TYPE"))
              .append(':')
              .append(rs.getInt("ORDINAL_POSITION"))
              .append(':')
              .append(rs.getString("REMARKS"))
              .append(' ');
        }
      }
      assertEquals(
          "time:timestamptz:93:1:TIME device:text:12:2:TAG temp:float8:8:3:FIELD"
              + " ok:bool:-7:4:FIELD n:int4:4:5:FIELD label:text:12:6:FIELD ",
          columns.toString());
    }
  }

  private static final String SLOW =
      "SELECT count(*) FROM refdb.many a, refdb.many b, refdb.many c WHERE a.v + b.v + c.v = -1";

  /** CancelRequest from a second connection stops a running query with 57014. */
  @Test
  public void cancelRequestStopsARunningQuery() throws Exception {
    try (Connection c = connect("root", ROOT_PASSWORD, "extended");
        Statement s = c.createStatement()) {
      java.util.concurrent.ScheduledExecutorService timer =
          java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
      try {
        timer.schedule(
            () -> {
              try {
                s.cancel();
              } catch (SQLException e) {
                throw new IllegalStateException(e);
              }
            },
            1,
            java.util.concurrent.TimeUnit.SECONDS);
        long start = System.nanoTime();
        try {
          s.executeQuery(SLOW);
          fail("cancelled");
        } catch (SQLException e) {
          assertEquals(e.getMessage(), "57014", e.getSQLState());
        }
        long seconds = (System.nanoTime() - start) / 1_000_000_000L;
        assertTrue("stopped within a few seconds, took " + seconds, seconds < 10);
      } finally {
        timer.shutdownNow();
      }
      // the connection is usable afterwards
      try (ResultSet rs = s.executeQuery("SELECT count(*) FROM refdb.many")) {
        assertTrue(rs.next());
        assertEquals(2000L, rs.getLong(1));
      }
      // statement_timeout ends a query the same way
      s.execute("SET statement_timeout = '1s'");
      long start = System.nanoTime();
      try {
        s.executeQuery(SLOW);
        fail("timed out");
      } catch (SQLException e) {
        assertEquals(e.getMessage(), "57014", e.getSQLState());
      }
      assertTrue((System.nanoTime() - start) / 1_000_000_000L < 10);
    }
  }

  /** psycopg's server-side cursors: DECLARE, FETCH, MOVE, CLOSE in a transaction. */
  @Test
  public void sqlCursorsStreamInPortions() throws Exception {
    for (String mode : new String[] {"simple", "extended"}) {
      try (Connection c = connect("pguser", PASSWORD, mode);
          Statement s = c.createStatement()) {
        try {
          s.execute("DECLARE outside CURSOR FOR SELECT 1");
          fail("only in a transaction block");
        } catch (SQLException e) {
          assertEquals("25P01", e.getSQLState());
        }
        s.execute("BEGIN");
        s.execute("DECLARE \"big\" CURSOR FOR SELECT time, temp FROM plant.sensors ORDER BY time");
        assertEquals(mode, "1000:20.5 2000:21.5 ", fetch(s, "FETCH FORWARD 2 FROM \"big\""));
        assertEquals(mode, "3000:19 ", fetch(s, "FETCH FORWARD 2 FROM \"big\""));
        assertEquals(mode, "", fetch(s, "FETCH ALL FROM \"big\""));
        s.execute("CLOSE \"big\"");
        try {
          s.executeQuery("FETCH 1 FROM \"big\"");
          fail("closed");
        } catch (SQLException e) {
          assertEquals("34000", e.getSQLState());
        }
        s.execute("ROLLBACK");

        s.execute("BEGIN");
        s.execute("DECLARE c2 CURSOR FOR SELECT time FROM plant.sensors ORDER BY time");
        s.execute("MOVE FORWARD 2 IN c2");
        assertEquals(mode, "3000 ", fetch(s, "FETCH NEXT FROM c2"));
        s.execute("COMMIT");
        try {
          s.executeQuery("FETCH 1 FROM c2");
          fail("a cursor without HOLD ends with the transaction");
        } catch (SQLException e) {
          assertEquals("34000", e.getSQLState());
        }

        s.execute("BEGIN");
        s.execute("DECLARE kept CURSOR WITH HOLD FOR SELECT time FROM plant.sensors ORDER BY time");
        s.execute("COMMIT");
        assertEquals(mode, "1000 ", fetch(s, "FETCH 1 FROM kept"));
        s.execute("CLOSE ALL");
      }
    }
  }

  private static String fetch(Statement s, String sql) throws SQLException {
    StringBuilder rows = new StringBuilder();
    try (ResultSet rs = s.executeQuery(sql)) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        rows.append(rs.getTimestamp(1).getTime());
        for (int i = 2; i <= columns; i++) {
          rows.append(':').append(rs.getString(i));
        }
        rows.append(' ');
      }
    }
    return rows.toString();
  }

  @Test
  public void theCatalogShowsOnlyWhatTheUserMayRead() throws Exception {
    try (Connection c = connect("pguser", PASSWORD);
        Statement s = c.createStatement()) {
      Map<String, String> tables = new HashMap<>();
      try (ResultSet rs =
          s.executeQuery(
              "SELECT table_schema, table_name, table_type FROM information_schema.tables"
                  + " WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")) {
        while (rs.next()) {
          tables.put(rs.getString(1) + "." + rs.getString(2), rs.getString(3));
        }
      }
      assertEquals("{plant.sensors=BASE TABLE}", tables.toString());

      StringBuilder columns = new StringBuilder();
      try (ResultSet rs =
          s.executeQuery(
              "SELECT a.attname, t.typname, a.attnotnull FROM pg_catalog.pg_attribute a"
                  + " JOIN pg_catalog.pg_class c ON a.attrelid = c.oid"
                  + " JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid"
                  + " JOIN pg_catalog.pg_type t ON a.atttypid = t.oid"
                  + " WHERE n.nspname = 'plant' AND c.relname = 'sensors' ORDER BY a.attnum")) {
        while (rs.next()) {
          columns.append(rs.getString(1)).append(':').append(rs.getString(2));
          columns.append(rs.getBoolean(3) ? "! " : " ");
        }
      }
      assertEquals(
          "time:timestamptz! device:text! temp:float8 ok:bool n:int4 label:text ",
          columns.toString());

      try (ResultSet rs =
          s.executeQuery("SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname")) {
        StringBuilder names = new StringBuilder();
        while (rs.next()) {
          names.append(rs.getString(1)).append(' ');
        }
        assertEquals("information_schema pg_catalog plant ", names.toString());
      }
    }
  }

  @AfterClass
  public static void stop() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Connection connect(String user, String password) throws SQLException {
    return connect(user, password, "simple");
  }

  private static Connection connect(String user, String password, String queryMode)
      throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("preferQueryMode", queryMode);
    props.setProperty("ApplicationName", "pg-it");
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/anydb", props);
  }

  @Test
  public void userLogsInAndUsesSessionCommands() throws Exception {
    try (Connection c = connect("pguser", PASSWORD);
        Statement s = c.createStatement()) {
      s.execute("SET application_name = 'pg-it-2'");
      s.execute("BEGIN");
      s.execute("COMMIT");
      try (ResultSet rs = s.executeQuery("SHOW server_version")) {
        assertTrue(rs.next());
        assertTrue(rs.getString(1), rs.getString(1).startsWith("14.0 (IoTDB "));
      }
      try (ResultSet rs = s.executeQuery("SHOW application_name")) {
        assertTrue(rs.next());
        assertEquals("pg-it-2", rs.getString(1));
      }
      s.execute("SET TimeZone = 'Europe/Berlin'");
      try (ResultSet rs = s.executeQuery("SHOW TimeZone")) {
        assertTrue(rs.next());
        assertEquals("Europe/Berlin", rs.getString(1));
      }
      String startZone = java.time.ZoneId.systemDefault().getId();
      s.execute("SET TimeZone = 'Pacific/Auckland'");
      s.execute("RESET TimeZone");
      try (ResultSet rs = s.executeQuery("SHOW TimeZone")) {
        assertTrue(rs.next());
        assertEquals("back to the zone of the start", startZone, rs.getString(1));
      }
      s.execute("SET statement_timeout = '30s'");
      try (ResultSet rs = s.executeQuery("SELECT count(*) FROM plant.sensors")) {
        assertTrue(rs.next());
      }
      s.execute("RESET ALL");
      try (ResultSet rs = s.executeQuery("SHOW statement_timeout")) {
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
      }
      try {
        s.execute("SET statement_timeout = 'soon'");
        fail("invalid timeout");
      } catch (SQLException e) {
        assertEquals("22023", e.getSQLState());
      }
      try {
        s.execute("SET client_encoding = 'LATIN1'");
        fail("only UTF8 is accepted");
      } catch (SQLException e) {
        assertEquals("0A000", e.getSQLState());
      }
    }
  }

  @Test
  public void queriesRunThroughTheSimpleProtocol() throws Exception {
    try (Connection c = connect("pguser", PASSWORD);
        Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT time, device, temp, ok, n, label FROM plant.sensors"
                    + " WHERE device = 'd1' ORDER BY time")) {
      assertEquals("timestamptz", rs.getMetaData().getColumnTypeName(1));
      assertEquals("float8", rs.getMetaData().getColumnTypeName(3));
      assertTrue(rs.next());
      assertEquals(1000L, rs.getTimestamp(1).getTime());
      assertEquals("d1", rs.getString(2));
      assertEquals(20.5, rs.getDouble(3), 0.0);
      assertTrue(rs.getBoolean(4));
      assertEquals(1, rs.getInt(5));
      assertEquals("a", rs.getString(6));
      assertTrue(rs.next());
      assertEquals(2000L, rs.getTimestamp(1).getTime());
      assertTrue(!rs.next());
    }
  }

  @Test
  public void queriesRunThroughTheExtendedProtocolWithParameters() throws Exception {
    try (Connection c = connect("pguser", PASSWORD, "extended");
        java.sql.PreparedStatement p =
            c.prepareStatement(
                "SELECT device, avg(temp) AS t, count(*) FROM plant.sensors"
                    + " WHERE time >= ? AND device <> ? GROUP BY device ORDER BY device")) {
      for (int round = 0; round < 6; round++) {
        // pgJDBC switches to a named server-side statement and binary results after 5 runs
        p.setTimestamp(1, new java.sql.Timestamp(1000L));
        p.setString(2, "x");
        try (ResultSet rs = p.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("d1", rs.getString(1));
          assertEquals(21.0, rs.getDouble(2), 1e-9);
          assertEquals(2L, rs.getLong(3));
          assertTrue(rs.next());
          assertEquals("d2", rs.getString(1));
          assertTrue(!rs.next());
        }
      }
    }
  }

  @Test
  public void fetchSizeStreamsInPortions() throws Exception {
    try (Connection c = connect("pguser", PASSWORD, "extended")) {
      c.setAutoCommit(false); // pgJDBC uses portals with a row limit only inside a transaction
      try (Statement s = c.createStatement()) {
        s.setFetchSize(1);
        try (ResultSet rs = s.executeQuery("SELECT time FROM plant.sensors ORDER BY time")) {
          int rows = 0;
          while (rs.next()) {
            rows++;
          }
          assertEquals(3, rows);
        }
      }
      c.commit();
    }
  }

  @Test
  public void untranslatableAndWritingStatementsAreRefused() throws Exception {
    try (Connection c = connect("pguser", PASSWORD);
        Statement s = c.createStatement()) {
      try {
        s.executeQuery("SELECT DISTINCT ON (device) * FROM plant.sensors");
        fail("refused");
      } catch (SQLException e) {
        assertEquals("0A000", e.getSQLState());
        assertTrue(e.getMessage(), e.getMessage().contains("DISTINCT ON"));
      }
      try {
        s.execute("INSERT INTO plant.sensors (time, device) VALUES (5, 'x')");
        fail("refused");
      } catch (SQLException e) {
        assertEquals("25006", e.getSQLState());
      }
      try {
        s.executeQuery("SELECT * FROM plant.nothing_here");
        fail("refused");
      } catch (SQLException e) {
        // the engine checks the privilege before the table exists, so a user without the
        // privilege cannot tell absent tables from hidden ones
        assertEquals(e.getMessage(), "42501", e.getSQLState());
      }
      // the connection still works after errors
      try (ResultSet rs = s.executeQuery("SELECT count(*) FROM plant.sensors")) {
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
      }
    }
  }

  /** Prepared statements with parameters of every type. */
  @Test
  public void preparedStatementsWithEveryParameterType() throws Exception {
    try (Connection c = connect("root", ROOT_PASSWORD, "extended");
        java.sql.PreparedStatement p =
            c.prepareStatement(
                "SELECT n, big, label, raw, day FROM refdb.ptypes WHERE device = ? AND ok = ?"
                    + " AND n >= ? AND big >= ? AND f >= ? AND temp >= ? AND day >= ?"
                    + " AND time >= ? ORDER BY time LIMIT 1")) {
      for (int round = 0; round < 6; round++) {
        p.setString(1, "d0");
        p.setBoolean(2, true);
        p.setInt(3, 6);
        p.setLong(4, 60_000_000_000L);
        p.setFloat(5, 3.0f);
        p.setDouble(6, 1.5);
        p.setObject(7, java.time.LocalDate.of(2026, 1, 1));
        p.setTimestamp(8, new java.sql.Timestamp(1000));
        try (ResultSet rs = p.executeQuery()) {
          assertTrue("round " + round, rs.next());
          assertEquals(6, rs.getInt(1));
          assertEquals(60_000_000_000L, rs.getLong(2));
          assertEquals("l6", rs.getString(3));
          assertEquals(
              java.time.LocalDate.of(2026, 1, 7), rs.getObject(5, java.time.LocalDate.class));
        }
      }
    }
  }
}
