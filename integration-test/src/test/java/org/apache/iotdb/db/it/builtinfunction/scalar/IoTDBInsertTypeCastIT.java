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

package org.apache.iotdb.db.it.builtinfunction.scalar;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringJoiner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
public class IoTDBInsertTypeCastIT {

  private static final List<TSDataType> MATRIX_TYPES =
      Arrays.asList(
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.DATE,
          TSDataType.TIMESTAMP,
          TSDataType.BLOB);

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testGlobalSwitchOnEveryDataNode() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String device = "root.insert_cast_switch.d";
      statement.execute("CREATE DATABASE root.insert_cast_switch");
      statement.execute("CREATE TIMESERIES " + device + ".i WITH DATATYPE=INT32");
      try {
        for (int phase = 0; phase < 3; phase++) {
          boolean enabled = phase == 1;
          statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='" + enabled + "'");
          int nodeIndex = 0;
          for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
            try (Connection nodeConnection =
                    EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node);
                Statement nodeStatement = nodeConnection.createStatement()) {
              long time = phase * 100L + nodeIndex++;
              String insert = "INSERT INTO " + device + "(time, i) VALUES (" + time + ", '5')";
              if (enabled) {
                nodeStatement.execute(insert);
              } else {
                assertThrows(SQLException.class, () -> nodeStatement.execute(insert));
              }
              try (ResultSet result =
                  nodeStatement.executeQuery("SELECT i FROM " + device + " WHERE time = " + time)) {
                assertEquals(enabled, result.next());
                if (enabled) {
                  assertEquals(5, result.getInt(2));
                  assertFalse(result.next());
                }
              }
            }
          }
        }
      } finally {
        statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='false'");
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testInsertConversionMatchesQueryCast() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      statement.execute("CREATE DATABASE root.insert_cast");
      statement.execute(
          "CREATE TIMESERIES root.insert_cast.d.text_value WITH DATATYPE=TEXT, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.insert_cast.d.double_value WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.insert_cast.d.actual WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.insert_cast.d.other WITH DATATYPE=INT32, ENCODING=PLAIN");

      Tablet tablet =
          new Tablet(
              "root.insert_cast.d",
              Arrays.asList(
                  new MeasurementSchema("actual", TSDataType.TEXT),
                  new MeasurementSchema("other", TSDataType.INT32)),
              2);
      tablet.addTimestamp(0, 3);
      tablet.addValue("actual", 0, "5");
      tablet.addValue("other", 0, 1);
      assertThrows(StatementExecutionException.class, () -> session.insertTablet(tablet));

      statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='true'");
      statement.execute(
          "INSERT INTO root.insert_cast.d(time, text_value, actual) VALUES (1, '5', '5')");
      statement.execute(
          "INSERT INTO root.insert_cast.d(time, text_value, actual) VALUES (2, '5', \"5\")");
      statement.execute("INSERT INTO root.insert_cast.d(time, text_value) VALUES (3, '5')");
      session.insertTablet(tablet);
      session.insertRecord(
          "root.insert_cast.d",
          4,
          Arrays.asList("double_value", "actual"),
          Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
          Arrays.asList(5.6, 5.6));

      assertMatchesQueryCast(statement, "text_value", "time <= 3", 3, 5);
      assertMatchesQueryCast(statement, "double_value", "time = 4", 1, 6);

      tablet.reset();
      tablet.addTimestamp(0, 5);
      tablet.addValue("actual", 0, "6");
      tablet.addValue("other", 0, 1);
      tablet.addTimestamp(1, 6);
      tablet.addValue("actual", 1, "bad");
      tablet.addValue("other", 1, 2);
      StatementExecutionException failure =
          assertThrows(StatementExecutionException.class, () -> session.insertTablet(tablet));
      assertTrue(failure.getMessage().contains("bad"));
      try (ResultSet result =
          statement.executeQuery("SELECT actual, other FROM root.insert_cast.d WHERE time >= 5")) {
        for (int row = 1; row <= 2; row++) {
          assertTrue(result.next());
          assertNull(result.getObject(2));
          assertEquals(row, result.getInt(3));
        }
        assertFalse(result.next());
      }

      statement.execute("SET CONFIGURATION 'enable_partial_insert'='false'");
      try {
        assertThrows(
            SQLException.class,
            () ->
                statement.execute(
                    "INSERT INTO root.insert_cast.d(time, actual, other) VALUES (7, 'bad', 3)"));
        try (ResultSet result =
            statement.executeQuery("SELECT other FROM root.insert_cast.d WHERE time = 7")) {
          assertFalse(result.next());
        }
      } finally {
        statement.execute("SET CONFIGURATION 'enable_partial_insert'='true'");
        statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='false'");
      }
      assertThrows(
          SQLException.class,
          () -> statement.execute("INSERT INTO root.insert_cast.d(time, actual) VALUES (7, '5')"));
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testStringRecordsAndTabletTimestampOverflow() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String device = "root.insert_cast_edges.d";
      statement.execute("CREATE DATABASE root.insert_cast_edges");
      statement.execute("CREATE TIMESERIES " + device + ".i WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES " + device + ".other WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES " + device + ".stamp WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN");
      statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='true'");
      try {
        assertThrows(
            StatementExecutionException.class,
            () ->
                session.insertRecord(
                    device, 1, Arrays.asList("i", "other"), Arrays.asList("5.6", "1")));
        statement.execute("INSERT INTO " + device + "(time, i, other) VALUES (2, 5.6, 2)");

        Tablet tablet =
            new Tablet(
                device,
                Arrays.asList(
                    new MeasurementSchema("stamp", TSDataType.TEXT),
                    new MeasurementSchema("other", TSDataType.INT32)),
                2);
        tablet.addTimestamp(0, 3);
        tablet.addValue("stamp", 0, "2026-01-01T00:00:00+00:00");
        tablet.addValue("other", 0, 3);
        tablet.addTimestamp(1, 4);
        tablet.addValue("stamp", 1, "999999999-01-01T00:00:00+00:00");
        tablet.addValue("other", 1, 4);
        StatementExecutionException failure =
            assertThrows(StatementExecutionException.class, () -> session.insertTablet(tablet));
        assertTrue(failure.getMessage(), failure.getMessage().contains("long overflow"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("stamp"));

        try (ResultSet result =
            statement.executeQuery("SELECT i, stamp, other FROM " + device + " ORDER BY time")) {
          for (int time = 1; time <= 4; time++) {
            assertTrue(result.next());
            assertEquals(time, result.getLong(1));
            if (time == 2) {
              assertEquals(6, result.getInt(2));
            } else {
              assertNull(result.getObject(2));
            }
            assertNull(result.getObject(3));
            assertEquals(time, result.getInt(4));
          }
          assertFalse(result.next());
        }
      } finally {
        statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='false'");
      }
    }
  }

  @Test
  @Category({TableLocalStandaloneIT.class, TableClusterIT.class})
  public void testTableInsertConversionMatchesQueryCast() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      statement.execute("CREATE DATABASE insert_cast");
      statement.execute("USE insert_cast");
      session.executeNonQueryStatement("USE insert_cast");
      statement.execute(
          "CREATE TABLE readings(device STRING TAG, text_value STRING FIELD, actual INT32 FIELD)");
      statement.execute("SET CONFIGURATION enable_insert_type_cast='true'");
      try {
        Tablet tablet =
            new Tablet(
                "readings",
                Arrays.asList("device", "text_value", "actual"),
                Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
                1);
        tablet.addTimestamp(0, 1);
        tablet.addValue("device", 0, "d1");
        tablet.addValue("text_value", 0, "5");
        tablet.addValue("actual", 0, "5");
        session.insert(tablet);
        statement.execute("INSERT INTO readings VALUES (2, 'd1', '6', 5.6)");

        try (ResultSet result =
            statement.executeQuery(
                "SELECT CAST(text_value AS INT32), actual FROM readings ORDER BY time")) {
          for (int expected = 5; expected <= 6; expected++) {
            assertTrue(result.next());
            assertEquals(expected, result.getInt(1));
            assertEquals(result.getInt(1), result.getInt(2));
            assertFalse(result.wasNull());
          }
          assertFalse(result.next());
        }
      } finally {
        statement.execute("SET CONFIGURATION enable_insert_type_cast='false'");
      }
    }
  }

  @Test
  @Category({TableLocalStandaloneIT.class, TableClusterIT.class})
  public void testSqlCastPreservesSourceType() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE insert_cast_source");
      statement.execute("USE insert_cast_source");
      statement.execute(
          "CREATE TABLE readings(device STRING TAG, i INT32 FIELD, b BLOB FIELD, day DATE FIELD)");
      statement.execute("SET CONFIGURATION enable_insert_type_cast='true'");
      try {
        assertThrows(
            SQLException.class,
            () ->
                statement.execute("INSERT INTO readings(time, device, i) VALUES (1, 'd', X'35')"));
        assertThrows(
            SQLException.class,
            () ->
                statement.execute("INSERT INTO readings(time, device, i) VALUES (2, 'd', 'NULL')"));
        assertThrows(
            SQLException.class,
            () ->
                statement.execute(
                    "INSERT INTO readings(time, device, day) VALUES (3, 'd', 9990101)"));
        statement.execute("INSERT INTO readings VALUES (4, 'd', '5', 5, 20260228)");
        statement.execute(
            "INSERT INTO readings(time, device, day) VALUES (5, 'd', CAST('2026-03-01' AS DATE))");
        assertThrows(
            SQLException.class,
            () ->
                statement.execute(
                    "INSERT INTO readings(time, device, day) VALUES (6, 'd', CAST(20260230 AS DATE))"));
        try (ResultSet result =
            statement.executeQuery(
                "SELECT time, i, b, CAST(i AS BLOB), day FROM readings ORDER BY time")) {
          assertTrue(result.next());
          assertEquals(4, result.getTimestamp(1).getTime());
          assertEquals(5, result.getInt(2));
          assertArrayEquals(new byte[] {0, 0, 0, 5}, result.getBytes(3));
          assertArrayEquals(result.getBytes(4), result.getBytes(3));
          assertEquals("2026-02-28", result.getString(5));
          assertTrue(result.next());
          assertEquals(5, result.getTimestamp(1).getTime());
          assertEquals("2026-03-01", result.getString(5));
          assertFalse(result.next());
        }
      } finally {
        statement.execute("SET CONFIGURATION enable_insert_type_cast='false'");
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testTimestampToTextMatchesQueryCast() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String device = "root.insert_cast_timestamp.d";
      statement.execute("CREATE DATABASE root.insert_cast_timestamp");
      statement.execute("CREATE TIMESERIES " + device + ".source WITH DATATYPE=TIMESTAMP");
      statement.execute("CREATE TIMESERIES " + device + ".text_value WITH DATATYPE=TEXT");
      statement.execute("CREATE TIMESERIES " + device + ".string_value WITH DATATYPE=STRING");
      session.setTimeZone("UTC");
      connection.setClientInfo("time_zone", "UTC");
      List<String> names = Arrays.asList("source", "text_value", "string_value");
      List<TSDataType> types =
          Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP);
      Tablet tablet =
          new Tablet(
              device,
              Arrays.asList(
                  new MeasurementSchema("source", TSDataType.TIMESTAMP),
                  new MeasurementSchema("text_value", TSDataType.TIMESTAMP),
                  new MeasurementSchema("string_value", TSDataType.TIMESTAMP)),
              1);
      try {
        for (int phase = 0; phase < 3; phase++) {
          boolean enabled = phase == 1;
          statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='" + enabled + "'");
          session.insertRecord(device, phase * 2 + 1, names, types, Arrays.asList(5L, 5L, 5L));
          tablet.reset();
          tablet.addTimestamp(0, phase * 2 + 2);
          for (String name : names) {
            tablet.addValue(name, 0, 5L);
          }
          session.insertTablet(tablet);
        }
        statement.execute("FLUSH");
        try (ResultSet result =
            statement.executeQuery(
                "SELECT text_value, string_value, CAST(source AS TEXT), CAST(source AS STRING) FROM "
                    + device
                    + " ORDER BY time")) {
          for (int time = 1; time <= 6; time++) {
            assertTrue(result.next());
            assertEquals(time, result.getLong(1));
            assertNotNull(result.getString(4));
            assertNotNull(result.getString(5));
            assertNotEquals("5", result.getString(4));
            assertNotEquals("5", result.getString(5));
            boolean enabled = time == 3 || time == 4;
            assertEquals(enabled ? result.getString(4) : "5", result.getString(2));
            assertEquals(enabled ? result.getString(5) : "5", result.getString(3));
          }
          assertFalse(result.next());
        }
      } finally {
        statement.execute("SET CONFIGURATION 'enable_insert_type_cast'='false'");
      }
    }
  }

  @Test
  @Category({TableLocalStandaloneIT.class, TableClusterIT.class})
  public void testCompleteInsertCastMatrix() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      statement.execute("CREATE DATABASE insert_cast_matrix");
      statement.execute("USE insert_cast_matrix");
      session.executeNonQueryStatement("USE insert_cast_matrix");

      StringJoiner fields = new StringJoiner(", ");
      fields.add("device STRING TAG");
      for (TSDataType type : MATRIX_TYPES) {
        fields.add(matrixColumn("source", type) + " " + type + " FIELD");
        fields.add(matrixColumn("target", type) + " " + type + " FIELD");
      }
      statement.execute("CREATE TABLE matrix (" + fields + ")");
      statement.execute("SET CONFIGURATION enable_insert_type_cast='true'");
      try {
        List<CastMatrixSample> samples = castMatrixSamples();
        for (int i = 0; i < samples.size(); i++) {
          writeMatrixSample(session, i + 1, samples.get(i));
        }
        statement.execute("FLUSH");
        for (int i = 0; i < samples.size(); i++) {
          assertMatrixSample(statement, i + 1, samples.get(i));
        }
      } finally {
        statement.execute("SET CONFIGURATION enable_insert_type_cast='false'");
      }
    }
  }

  private static List<CastMatrixSample> castMatrixSamples() {
    Set<TSDataType> allTypes = EnumSet.copyOf(MATRIX_TYPES);
    Set<TSDataType> booleanTargets = EnumSet.copyOf(allTypes);
    booleanTargets.remove(TSDataType.DATE);
    List<CastMatrixSample> samples =
        new ArrayList<>(
            Arrays.asList(
                new CastMatrixSample(TSDataType.INT32, 20260228, allTypes),
                new CastMatrixSample(TSDataType.INT64, 20260228L, allTypes),
                new CastMatrixSample(TSDataType.FLOAT, 20260228.0F, allTypes),
                new CastMatrixSample(TSDataType.DOUBLE, 20260228.25D, allTypes),
                new CastMatrixSample(TSDataType.BOOLEAN, true, booleanTargets),
                new CastMatrixSample(TSDataType.DATE, LocalDate.of(2026, 2, 28), allTypes),
                new CastMatrixSample(TSDataType.TIMESTAMP, 5L, allTypes),
                new CastMatrixSample(
                    TSDataType.BLOB,
                    new Binary("5", StandardCharsets.UTF_8),
                    EnumSet.of(TSDataType.BLOB, TSDataType.TEXT, TSDataType.STRING))));
    for (TSDataType text : Arrays.asList(TSDataType.TEXT, TSDataType.STRING)) {
      samples.add(
          new CastMatrixSample(
              text,
              "5",
              EnumSet.of(
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE,
                  TSDataType.TEXT,
                  TSDataType.STRING,
                  TSDataType.BLOB)));
      samples.add(
          new CastMatrixSample(
              text,
              "true",
              EnumSet.of(TSDataType.BOOLEAN, TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB)));
      samples.add(
          new CastMatrixSample(
              text,
              "2026-02-28",
              EnumSet.of(
                  TSDataType.DATE,
                  TSDataType.TIMESTAMP,
                  TSDataType.TEXT,
                  TSDataType.STRING,
                  TSDataType.BLOB)));
    }
    return samples;
  }

  private static void writeMatrixSample(ITableSession session, int time, CastMatrixSample sample)
      throws Exception {
    List<String> columns = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    columns.add("device");
    types.add(TSDataType.STRING);
    categories.add(ColumnCategory.TAG);

    columns.add(matrixColumn("source", sample.source()));
    for (TSDataType target : MATRIX_TYPES) {
      columns.add(matrixColumn("target", target));
    }
    for (int column = 1; column < columns.size(); column++) {
      types.add(sample.source());
      categories.add(ColumnCategory.FIELD);
    }

    Tablet tablet = new Tablet("matrix", columns, types, categories, 1);
    tablet.addTimestamp(0, time);
    tablet.addValue("device", 0, "d");
    for (int column = 1; column < columns.size(); column++) {
      tablet.addValue(columns.get(column), 0, sample.value());
    }
    if (sample.targets().containsAll(MATRIX_TYPES)) {
      session.insert(tablet);
      return;
    }
    StatementExecutionException failure =
        assertThrows(
            sample.toString(), StatementExecutionException.class, () -> session.insert(tablet));
    for (TSDataType target : MATRIX_TYPES) {
      if (!sample.targets().contains(target)) {
        assertTrue(
            sample + " -> " + target + " was not reported: " + failure.getMessage(),
            failure.getMessage().contains(matrixColumn("target", target)));
      }
    }
  }

  private static void assertMatrixSample(Statement statement, int time, CastMatrixSample sample)
      throws SQLException {
    String sourceColumn = matrixColumn("source", sample.source());
    StringJoiner projections = new StringJoiner(", ");
    for (TSDataType target : MATRIX_TYPES) {
      String targetColumn = matrixColumn("target", target);
      projections.add(targetColumn);
      if (sample.targets().contains(target)) {
        projections.add(
            target == sample.source()
                ? sourceColumn
                : "CAST(" + sourceColumn + " AS " + target + ")");
      }
    }
    try (ResultSet result =
        statement.executeQuery("SELECT " + projections + " FROM matrix WHERE time = " + time)) {
      assertTrue(sample.toString(), result.next());
      int column = 1;
      for (TSDataType target : MATRIX_TYPES) {
        String description = sample + " -> " + target;
        int actual = column++;
        if (!sample.targets().contains(target)) {
          assertNull(description, result.getObject(actual));
        } else if (target == TSDataType.BLOB) {
          byte[] expected = result.getBytes(column++);
          assertNotNull(description, expected);
          assertArrayEquals(description, expected, result.getBytes(actual));
        } else {
          String queryCast = result.getString(column++);
          String inserted = result.getString(actual);
          assertNotNull(description, queryCast);
          assertEquals(description, queryCast, inserted);
        }
      }
      assertFalse(sample.toString(), result.next());
    }
  }

  private static String matrixColumn(String prefix, TSDataType type) {
    return prefix + "_" + type.name().toLowerCase(Locale.ROOT);
  }

  private record CastMatrixSample(TSDataType source, Object value, Set<TSDataType> targets) {}

  private static void assertMatchesQueryCast(
      Statement statement, String source, String filter, int count, int expected)
      throws SQLException {
    try (ResultSet result =
        statement.executeQuery(
            "SELECT CAST("
                + source
                + " AS INT32), actual FROM root.insert_cast.d WHERE "
                + filter)) {
      for (int row = 0; row < count; row++) {
        assertTrue(result.next());
        assertEquals(expected, result.getInt(2));
        assertEquals(result.getInt(2), result.getInt(3));
        assertFalse(result.wasNull());
      }
      assertFalse(result.next());
    }
  }
}
