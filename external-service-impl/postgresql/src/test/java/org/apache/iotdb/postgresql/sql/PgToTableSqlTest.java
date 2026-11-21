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

package org.apache.iotdb.postgresql.sql;

import org.apache.iotdb.postgresql.protocol.PgException;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PgToTableSqlTest {

  private static final TranslationContext CONTEXT =
      new TranslationContext() {
        @Override
        public List<String> searchPath() {
          return Arrays.asList("plant", "public");
        }

        @Override
        public boolean tableExists(String database, String table) {
          return database.equals("plant") && table.equals("sensors");
        }

        @Override
        public String user() {
          return "alice";
        }

        @Override
        public String version() {
          return "PostgreSQL 14.0 (IoTDB)";
        }

        @Override
        public String currentDatabase() {
          return "iotdb";
        }

        @Override
        public int backendPid() {
          return 42;
        }
      };

  /** The translation without PostgreSQL output names (tested in {@link #outputColumnNames}). */
  private static String sql(String pg) throws PgException {
    return PgToTableSql.translate(pg, CONTEXT, false).sql();
  }

  private static void refused(String pg, String sqlState, String messagePart) {
    try {
      fail("refused, but translated to: " + sql(pg));
    } catch (PgException e) {
      assertEquals(e.getMessage(), sqlState, e.sqlState());
      assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
    }
  }

  @Test
  public void namesAreFoldedAndQuoted() throws Exception {
    assertEquals(
        "SELECT \"time\", \"Device\", \"t\".\"temp\" FROM \"mydb\".\"sensors\" AS \"t\"",
        sql("SELECT Time, \"Device\", t.TEMP FROM MyDb.sensors t"));
  }

  @Test
  public void unqualifiedTablesUseTheSearchPath() throws Exception {
    assertEquals("SELECT * FROM \"plant\".\"sensors\"", sql("SELECT * FROM sensors"));
    refused("SELECT * FROM missing", "42P01", "relation \"missing\" does not exist");
  }

  @Test
  public void catalogNamesGoToInformationSchema() throws Exception {
    assertEquals(
        "SELECT \"nspname\" FROM information_schema.\"pg_namespace\"",
        sql("SELECT nspname FROM pg_catalog.pg_namespace"));
    assertEquals(
        "SELECT \"oid\" FROM information_schema.\"pg_type\"", sql("SELECT oid FROM pg_type"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_is_tables\"",
        sql("SELECT * FROM information_schema.tables"));
  }

  @Test
  public void castsParametersAndLiterals() throws Exception {
    PgToTableSql.Translation t =
        PgToTableSql.translate(
            "SELECT temp::float8, CAST(x AS varchar(10)) FROM plant.sensors"
                + " WHERE time > $2 AND device = $1 AND other = $2",
            CONTEXT,
            false);
    assertEquals(
        "SELECT CAST(\"temp\" AS DOUBLE), CAST(\"x\" AS STRING) FROM \"plant\".\"sensors\""
            + " WHERE \"time\" > ? AND \"device\" = ? AND \"other\" = ?",
        t.sql());
    assertEquals(Arrays.asList(2, 1, 2), t.parameterOrder());
    assertEquals(2, t.parameterCount());
    assertEquals("SELECT 'it''s', 'a\nb', TRUE", sql("SELECT 'it''s', E'a\\nb', true"));
  }

  @Test
  public void sessionFunctionsBecomeConstants() throws Exception {
    assertEquals(
        "SELECT 'PostgreSQL 14.0 (IoTDB)', 'plant', 'iotdb', 42, 'alice', TRUE",
        sql(
            "SELECT version(), current_schema(), current_database(), pg_backend_pid(),"
                + " current_user, has_table_privilege('x', 'select')"));
  }

  @Test
  public void limitsOrderingAndIlike() throws Exception {
    assertEquals(
        "SELECT * FROM \"plant\".\"sensors\" WHERE lower(\"device\") LIKE lower('a%')"
            + " ORDER BY \"time\" DESC NULLS FIRST, \"device\" NULLS LAST LIMIT 5",
        sql(
            "SELECT * FROM sensors WHERE device ILIKE 'a%' ORDER BY time DESC, device"
                + " FETCH FIRST 5 ROWS ONLY"));
    assertEquals(
        "SELECT count(DISTINCT \"device\"), avg(\"temp\") FROM \"plant\".\"sensors\""
            + " GROUP BY \"device\" LIMIT 10 OFFSET 20",
        sql(
            "SELECT count(DISTINCT device), avg(temp) FROM sensors GROUP BY device"
                + " LIMIT 10 OFFSET 20"));
  }

  @Test
  public void constructsWithoutExactTranslationAreRefusedByName() {
    refused("SELECT DISTINCT ON (device) * FROM sensors", "0A000", "DISTINCT ON");
    refused("SELECT a = ANY(ARRAY[1,2]) FROM sensors", "0A000", "ARRAY");
    refused("SELECT now() - interval '1 month'", "0A000", "months and years");
    refused("SELECT interval '1 day'", "0A000", "INTERVAL values");
    refused(
        "SELECT sum(temp) OVER (ORDER BY time ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sensors",
        "0A000",
        "this form of the window function sum()");
    refused("SELECT median(temp) OVER () FROM sensors", "0A000", "window function median()");
    refused("SELECT x::numeric(10,2) FROM sensors", "0A000", "numeric");
    refused("SELECT md5(device) FROM sensors", "0A000", "function md5()");
    refused("SELECT * FROM sensors FOR UPDATE", "0A000", "FOR UPDATE");
  }

  @Test
  public void writesAreReadOnlyErrors() {
    refused("INSERT INTO plant.sensors VALUES (1)", "25006", "read-only");
    refused("DELETE FROM plant.sensors", "25006", "read-only");
    refused("CREATE TABLE x (a int)", "25006", "read-only");
  }

  @Test
  public void syntaxErrorsAre42601() {
    refused("SELEC 1", "42601", "syntax error");
  }

  @Test
  public void commonTableExpressionsStayUnqualified() throws Exception {
    assertEquals(
        "WITH w AS (SELECT \"device\" FROM \"plant\".\"sensors\") SELECT * FROM \"w\"",
        sql("WITH w AS (SELECT device FROM sensors) SELECT * FROM w"));
  }

  @Test
  public void objectIdentifierCastsOfLiterals() throws Exception {
    assertEquals("SELECT 1259", sql("SELECT 'pg_class'::regclass"));
    assertEquals("SELECT 1259", sql("SELECT 'pg_catalog.pg_class'::pg_catalog.regclass"));
    assertEquals(
        "SELECT (SELECT max(pg_rc.\"oid\") FROM information_schema.\"pg_class\" pg_rc"
            + " JOIN information_schema.\"pg_namespace\" pg_rn"
            + " ON pg_rc.\"relnamespace\" = pg_rn.\"oid\""
            + " WHERE pg_rn.\"nspname\" = 'plant' AND pg_rc.\"relname\" = 'sensors')",
        sql("SELECT 'sensors'::regclass"));
    assertEquals("SELECT 23, 1185", sql("SELECT 'integer'::regtype, 'timestamptz[]'::regtype"));
    assertEquals("SELECT 750", sql("SELECT 'pg_catalog.array_in'::regproc"));
    assertEquals("SELECT 11", sql("SELECT 'pg_catalog'::regnamespace"));
    refused("SELECT c.oid::regclass FROM pg_class c", "0A000", "casts of expressions to regclass");
    refused("SELECT 'nosuchtype'::regtype", "42704", "nosuchtype");
    refused("SELECT 'nosuchfunc'::regproc", "42883", "nosuchfunc");
    refused("SELECT 'missing'::regclass", "42P01", "missing");
  }

  @Test
  public void catalogFunctions() throws Exception {
    String formatType = sql("SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a");
    assertTrue(formatType, formatType.startsWith("SELECT CASE CAST(\"a\".\"atttypid\" AS INT32)"));
    assertTrue(formatType, formatType.contains("WHEN 1184 THEN 'timestamp with time zone'"));
    assertTrue(formatType, formatType.contains("WHEN 1007 THEN 'integer[]'"));
    assertEquals(
        "SELECT CAST(NULL AS STRING) AS \"pg_get_expr\" FROM information_schema.\"pg_attrdef\""
            + " WHERE FALSE",
        sql("SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d"));
    assertEquals(
        "SELECT (SELECT max(pg_d.\"description\") FROM information_schema.\"pg_description\" pg_d"
            + " WHERE pg_d.\"objoid\" = \"c\".\"oid\" AND pg_d.\"objsubid\" = 0"
            + " AND pg_d.\"classoid\" = 1259) FROM information_schema.\"pg_class\" AS \"c\"",
        sql("SELECT obj_description(c.oid, 'pg_class') FROM pg_class c"));
    assertEquals(
        "SELECT (SELECT max(pg_d.\"description\") FROM information_schema.\"pg_description\" pg_d"
            + " WHERE pg_d.\"objoid\" = \"a\".\"attrelid\" AND pg_d.\"objsubid\" ="
            + " \"a\".\"attnum\" AND pg_d.\"classoid\" = 1259)"
            + " FROM information_schema.\"pg_attribute\" AS \"a\"",
        sql("SELECT col_description(a.attrelid, a.attnum) FROM pg_attribute a"));
    assertEquals("SELECT 'alice'", sql("SELECT pg_get_userbyid(10)"));
  }

  @Test
  public void quotedLiteralsTakeTheTypeOfCatalogColumns() throws Exception {
    assertEquals(
        "SELECT \"a\".\"attname\" FROM information_schema.\"pg_attribute\" AS \"a\""
            + " WHERE \"a\".\"attrelid\" = 16386 AND \"a\".\"attisdropped\" = FALSE",
        sql(
            "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = '16386'"
                + " AND a.attisdropped = 'f'"));
    // unqualified: only when every table is a catalog table
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" WHERE \"oid\" = 5",
        sql("SELECT * FROM pg_class WHERE oid = '5'"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\", \"plant\".\"sensors\""
            + " WHERE \"oid\" = '5'",
        sql("SELECT * FROM pg_class, sensors WHERE oid = '5'"));
    // text columns keep the literal
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" AS \"c\" WHERE \"c\".\"relkind\" = 'r'",
        sql("SELECT * FROM pg_class c WHERE c.relkind = 'r'"));
    refused("SELECT * FROM pg_class c WHERE c.oid = 'abc'", "22P02", "abc");
  }

  @Test
  public void nullResultsOfCaseTakeTheTypeOfTheOthers() throws Exception {
    assertEquals(
        "SELECT CASE WHEN \"a\".\"attstattarget\" = -1 THEN CAST(NULL AS INT32)"
            + " ELSE \"a\".\"attstattarget\" END FROM information_schema.\"pg_attribute\" AS \"a\"",
        sql(
            "SELECT CASE WHEN a.attstattarget = -1 THEN NULL ELSE a.attstattarget END"
                + " FROM pg_attribute a"));
    assertEquals(
        "SELECT CASE WHEN TRUE THEN 'x' ELSE CAST(NULL AS STRING) END",
        sql("SELECT CASE WHEN true THEN 'x' ELSE NULL END"));
  }

  @Test
  public void outputNamesAreFolded() throws Exception {
    assertEquals(
        "SELECT \"temp\" AS \"table_cat\", \"temp\" AS \"Mixed\" FROM \"plant\".\"sensors\"",
        sql("SELECT temp AS TABLE_CAT, temp AS \"Mixed\" FROM plant.sensors"));
  }

  @Test
  public void anchoredRegularExpressionsBecomeLikeOrEquality() throws Exception {
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" AS \"c\" WHERE (\"c\".\"relname\" = 'sensors')",
        sql(
            "SELECT * FROM pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(sensors)$'"
                + " COLLATE pg_catalog.default"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_namespace\" AS \"n\""
            + " WHERE NOT (\"n\".\"nspname\" LIKE 'pg\\_%' ESCAPE '\\')",
        sql("SELECT * FROM pg_namespace n WHERE n.nspname !~ '^pg_'"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" AS \"c\""
            + " WHERE (\"c\".\"relname\" LIKE 'sens%')",
        sql("SELECT * FROM pg_class c WHERE c.relname ~ '^(sens.*)$'"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" AS \"c\""
            + " WHERE (\"c\".\"relname\" IN ('a', 'b'))",
        sql("SELECT * FROM pg_class c WHERE c.relname ~ '^(a|b)$'"));
    assertEquals(
        "SELECT * FROM information_schema.\"pg_class\" AS \"c\""
            + " WHERE (lower(\"c\".\"relname\") LIKE '%ab_c%')",
        sql("SELECT * FROM pg_class c WHERE c.relname ~* 'AB.c'"));
    refused("SELECT * FROM pg_class c WHERE c.relname ~ '^[a-z]+$'", "0A000", "[a-z]+");
    refused("SELECT * FROM pg_class c WHERE c.relname ~ c.relname", "0A000", "not literals");
    // OPERATOR(...) inside a literal is text, not syntax
    assertEquals("SELECT 'OPERATOR(pg_catalog.~)'", sql("SELECT 'OPERATOR(pg_catalog.~)'"));
  }

  @Test
  public void queriesOverAlwaysEmptyCatalogTablesSelectTypedNulls() throws Exception {
    assertEquals(
        "SELECT CAST(NULL AS STRING) AS \"polname\", CAST(NULL AS STRING) AS \"r\""
            + " FROM information_schema.\"pg_policy\" WHERE FALSE",
        sql(
            "SELECT pol.polname, array_to_string(array(select 1), ',') AS r"
                + " FROM pg_catalog.pg_policy pol WHERE 'd' = any(pol.polroles) ORDER BY 1"));
    // an aggregate over no rows is still one row: not rewritten
    assertEquals(
        "SELECT count(*) FROM information_schema.\"pg_inherits\"",
        sql("SELECT count(*) FROM pg_inherits"));
    // a scalar sub-query over an empty table is NULL
    assertEquals(
        "SELECT CAST(NULL AS STRING) AS \"adbin\" FROM information_schema.\"pg_class\"",
        sql(
            "SELECT (SELECT d.adbin FROM pg_attrdef d WHERE d.adrelid = pg_class.oid) AS adbin"
                + " FROM pg_class"));
  }

  @Test
  public void outerConditionsMoveInFrontOfScalarSubQueries() throws Exception {
    assertEquals(
        "SELECT CASE WHEN \"a\".\"atthasdef\" THEN (SELECT \"t\".\"typname\""
            + " FROM information_schema.\"pg_type\" AS \"t\" WHERE \"t\".\"oid\" = \"a\".\"atttypid\")"
            + " END FROM information_schema.\"pg_attribute\" AS \"a\"",
        sql(
            "SELECT (SELECT t.typname FROM pg_type t WHERE t.oid = a.atttypid AND a.atthasdef)"
                + " FROM pg_attribute a"));
    // not for EXISTS, and not for aggregating sub-queries
    String exists =
        sql(
            "SELECT 1 FROM pg_attribute a WHERE EXISTS (SELECT 1 FROM pg_type t"
                + " WHERE t.oid = a.atttypid AND a.atthasdef)");
    assertTrue(exists, !exists.contains("CASE"));
    String count =
        sql(
            "SELECT (SELECT count(*) FROM pg_type t WHERE t.oid = a.atttypid AND a.atthasdef)"
                + " FROM pg_attribute a");
    assertTrue(count, !count.contains("CASE"));
  }

  @Test
  public void morePsqlFunctions() throws Exception {
    String encoding = sql("SELECT pg_encoding_to_char(d.encoding) FROM pg_database d");
    assertTrue(encoding, encoding.contains("WHEN 6 THEN 'UTF8'"));
    assertTrue(encoding, encoding.contains("ELSE ''"));
    assertEquals(
        "SELECT CAST(NULL AS STRING) FROM information_schema.\"pg_database\" AS \"d\"",
        sql("SELECT array_to_string(d.datacl, E'\\n') FROM pg_database d"));
    refused(
        "SELECT array_to_string(d.datname, ',') FROM pg_database d", "0A000", "array_to_string");
    String regtype = sql("SELECT c.reloftype::pg_catalog.regtype::pg_catalog.text FROM pg_class c");
    assertTrue(regtype, regtype.contains("WHEN 23 THEN 'integer'"));
    assertTrue(
        regtype,
        regtype.endsWith(
            "ELSE CAST(\"c\".\"reloftype\" AS STRING) END"
                + " FROM information_schema.\"pg_class\" AS \"c\""));
    assertEquals("SELECT CAST(NULL AS STRING) AS \"x\"", sql("SELECT NULL AS x"));
  }

  private static TranslationContext inZone(String zone) {
    return new TranslationContext() {
      @Override
      public List<String> searchPath() {
        return CONTEXT.searchPath();
      }

      @Override
      public boolean tableExists(String database, String table) {
        return CONTEXT.tableExists(database, table);
      }

      @Override
      public String user() {
        return "alice";
      }

      @Override
      public String version() {
        return "v";
      }

      @Override
      public String currentDatabase() {
        return "iotdb";
      }

      @Override
      public int backendPid() {
        return 42;
      }

      @Override
      public java.time.ZoneId zone() {
        return java.time.ZoneId.of(zone);
      }
    };
  }

  private static String sqlIn(String zone, String pg) throws PgException {
    return PgToTableSql.translate(pg, inZone(zone), false).sql();
  }

  @Test
  public void dateTruncInTheSessionTimeZone() throws Exception {
    // Europe/Berlin: local 1970-01-01 00:00 is -3600000 ms
    assertEquals(
        "SELECT date_bin(1mo, \"time\", -3600000) FROM \"plant\".\"sensors\"",
        sqlIn("Europe/Berlin", "SELECT date_trunc('month', time) FROM sensors"));
    assertEquals(
        "SELECT date_bin(3mo, \"time\", -3600000), date_bin(12mo, \"time\", -3600000)"
            + " FROM \"plant\".\"sensors\"",
        sqlIn(
            "Europe/Berlin",
            "SELECT date_trunc('quarter', time), date_trunc('YEAR', time) FROM sensors"));
    assertEquals(
        "SELECT CAST(CAST(\"time\" AS DATE) AS TIMESTAMP) FROM \"plant\".\"sensors\"",
        sqlIn("Europe/Berlin", "SELECT date_trunc('day', time) FROM sensors"));
    assertEquals(
        "SELECT date_bin(1h, \"time\", 0) FROM \"plant\".\"sensors\"",
        sqlIn("Europe/Berlin", "SELECT date_trunc('hour', time) FROM sensors"));
    assertEquals(
        "SELECT date_bin(1w, \"time\", 345600000) FROM \"plant\".\"sensors\"",
        sql("SELECT date_trunc('week', time) FROM sensors"));
    // half-hour offsets: hours are not whole; DST: weeks have no fixed length
    refused2("Asia/Kolkata", "SELECT date_trunc('hour', time) FROM sensors", "hour");
    refused2("Europe/Berlin", "SELECT date_trunc('week', time) FROM sensors", "daylight saving");
    refused2("UTC", "SELECT date_trunc('century', time) FROM sensors", "century");
    // other expressions are cast to TIMESTAMP (a DATE becomes local midnight)
    assertEquals(
        "SELECT CAST(CAST(CAST(CAST('2026-03-29' AS DATE) AS TIMESTAMP) AS DATE) AS TIMESTAMP)",
        sql("SELECT date_trunc('day', CAST('2026-03-29' AS date))"));
  }

  @Test
  public void datePartsAsPostgreSql() throws Exception {
    assertEquals(
        "SELECT EXTRACT(YEAR FROM \"time\"), (EXTRACT(DOW FROM \"time\") % 7)"
            + " FROM \"plant\".\"sensors\"",
        sql("SELECT extract(year from time), extract(dow from time) FROM sensors"));
    assertEquals(
        "SELECT CAST(EXTRACT(MONTH FROM \"time\") AS DOUBLE) FROM \"plant\".\"sensors\"",
        sql("SELECT date_part('month', time) FROM sensors"));
    assertEquals(
        "SELECT (CAST(CAST(\"time\" AS INT64) AS DOUBLE) / 1000) FROM \"plant\".\"sensors\"",
        sql("SELECT extract(epoch from time) FROM sensors"));
    refused("SELECT extract(week from time) FROM sensors", "0A000", "'week'");
  }

  @Test
  public void intervalArithmetic() throws Exception {
    assertEquals(
        "SELECT * FROM \"plant\".\"sensors\" WHERE \"time\" >= (now() - 7200000)",
        sqlIn("Europe/Berlin", "SELECT * FROM sensors WHERE time >= now() - interval '2 hours'"));
    assertEquals(
        "SELECT (CAST(\"time\" AS TIMESTAMP) + 5400000) FROM \"plant\".\"sensors\"",
        sql("SELECT time + '1:30'::interval FROM sensors"));
    assertEquals(
        "SELECT (now() - 2592000000)", sql("SELECT CURRENT_TIMESTAMP - INTERVAL '30' DAY"));
    assertEquals("SELECT (now() - 90000)", sql("SELECT now() - interval '1 min 30 sec'"));
    refused2("Europe/Berlin", "SELECT now() - interval '7 days'", "daylight saving time");
    refused("SELECT now() - interval '1 microsecond'", "0A000", "precision");
  }

  @Test
  public void stringRules() throws Exception {
    assertEquals(
        "SELECT CASE WHEN \"a\" IS NULL OR \"b\" IS NULL THEN CAST(NULL AS STRING)"
            + " ELSE concat(\"a\", \"b\") END FROM \"plant\".\"sensors\"",
        sql("SELECT a || b FROM sensors"));
    assertEquals(
        "SELECT COALESCE(concat(\"a\", 'x'), '') FROM \"plant\".\"sensors\"",
        sql("SELECT concat(a, 'x') FROM sensors"));
    refused("SELECT lpad(a, 5) FROM sensors", "0A000", "lpad");
  }

  private static void refused2(String zone, String pg, String messagePart) {
    try {
      fail("refused, but translated to: " + sqlIn(zone, pg));
    } catch (PgException e) {
      assertEquals(e.getMessage(), "0A000", e.sqlState());
      assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
    }
  }

  @Test
  public void dateAndTimestampLiteralsAsPostgreSqlReadsThem() throws Exception {
    // timestamp: local time in the session zone, an offset is ignored
    assertEquals(
        "SELECT CAST(1792879200000 AS TIMESTAMP)",
        sqlIn("Europe/Berlin", "SELECT TIMESTAMP '2026-10-25 00:00:00'"));
    assertEquals(
        "SELECT CAST(1792879200000 AS TIMESTAMP)",
        sqlIn("Europe/Berlin", "SELECT TIMESTAMP '2026-10-25 00:00:00+05'"));
    // timestamptz: its offset counts
    assertEquals(
        "SELECT CAST(1792886400000 AS TIMESTAMP)",
        sqlIn("Europe/Berlin", "SELECT '2026-10-25 00:00:00+00'::timestamptz"));
    assertEquals(
        "SELECT CAST(1792886400250 AS TIMESTAMP)",
        sql("SELECT timestamptz '2026-10-25T00:00:00.25Z'"));
    assertEquals("SELECT CAST('2026-03-01' AS DATE)", sql("SELECT DATE '2026-3-1'"));
    assertEquals("SELECT CAST('2026-03-01' AS DATE)", sql("SELECT '2026-03-01'::date"));
    refused("SELECT TIMESTAMP 'yesterday'", "0A000", "yesterday");
    refused("SELECT TIME '10:00'", "0A000", "TIME literals");
  }

  @Test
  public void dateParametersAreWrittenIntoTheStatement() throws Exception {
    PgToTableSql.Translation t =
        PgToTableSql.translate(
                "SELECT * FROM sensors WHERE label = '?' AND day >= $2 AND n = $1", CONTEXT)
            .withTypedParameters(new int[] {23, 1082});
    java.util.List<Object> values =
        java.util.Arrays.asList(
            new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral(
                "2026-01-02"),
            new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral("5"));
    assertEquals(
        "SELECT * FROM \"plant\".\"sensors\" WHERE \"label\" = '?'"
            + " AND \"day\" >= CAST('2026-01-02' AS DATE) AND \"n\" = ?",
        t.sqlFor(values));
    assertEquals(1, t.remaining(values).size());
    assertTrue(
        t.remaining(values).get(0)
            instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral);
  }

  @Test
  public void outputColumnNames() throws Exception {
    assertEquals(
        "SELECT count(*) AS \"count\", \"temp\", CAST(\"temp\" AS STRING) AS \"temp\","
            + " CAST('1' AS INT32) AS \"int4\", 1 AS \"?column?\", now() AS \"now\","
            + " \"temp\" AS \"t\" FROM \"plant\".\"sensors\" GROUP BY \"temp\"",
        PgToTableSql.translate(
                "SELECT count(*), temp, temp::text, '1'::integer, 1, now(), temp AS t"
                    + " FROM sensors GROUP BY temp",
                CONTEXT)
            .sql());
    // only the outer select list is named; a UNION takes the first branch's names
    assertEquals(
        "SELECT max(\"n\") AS \"max\" FROM (SELECT count(*) FROM \"plant\".\"sensors\") x"
            + " UNION ALL SELECT 1",
        PgToTableSql.translate(
                "SELECT max(n) FROM (SELECT count(*) FROM sensors) x UNION ALL SELECT 1", CONTEXT)
            .sql());
  }
}
