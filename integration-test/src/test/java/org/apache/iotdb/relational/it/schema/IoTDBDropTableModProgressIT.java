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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.CACHED_DEVICE_NUMBER;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDropTableModProgressIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnforceStrongPassword(false)
        .setEnableLastCache(true)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDropTableModFileProgress() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final Statement procedureStatement = TestUtils.createSingleNodeStatement(connection);
        final ITableSession longLivedSession = EnvFactory.getEnv().getTableSessionConnection()) {
      statement.execute("create database async_drop_mod_db");
      statement.execute("use async_drop_mod_db");
      statement.execute(
          "create table drop_target (device string tag, reading int32) with (need_last_cache=true)");
      statement.execute("create table retained (device string tag, reading int32)");
      // One region ensures both tables share a TsFile. Dropping just one table must write a mod
      // file rather than delete that TsFile outright, and replicas must not multiply the count.
      statement.execute("insert into drop_target(time, device, reading) values (1, 'd1', 10)");
      statement.execute("insert into retained(time, device, reading) values (1, 'd1', 20)");
      assertLatestReading(statement, 1, 10);
      await()
          .atMost(20, TimeUnit.SECONDS)
          .untilAsserted(() -> assertLastCacheHit(procedureStatement, 1, 10));
      longLivedSession.executeNonQueryStatement("use async_drop_mod_db");
      try (SessionDataSet resultSet =
          longLivedSession.executeQueryStatement("select device, reading from drop_target")) {
        assertTrue(resultSet.hasNext());
        assertEquals(Arrays.asList("device", "reading"), resultSet.getColumnNames());
        assertEquals(Arrays.asList("STRING", "INT32"), resultSet.getColumnTypes());
        resultSet.next();
        assertFalse(resultSet.hasNext());
      }
      statement.execute("flush");
      statement.execute("drop table drop_target");
      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (final ResultSet resultSet =
                    procedureStatement.executeQuery(
                        "select state, written_mod_file_count, remaining_mod_file_count "
                            + "from information_schema.drop_table_procedures "
                            + "where database = 'async_drop_mod_db' and table_name = 'drop_target'")) {
                  assertTrue(resultSet.next());
                  assertEquals("SUCCESS", resultSet.getString(1));
                  assertEquals(1, resultSet.getLong(2));
                  assertFalse(resultSet.wasNull());
                  assertEquals(0, resultSet.getLong(3));
                  assertFalse(resultSet.wasNull());
                  assertFalse(resultSet.next());
                }
              });
      statement.execute(
          "create table drop_target (device string tag, reading double) with (need_last_cache=true)");
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last(time), last_by(reading, time) from drop_target where device = 'd1'")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getObject(1));
        assertNull(resultSet.getObject(2));
        assertFalse(resultSet.next());
      }
      // Keep the old session and use a Tablet whose order differs from the table definition.
      // A lower timestamp also prevents a stale LAST entry from being hidden by a newer insert.
      Tablet tablet =
          new Tablet(
              "drop_target",
              Arrays.asList("reading", "device"),
              Arrays.asList(TSDataType.DOUBLE, TSDataType.STRING),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.TAG),
              1);
      tablet.addTimestamp(0, 0);
      tablet.addValue("reading", 0, 20.5);
      tablet.addValue("device", 0, "d1");
      longLivedSession.insert(tablet);
      assertLatestReading(statement, 0, 20.5);
      await()
          .atMost(20, TimeUnit.SECONDS)
          .untilAsserted(() -> assertLastCacheHit(procedureStatement, 0, 20.5));
      // Issue a new query on the session opened against the old INT32 definition.
      try (SessionDataSet resultSet =
          longLivedSession.executeQueryStatement(
              "select reading, device, cast(time as int64) as sample_time from drop_target")) {
        assertEquals(Arrays.asList("reading", "device", "sample_time"), resultSet.getColumnNames());
        assertEquals(Arrays.asList("DOUBLE", "STRING", "INT64"), resultSet.getColumnTypes());
        assertTrue(resultSet.hasNext());
        RowRecord row = resultSet.next();
        assertEquals(3, row.getFields().size());
        assertEquals(20.5, row.getFields().get(0).getDoubleV(), 0);
        assertEquals("d1", row.getFields().get(1).getStringValue());
        assertEquals(0, row.getFields().get(2).getLongV());
        assertFalse(resultSet.hasNext());
      }
      TestUtils.tableResultSetEqualTest(
          "DESCRIBE drop_target",
          new String[] {"ColumnName", "DataType", "Category"},
          new String[] {"time,TIMESTAMP,TIME,", "device,STRING,TAG,", "reading,DOUBLE,FIELD,"},
          "async_drop_mod_db");
      try (ResultSet resultSet =
          statement.executeQuery("select time, reading from drop_target where device = 'd1'")) {
        assertEquals("time", resultSet.getMetaData().getColumnName(1));
        assertEquals("reading", resultSet.getMetaData().getColumnName(2));
        assertEquals(Types.DOUBLE, resultSet.getMetaData().getColumnType(2));
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(1));
        assertEquals(20.5, resultSet.getDouble(2), 0);
        assertFalse(resultSet.next());
      }
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select device, reading from retained"),
          "device,reading,",
          Collections.singleton("d1,20,"));
    }
  }

  private static void assertLastCacheHit(Statement statement, long time, double reading)
      throws Exception {
    StringBuilder plan = new StringBuilder();
    // A single-node statement avoids comparing per-node timings in the cluster result wrapper.
    try (ResultSet resultSet =
        statement.executeQuery(
            "explain analyze verbose select last(time), last_by(reading, time) "
                + "from async_drop_mod_db.drop_target where device = 'd1'")) {
      while (resultSet.next()) {
        plan.append(resultSet.getString(1)).append('\n');
      }
    }
    assertTrue(
        "LAST must actually hit the target cache: " + plan,
        Pattern.compile(Pattern.quote(CACHED_DEVICE_NUMBER) + ":\\s*[1-9][0-9]*")
            .matcher(plan)
            .find());
    // Validate the result on the same connection after proving this query uses the LAST cache.
    // EXPLAIN ANALYZE returns the plan, so its cache-hit counter alone cannot check cached values.
    assertLatestReading(statement, time, reading);
  }

  private static void assertLatestReading(Statement statement, long time, double reading)
      throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery(
            "select last(time), last_by(reading, time) from drop_target where device = 'd1'")) {
      assertTrue(resultSet.next());
      assertEquals(time, resultSet.getLong(1));
      assertFalse(resultSet.wasNull());
      assertEquals(reading, resultSet.getDouble(2), 0);
      assertFalse(resultSet.wasNull());
      assertFalse(resultSet.next());
    }
  }
}
