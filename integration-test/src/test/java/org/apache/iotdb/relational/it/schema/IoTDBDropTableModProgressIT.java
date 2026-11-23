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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        final Statement procedureStatement = TestUtils.createSingleNodeStatement(connection)) {
      statement.execute("create database async_drop_mod_db");
      statement.execute("use async_drop_mod_db");
      statement.execute("create table drop_target (device string tag, reading int32)");
      statement.execute("create table retained (device string tag, reading int32)");
      // One region ensures both tables share a TsFile. Dropping just one table must write a mod
      // file rather than delete that TsFile outright, and replicas must not multiply the count.
      statement.execute("insert into drop_target(time, device, reading) values (1, 'd1', 10)");
      statement.execute("insert into retained(time, device, reading) values (1, 'd1', 20)");
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
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select device, reading from retained"),
          "device,reading,",
          Collections.singleton("d1,20,"));
    }
  }

  @Test
  public void testDropColumnModFileProgress() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final Statement procedureStatement = TestUtils.createSingleNodeStatement(connection)) {
      statement.execute("create database async_drop_column_mod_db");
      statement.execute("use async_drop_column_mod_db");
      statement.execute(
          "create table target (device string tag, label string attribute, reading int32, retained int32)");
      statement.execute(
          "insert into target(time, device, label, reading, retained) values (1, 'd1', 'label', 10, 20)");
      statement.execute("flush");
      statement.execute("alter table target drop column reading");
      awaitColumnDrop(procedureStatement, "async_drop_column_mod_db", "reading", 1);
      statement.execute("alter table target drop column label");
      awaitColumnDrop(procedureStatement, "async_drop_column_mod_db", "label", 0);
      // A later deletion must not overwrite the retained result for the earlier column.
      awaitColumnDrop(procedureStatement, "async_drop_column_mod_db", "reading", 1);
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select device, retained from target"),
          "device,retained,",
          Collections.singleton("d1,20,"));
      try (final ResultSet resultSet = statement.executeQuery("describe target details")) {
        while (resultSet.next()) {
          assertFalse("reading".equals(resultSet.getString(1)));
          assertFalse("label".equals(resultSet.getString(1)));
        }
      }
    }
  }

  @Test
  public void testDropColumnWithoutDataModFileProgress() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final Statement procedureStatement = TestUtils.createSingleNodeStatement(connection)) {
      statement.execute("create database async_drop_empty_column_db");
      statement.execute("use async_drop_empty_column_db");
      statement.execute("create table target (device string tag, reading int32, retained int32)");
      statement.execute("alter table target drop column reading");
      awaitColumnDrop(procedureStatement, "async_drop_empty_column_db", "reading", 0);
    }
  }

  private static void awaitColumnDrop(
      final Statement statement,
      final String database,
      final String column,
      final long writtenModFiles) {
    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final ResultSet resultSet =
                  statement.executeQuery(
                      "select * from information_schema.drop_table_column_procedures "
                          + "where database = '"
                          + database
                          + "' and table_name = 'target' "
                          + "and column_name = '"
                          + column
                          + "'")) {
                final String[] columns = {
                  "database",
                  "table_name",
                  "column_name",
                  "procedure_id",
                  "state",
                  "progress",
                  "error_message",
                  "written_mod_file_count",
                  "remaining_mod_file_count"
                };
                assertEquals(columns.length, resultSet.getMetaData().getColumnCount());
                for (int i = 0; i < columns.length; i++) {
                  assertEquals(columns[i], resultSet.getMetaData().getColumnLabel(i + 1));
                }
                assertTrue(resultSet.next());
                assertTrue(resultSet.getLong("procedure_id") >= 0);
                assertEquals("SUCCESS", resultSet.getString("state"));
                assertEquals("SUCCESS", resultSet.getString("progress"));
                assertEquals(null, resultSet.getString("error_message"));
                assertEquals(writtenModFiles, resultSet.getLong("written_mod_file_count"));
                assertFalse(resultSet.wasNull());
                assertEquals(0, resultSet.getLong("remaining_mod_file_count"));
                assertFalse(resultSet.wasNull());
                assertFalse(resultSet.next());
              }
            });
  }
}
