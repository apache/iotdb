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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCaseDeviceCacheIT {
  private static final String DATABASE = "case_device_cache";
  private static final String CASE_RESULT = "CASE WHEN src = 'match' THEN 'match' ELSE 'other' END";

  @BeforeClass
  public static void setUp() throws Exception {
    // CUSTOM fixes the region count; PROACTIVE treats the default count as a minimum.
    // Keep devices in one schema region to exercise mixed-row and repeated batches.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(2);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(new String[] {"CREATE DATABASE " + DATABASE});
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDevicePredicatesAcrossBatches() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTable(statement, "predicate_cache");
      for (String predicate : new String[] {CASE_RESULT + " = src", "src = " + CASE_RESULT}) {
        assertResultSetEqual(
            statement.executeQuery("SHOW DEVICES FROM predicate_cache WHERE " + predicate),
            "id,src,a,b,",
            new HashSet<>(
                Arrays.asList(
                    "d0,match,null,null,",
                    "d1,match,null,null,",
                    "d2,other,null,null,",
                    "d3,other,null,null,")));
        assertResultSetEqual(
            statement.executeQuery("COUNT DEVICES FROM predicate_cache WHERE " + predicate),
            "count(devices),",
            Collections.singleton("4,"));
      }
    }
  }

  @Test
  public void testAttributeProjectionAcrossBatches() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTable(statement, "projection_cache");
      statement.execute("UPDATE projection_cache SET a = " + CASE_RESULT + ", b = src");
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM projection_cache"),
          "id,src,a,b,",
          new HashSet<>(
              Arrays.asList(
                  "d0,match,match,match,",
                  "d1,match,match,match,",
                  "d2,other,other,other,",
                  "d3,other,other,other,")));
    }
  }

  @Test
  public void testAttributeConcatRetainsInputClearedByCase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTable(statement, "concat_cache");
      statement.execute(
          "UPDATE concat_cache SET b = concat(src, "
              + "CASE WHEN src = 'match' THEN '-m' ELSE '-o' END)");
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM concat_cache"),
          "id,src,a,b,",
          new HashSet<>(
              Arrays.asList(
                  "d0,match,null,match-m,",
                  "d1,match,null,match-m,",
                  "d2,other,null,other-o,",
                  "d3,other,null,other-o,")));
    }
  }

  @Test
  public void testAttributeCommonExpressionClearedByCase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTable(statement, "common_cache");
      statement.execute(
          "UPDATE common_cache SET a = upper(src) "
              + "WHERE CASE WHEN upper(src) = 'MATCH' THEN true ELSE false END");
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM common_cache"),
          "id,src,a,b,",
          new HashSet<>(
              Arrays.asList(
                  "d0,match,MATCH,null,",
                  "d1,match,MATCH,null,",
                  "d2,other,null,null,",
                  "d3,other,null,null,")));
    }
  }

  @Test
  public void testGuardedCommonCastInAttributeUpdate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE guarded_cast "
              + "(id STRING TAG, src STRING ATTRIBUTE, a STRING ATTRIBUTE, v DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO guarded_cast(time, id, src, a, v) VALUES "
              + "(1, 'd0', 'n/a', 'seed', 1), (2, 'd1', '5', 'seed', 1)");
      String update =
          "UPDATE guarded_cast SET a = CAST(CAST(src AS INT32) + 1 AS STRING) "
              + "WHERE CASE WHEN src <> 'n/a' THEN CAST(src AS INT32) > 10 ELSE false END";

      statement.execute(update);
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM guarded_cast"),
          "id,src,a,",
          new HashSet<>(Arrays.asList("d0,n/a,seed,", "d1,5,seed,")));

      statement.execute("UPDATE guarded_cast SET src = '50' WHERE id = 'd1'");
      statement.execute(update);
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM guarded_cast"),
          "id,src,a,",
          new HashSet<>(Arrays.asList("d0,n/a,seed,", "d1,50,51,")));
    }
  }

  @Test
  public void testAttributeCommonDiffClearedByCase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE diff_cache "
              + "(id STRING TAG, src STRING ATTRIBUTE, a STRING ATTRIBUTE, v DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO diff_cache(time, id, src, a, v) VALUES "
              + "(1, 'd1', '10', 'seed', 1), (2, 'd2', '20', 'seed', 1)");
      // The shared binary diff is stateful, so the guarded recompute must re-evaluate every row
      // instead of only the filter-passing rows, which would read the masked rows as nulls.
      statement.execute(
          "UPDATE diff_cache SET a = CAST(diff(CAST(src AS DOUBLE), src = 'x') AS STRING) "
              + "WHERE CASE WHEN src = 'x' THEN true ELSE "
              + "diff(CAST(src AS DOUBLE), src = 'x') > 5.0 END");
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM diff_cache"),
          "id,src,a,",
          new HashSet<>(Arrays.asList("d1,10,seed,", "d2,20,10.0,")));
    }
  }

  @Test
  public void testGuardedCommonDiffWithoutMatchingDevices() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE guarded_diff_no_match "
              + "(id STRING TAG, src STRING ATTRIBUTE, a STRING ATTRIBUTE, v DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO guarded_diff_no_match(time, id, src, a, v) VALUES "
              + "(1, 'd0', 'n/a', 'seed', 1), (2, 'd1', '5', 'seed', 1), "
              + "(3, 'd2', 'n/a', 'seed', 1)");
      String update =
          "UPDATE guarded_diff_no_match "
              + "SET a = CAST(diff(CAST(src AS DOUBLE), src = 'x') AS STRING) "
              + "WHERE CASE WHEN src = 'n/a' THEN false ELSE "
              + "diff(CAST(src AS DOUBLE), src = 'x') > 0 END";

      // Cover both a full batch and the final partial batch flushed when the updater closes.
      // No device matches, so neither batch may retry the guarded CAST on 'n/a'.
      statement.execute(update);
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM guarded_diff_no_match"),
          "id,src,a,",
          new HashSet<>(Arrays.asList("d0,n/a,seed,", "d1,5,seed,", "d2,n/a,seed,")));

      statement.execute(
          "UPDATE guarded_diff_no_match SET src = CASE id WHEN 'd0' THEN '10' ELSE '20' END");
      statement.execute(update);
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM guarded_diff_no_match"),
          "id,src,a,",
          new HashSet<>(Arrays.asList("d0,10,seed,", "d1,20,10.0,", "d2,20,seed,")));
    }
  }

  private static void createTable(Statement statement, String table) throws SQLException {
    statement.execute("USE " + DATABASE);
    statement.execute(
        "CREATE TABLE "
            + table
            + " (id STRING TAG, src STRING ATTRIBUTE, a STRING ATTRIBUTE, b STRING ATTRIBUTE, v DOUBLE FIELD)");
    statement.execute(
        "INSERT INTO "
            + table
            + "(time, id, src, v) VALUES (1, 'd0', 'match', 1), (2, 'd1', 'match', 1), "
            + "(3, 'd2', 'other', 1), (4, 'd3', 'other', 1)");
  }
}
