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
    // Keep every device in the same schema region and force a new batch for each device.
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultSchemaRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(1);
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
      String predicate = CASE_RESULT + " = src";
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
  public void testAttributeCommonExpressionClearedByCase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTable(statement, "common_cache");
      statement.execute(
          "UPDATE common_cache SET a = src "
              + "WHERE CASE WHEN src = 'match' THEN true ELSE false END");
      assertResultSetEqual(
          statement.executeQuery("SHOW DEVICES FROM common_cache"),
          "id,src,a,b,",
          new HashSet<>(
              Arrays.asList(
                  "d0,match,match,null,",
                  "d1,match,match,null,",
                  "d2,other,null,null,",
                  "d3,other,null,null,")));
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
