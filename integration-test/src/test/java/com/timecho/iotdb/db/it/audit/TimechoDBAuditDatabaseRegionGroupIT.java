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

package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static com.timecho.iotdb.db.it.audit.TimechoDBAuditLogBasicIT.AUDITABLE_CONTROL_EVENT_TYPE_WITHOUT_ENTITY_STATUS_CHANGED;
import static com.timecho.iotdb.db.it.audit.TimechoDBAuditLogBasicIT.AUDITABLE_OPERATION_LEVEL;
import static com.timecho.iotdb.db.it.audit.TimechoDBAuditLogBasicIT.AUDITABLE_OPERATION_RESULT;
import static com.timecho.iotdb.db.it.audit.TimechoDBAuditLogBasicIT.AUDITABLE_OPERATION_TYPE;
import static com.timecho.iotdb.db.it.audit.TimechoDBAuditLogBasicIT.ENABLE_AUDIT_LOG;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBAuditDatabaseRegionGroupIT {

  private static final String TREE_MODEL_AUDIT_DATABASE = "root.__audit";
  private static final String TABLE_MODEL_AUDIT_DATABASE = "__audit";
  private static final String AUTO_POLICY = "AUTO";
  private static final String CUSTOM_POLICY = "CUSTOM";
  private static final int EXPECTED_AUDIT_REGION_GROUP_NUM = 1;
  private static final int DEFAULT_REGION_GROUP_NUM = 3;
  private static final long POLL_TIMEOUT_MS = 30_000L;
  private static final long POLL_INTERVAL_MS = 500L;

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAuditDatabaseRegionGroupNumUnderAutoPolicyForTreeAndTableModel()
      throws Exception {
    initClusterWithRegionGroupPolicy(AUTO_POLICY);
    assertTreeModelAuditDatabaseCreatedWithSingleRegionGroup();
    assertTableModelAuditDatabaseCreatedWithSingleRegionGroup();
  }

  @Test
  public void testAuditDatabaseRegionGroupNumUnderCustomPolicyForTreeAndTableModel()
      throws Exception {
    initClusterWithRegionGroupPolicy(CUSTOM_POLICY);
    assertTreeModelAuditDatabaseCreatedWithSingleRegionGroup();
    assertTableModelAuditDatabaseCreatedWithSingleRegionGroup();
  }

  private static void initClusterWithRegionGroupPolicy(final String regionGroupExtensionPolicy)
      throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setEnableAuditLog(ENABLE_AUDIT_LOG)
        .setAuditableOperationType(AUDITABLE_OPERATION_TYPE)
        .setAuditableOperationLevel(AUDITABLE_OPERATION_LEVEL)
        .setAuditableOperationResult(AUDITABLE_OPERATION_RESULT)
        .setAuditableControlEventType(AUDITABLE_CONTROL_EVENT_TYPE_WITHOUT_ENTITY_STATUS_CHANGED)
        .setSchemaRegionGroupExtensionPolicy(regionGroupExtensionPolicy)
        .setDataRegionGroupExtensionPolicy(regionGroupExtensionPolicy)
        .setDefaultSchemaRegionGroupNumPerDatabase(DEFAULT_REGION_GROUP_NUM)
        .setDefaultDataRegionGroupNumPerDatabase(DEFAULT_REGION_GROUP_NUM);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  private static void assertTreeModelAuditDatabaseCreatedWithSingleRegionGroup() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      assertAuditDatabaseRegionGroupNumEventually(
          () -> assertTreeModelAuditDatabaseRegionGroupNum(statement));
    }
  }

  private static void assertTableModelAuditDatabaseCreatedWithSingleRegionGroup() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      assertAuditDatabaseRegionGroupNumEventually(
          () -> assertTableModelAuditDatabaseRegionGroupNum(statement));
    }
  }

  private static void assertAuditDatabaseRegionGroupNumEventually(final CheckedRunnable assertion)
      throws Exception {
    final long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try {
        assertion.run();
        return;
      } catch (final AssertionError ignored) {
        TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
      }
    }
    assertion.run();
  }

  private static void assertTreeModelAuditDatabaseRegionGroupNum(final Statement statement)
      throws SQLException {
    try (final ResultSet resultSet =
        statement.executeQuery("SHOW DATABASES DETAILS " + TREE_MODEL_AUDIT_DATABASE)) {
      Assert.assertTrue(
          "Expected audit database " + TREE_MODEL_AUDIT_DATABASE + " to be created",
          resultSet.next());
      Assert.assertEquals(TREE_MODEL_AUDIT_DATABASE, resultSet.getString("Database"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("SchemaRegionGroupNum"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("MaxSchemaRegionGroupNum"));
      Assert.assertEquals(EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("DataRegionGroupNum"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("MaxDataRegionGroupNum"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private static void assertTableModelAuditDatabaseRegionGroupNum(final Statement statement)
      throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery(tableModelAuditDatabaseQuery())) {
      Assert.assertTrue(
          "Expected audit database " + TABLE_MODEL_AUDIT_DATABASE + " to be created",
          resultSet.next());
      Assert.assertEquals(TABLE_MODEL_AUDIT_DATABASE, resultSet.getString("database"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("schema_region_group_num"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("max_schema_region_group_num"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("data_region_group_num"));
      Assert.assertEquals(
          EXPECTED_AUDIT_REGION_GROUP_NUM, resultSet.getInt("max_data_region_group_num"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private static String tableModelAuditDatabaseQuery() {
    return "SELECT database, schema_region_group_num, max_schema_region_group_num, "
        + "data_region_group_num, max_data_region_group_num FROM information_schema.databases "
        + "WHERE database = '"
        + TABLE_MODEL_AUDIT_DATABASE
        + "'";
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }
}
