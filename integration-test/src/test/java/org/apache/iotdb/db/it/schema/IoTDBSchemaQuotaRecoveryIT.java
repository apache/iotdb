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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.commons.utils.KillPoint.MetadataTestKillPoints;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.util.AbstractSchemaIT;
import org.apache.iotdb.util.MetadataRecoveryTestUtils.Gate;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MetadataRecoveryTestUtils.*;
import static org.junit.Assert.*;

/** P1-7: failed activation and deletion must release durable, asynchronously refreshed quota. */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSchemaQuotaRecoveryIT extends AbstractSchemaIT {
  public IoTDBSchemaQuotaRecoveryIT(SchemaTestMode mode) {
    super(mode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setClusterTimeseriesLimitThreshold(6)
        .setClusterDeviceLimitThreshold(3);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void cleanup() throws Exception {
    if (!node().isAlive()) {
      node().start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
    }
    try (Connection cleanup = connection(node(), BaseEnv.TREE_SQL_DIALECT)) {
      List<String> databases = new ArrayList<>();
      try (Statement statement = cleanup.createStatement();
          ResultSet result = statement.executeQuery("SHOW DATABASES")) {
        while (result.next()) {
          if (result.getString(1).startsWith("root.p1_")) {
            databases.add(result.getString(1));
          }
        }
      }
      for (String database : databases) {
        execute(cleanup, "DELETE DATABASE " + database);
      }
      List<String> templates = new ArrayList<>();
      try (Statement statement = cleanup.createStatement();
          ResultSet result = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        while (result.next()) {
          templates.add(result.getString(1));
        }
      }
      for (String template : templates) {
        execute(cleanup, "DROP DEVICE TEMPLATE " + template);
      }
    }
  }

  @Test
  public void testQuotaReleasedAfterFailedActivationDeletionAndRestart() throws Exception {
    try (Connection control = connection(node(), BaseEnv.TREE_SQL_DIALECT);
        Gate exhausted = new Gate(node(), MetadataTestKillPoints.SCHEMA_QUOTA_EXHAUSTED, false)) {
      execute(
          control,
          "CREATE DATABASE root.p1_quota_a",
          "CREATE DATABASE root.p1_quota_b",
          "CREATE DATABASE root.p1_quota_control",
          "CREATE DEVICE TEMPLATE p1_quota (s1 INT64 ENCODING=PLAIN,s2 INT64 ENCODING=PLAIN)",
          "SET DEVICE TEMPLATE p1_quota TO root.p1_quota_b",
          "CREATE TIMESERIES root.p1_quota_a.d.s1 INT64",
          "CREATE TIMESERIES root.p1_quota_a.d.s2 INT64",
          "CREATE TIMESERIES root.p1_quota_control.d.s1 INT64",
          "CREATE TIMESERIES root.p1_quota_control.d.s2 INT64",
          "CREATE TIMESERIES USING DEVICE TEMPLATE ON root.p1_quota_b.full",
          "INSERT INTO root.p1_quota_control.d(time,s1,s2) VALUES(1,10,20)");
      assertRows(control, "COUNT TIMESERIES root.**", "count(timeseries),", "6,");
      assertRows(control, "COUNT DEVICES root.**", "count(devices),", "3,");
      exhausted.await();
      for (int i = 0; i < 3; i++) {
        assertQuotaFailure(
            control, "CREATE TIMESERIES USING DEVICE TEMPLATE ON root.p1_quota_b.failed");
      }
      assertRows(
          control, "SHOW DEVICES root.p1_quota_b.failed", "Device,IsAligned,Template,TTL(ms),");
      execute(control, "DELETE DATABASE root.p1_quota_b", "FLUSH");
    }
    node().stopForcibly();
    node().start();
    EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
    try (Connection recovered = connection(node(), BaseEnv.TREE_SQL_DIALECT)) {
      assertRows(recovered, "COUNT TIMESERIES root.**", "count(timeseries),", "4,");
      execute(recovered, "SET DEVICE TEMPLATE p1_quota TO root.p1_quota_a.fresh");
      // Retrying after an explicit quota rejection is safe: no activation was committed. Success
      // proves that the asynchronous quota refresh crossed the deletion/recovery boundary.
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  execute(
                      recovered,
                      "CREATE TIMESERIES USING DEVICE TEMPLATE ON root.p1_quota_a.fresh");
                  return true;
                } catch (SQLException e) {
                  if (e.getErrorCode() != TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
                    throw e;
                  }
                  return false;
                }
              });
      assertRows(recovered, "COUNT TIMESERIES root.**", "count(timeseries),", "6,");
      assertRows(recovered, "COUNT DEVICES root.**", "count(devices),", "3,");
      try (Gate exhausted =
          new Gate(node(), MetadataTestKillPoints.SCHEMA_QUOTA_EXHAUSTED, false)) {
        exhausted.await();
      }
      assertQuotaFailure(recovered, "CREATE TIMESERIES root.p1_quota_a.d.extra INT64");
      assertRows(
          recovered,
          "SELECT s1,s2 FROM root.p1_quota_control.d",
          "Time,root.p1_quota_control.d.s1,root.p1_quota_control.d.s2,",
          "1,10,20,");
      execute(recovered, "INSERT INTO root.p1_quota_a.fresh(time,s1,s2) VALUES(2,30,40)");
      assertRows(
          recovered,
          "SELECT s1,s2 FROM root.p1_quota_a.fresh",
          "Time,root.p1_quota_a.fresh.s1,root.p1_quota_a.fresh.s2,",
          "2,30,40,");
    }
  }

  private void assertQuotaFailure(Connection connection, String sql) throws SQLException {
    try {
      execute(connection, sql);
      fail("Expected quota rejection: " + sql);
    } catch (SQLException e) {
      assertEquals(
          e.getMessage(), TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode(), e.getErrorCode());
    }
  }

  private static DataNodeWrapper node() {
    return EnvFactory.getEnv().getDataNodeWrapper(0);
  }
}
