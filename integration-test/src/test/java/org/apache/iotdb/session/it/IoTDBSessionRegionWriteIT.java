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

package org.apache.iotdb.session.it;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBSessionRegionWriteIT {
  private final String[] databases = new String[2];

  @BeforeClass
  public static void setUp() {
    // Single replicas make one Region unwritable without disabling the other Region.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setRegionGroupAllocatePolicy("GREEDY")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setTimePartitionInterval(1000)
        .setWalMode("SYNC");
    EnvFactory.getEnv().initClusterEnvironment(1, 2);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSplitTabletsRejectOnlyUnwritableRegionAndRetry() throws Exception {
    // A rejected Region must not roll back a successful peer or misalign the retried tablet.
    Map<String, Integer> owners = new HashMap<>();
    Map<String, Integer> regions = new HashMap<>();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Internal databases also own Regions. Select actual placements instead of assuming
      // that the first two business databases must land on different DataNodes.
      for (int candidate = 0; candidate < 8 && databases[1] == null; candidate++) {
        String database = "root.split_" + candidate;
        statement.execute("CREATE DATABASE " + database);
        statement.execute("CREATE TIMESERIES " + database + ".d.n INT64");
        statement.execute("CREATE TIMESERIES " + database + ".d.t TEXT");
        // Allocate both time partitions before the fault so that analysis can complete and
        // the rejection occurs when executing the already-routed child request.
        statement.execute(
            "INSERT INTO "
                + database
                + ".d(time,n,t) VALUES(0,-1,'baseline'),(1000,-1,'baseline')");
        try (ResultSet result = statement.executeQuery("SHOW REGIONS")) {
          while (result.next()) {
            if (database.equals(result.getString("Database"))
                && "DataRegion".equals(result.getString("Type"))) {
              assertEquals(
                  "The fixture requires one replica",
                  null,
                  owners.put(database, result.getInt("RpcPort")));
              regions.put(database, result.getInt("RegionId"));
            }
          }
        }
        assertNotNull("Missing DataRegion for " + database, owners.get(database));
        if (databases[0] == null) {
          databases[0] = database;
        } else if (!owners.get(databases[0]).equals(owners.get(database))) {
          databases[1] = database;
        }
      }
    }
    assertNotNull("No separately hosted Region in " + owners, databases[1]);
    assertNotEquals(regions.get(databases[0]), regions.get(databases[1]));
    assertNotEquals(
        "Both Regions must have different owners",
        owners.get(databases[0]),
        owners.get(databases[1]));
    DataNodeWrapper healthy = nodeAtPort(owners.get(databases[0]));
    DataNodeWrapper victim = nodeAtPort(owners.get(databases[1]));
    try (Session session =
        new Session.Builder()
            .host(healthy.getIp())
            .port(healthy.getPort())
            .enableAutoFetch(false)
            .enableRedirection(false)
            .maxRetryCount(0)
            .build()) {
      session.open(false, 30_000);
      try {
        setStatus(victim, "READONLY");
        EnvFactory.getEnv()
            .ensureNodeStatus(
                Collections.singletonList(victim), Collections.singletonList(NodeStatus.ReadOnly));
        // One RPC, two actual Regions, unsorted timestamps, and sparse text in each partition.
        Map<String, Tablet> batch = new LinkedHashMap<>();
        batch.put(databases[1] + ".d", tablet(1));
        batch.put(databases[0] + ".d", tablet(0));
        StatementExecutionException failure =
            assertThrows(StatementExecutionException.class, () -> session.insertTablets(batch));
        assertEquals(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), failure.getStatusCode());
        // This API returns a Region-level failure, not individual row acknowledgements.
        // The successful other Region proves that this was not whole-request prevalidation.
        assertData(0, true);
        assertData(1, false);
      } finally {
        setStatus(victim, "RUNNING");
      }
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(victim), Collections.singletonList(NodeStatus.Running));
      session.insertTablet(tablet(1));
      assertData(0, true);
      assertData(1, true);
      session.executeNonQueryStatement("FLUSH");
    }
    TestUtils.stopForciblyAndRestartDataNodes();
    assertData(0, true);
    assertData(1, true);
  }

  private static DataNodeWrapper nodeAtPort(int port) {
    return EnvFactory.getEnv().getDataNodeWrapperList().stream()
        .filter(node -> node.getPort() == port)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No DataNode at RPC port " + port));
  }

  @Test
  public void testDeleteAcrossRegionsKeepsPartialSuccessAndSupportsSafeRetry() throws Exception {
    final String[] targets = new String[2];
    final Map<String, Integer> owners = new HashMap<>();
    final Map<String, Integer> regions = new HashMap<>();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int candidate = 0; candidate < 8 && targets[1] == null; candidate++) {
        final String database = "root.delete_split_" + candidate;
        statement.execute("CREATE DATABASE " + database);
        statement.execute("CREATE TIMESERIES " + database + ".d.n INT64");
        statement.execute("CREATE TIMESERIES " + database + ".d.t TEXT");
        // Allocate both sides of the configured 1000-unit time-partition boundary before
        // making an owner read-only. No schema or partition creation is needed by DELETE.
        statement.execute(
            "INSERT INTO "
                + database
                + ".d(time,n,t) VALUES"
                + "(998,998,'keep-998'),(999,999,'keep-999'),(1000,1000,'keep-1000'),"
                + "(1001,1001,'keep-1001'),(1002,1002,'keep-1002')");
        try (ResultSet result = statement.executeQuery("SHOW REGIONS")) {
          while (result.next()) {
            if (database.equals(result.getString("Database"))
                && "DataRegion".equals(result.getString("Type"))) {
              assertEquals(null, owners.put(database, result.getInt("RpcPort")));
              regions.put(database, result.getInt("RegionId"));
            }
          }
        }
        assertNotNull("Missing DataRegion for " + database, owners.get(database));
        if (targets[0] == null) {
          targets[0] = database;
        } else if (!owners.get(targets[0]).equals(owners.get(database))) {
          targets[1] = database;
        }
      }
      statement.execute("FLUSH");
    }
    assertNotNull("No separately hosted Region in " + owners, targets[1]);
    assertNotEquals(regions.get(targets[0]), regions.get(targets[1]));
    assertNotEquals(owners.get(targets[0]), owners.get(targets[1]));
    final DataNodeWrapper healthy = nodeAtPort(owners.get(targets[0]));
    final DataNodeWrapper victim = nodeAtPort(owners.get(targets[1]));
    final String delete =
        "DELETE FROM "
            + targets[0]
            + ".d.n,"
            + targets[1]
            + ".d.n WHERE time >= 999 AND time <= 1001";
    try (Session session =
        new Session.Builder()
            .host(healthy.getIp())
            .port(healthy.getPort())
            .enableAutoFetch(false)
            .enableRedirection(false)
            .maxRetryCount(0)
            .build()) {
      session.open(false, 30_000);
      try {
        setStatus(victim, "READONLY");
        EnvFactory.getEnv()
            .ensureNodeStatus(
                Collections.singletonList(victim), Collections.singletonList(NodeStatus.ReadOnly));
        StatementExecutionException failure =
            assertThrows(
                StatementExecutionException.class, () -> session.executeNonQueryStatement(delete));
        assertEquals(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), failure.getStatusCode());
        // One original request really executed on one Region and was rejected on the other.
        // Cross-Region deletion does not promise atomic rollback.
        assertRegionDeleteRows(targets[0], true);
        assertRegionDeleteRows(targets[1], false);
      } finally {
        setStatus(victim, "RUNNING");
      }
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(victim), Collections.singletonList(NodeStatus.Running));
      // No writes occur between attempts. Repeating this same absolute range cannot remove
      // a later rewrite, unlike a retry interleaved with new writes within the deleted range.
      session.executeNonQueryStatement(delete);
      assertRegionDeleteRows(targets[0], true);
      assertRegionDeleteRows(targets[1], true);
      session.executeNonQueryStatement("FLUSH");
      assertRegionDeleteRows(targets[0], true);
      assertRegionDeleteRows(targets[1], true);
    }
  }

  private static void assertRegionDeleteRows(String database, boolean deleted) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery("SELECT n,t FROM " + database + ".d ORDER BY time")) {
      TestUtils.assertResultSetEqual(
          result,
          "Time," + database + ".d.n," + database + ".d.t,",
          new String[] {
            "998,998,keep-998,",
            "999," + (deleted ? "null" : "999") + ",keep-999,",
            "1000," + (deleted ? "null" : "1000") + ",keep-1000,",
            "1001," + (deleted ? "null" : "1001") + ",keep-1001,",
            "1002,1002,keep-1002,"
          });
    }
  }

  private static void setStatus(DataNodeWrapper node, String status) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node);
        Statement statement = connection.createStatement()) {
      statement.execute("SET SYSTEM TO " + status + " ON LOCAL");
    }
  }

  private Tablet tablet(int device) {
    Tablet tablet =
        new Tablet(
            databases[device] + ".d",
            Arrays.asList(
                new MeasurementSchema("n", TSDataType.INT64),
                new MeasurementSchema("t", TSDataType.TEXT)),
            4);
    long[] times = {1002, 2, 1001, 1};
    for (int row = 0; row < times.length; row++) {
      long time = times[row];
      tablet.addTimestamp(row, time);
      tablet.addValue("n", row, device * 10000L + time);
      tablet.addValue("t", row, time % 2 == 0 ? null : "device-" + device + "-time-" + time);
    }
    return tablet;
  }

  private void assertData(int device, boolean written) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT n,t FROM " + databases[device] + ".d")) {
      long[] times = written ? new long[] {0, 1, 2, 1000, 1001, 1002} : new long[] {0, 1000};
      for (long time : times) {
        assertTrue(result.next());
        assertEquals(time, result.getLong(1));
        boolean baseline = time == 0 || time == 1000;
        assertEquals(baseline ? -1 : device * 10000L + time, result.getLong(2));
        assertFalse(result.wasNull());
        assertEquals(
            baseline ? "baseline" : time % 2 == 0 ? null : "device-" + device + "-time-" + time,
            result.getString(3));
      }
      assertFalse(result.next());
    }
  }
}
