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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.NodeDebugger;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import com.sun.jdi.ObjectReference;
import com.sun.jdi.StackFrame;
import com.sun.jdi.ThreadReference;
import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMapWithLeader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Exercises WAL reclamation while the replacement replica cannot yet accept incrementals. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBRegionReconstructWALIT {
  private static final String SERVER = "org.apache.iotdb.consensus.iot.IoTConsensusServerImpl";
  private static final int ROWS = 12000;

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testReconstructRetainsIncrementalsDuringSnapshot() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM");
    Properties properties = new Properties();
    properties.setProperty("wal_file_size_threshold_in_byte", "16384");
    // Cleanup is invoked explicitly at the controlled snapshot boundary below.
    properties.setProperty("delete_wal_files_period_in_ms", "3600000");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(properties);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
    Map<DataNodeWrapper, Integer> ports = new HashMap<>();
    for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      node.stopForcibly();
      MppJVMConfig jvm = new MppJVMConfig();
      jvm.setDebugPort(port);
      node.changeConfig(
          new MppDataNodeConfig(),
          (MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig(),
          jvm);
      node.start();
      ports.put(node, port);
    }
    EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Map<Integer, Pair<Integer, Set<Integer>>> regions;
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("create timeseries root.rebuild.d.s with datatype=INT64, encoding=PLAIN");
        statement.execute("insert into root.rebuild.d(time,s) values(0,0)");
        statement.execute("flush");
        regions = getDataRegionMapWithLeader(statement);
      }
      assertEquals(1, regions.size());
      int region = regions.keySet().iterator().next();
      Pair<Integer, Set<Integer>> peers = regions.get(region);
      int sourceId = peers.left;
      int targetId = peers.right.stream().filter(id -> id != sourceId).findFirst().get();
      DataNodeWrapper source = EnvFactory.getEnv().dataNodeIdToWrapper(sourceId).get();
      DataNodeWrapper target = EnvFactory.getEnv().dataNodeIdToWrapper(targetId).get();
      DataNodeWrapper coordinator =
          EnvFactory.getEnv().getDataNodeWrapperList().stream()
              .filter(node -> node != source && node != target)
              .findFirst()
              .get();
      try (Connection stableConnection = EnvFactory.getEnv().getConnection(coordinator);
          Statement stableStatement = stableConnection.createStatement();
          NodeDebugger debugger = new NodeDebugger(ports.get(source), SERVER, "transmitSnapshot")) {
        Future<?> reconstruct =
            executor.submit(
                () -> {
                  try (Connection c = EnvFactory.getEnv().getConnection(coordinator);
                      Statement s = c.createStatement()) {
                    s.execute("reconstruct region " + region + " on " + targetId);
                  }
                  return null;
                });
        // Snapshot creation is complete; only its transmission thread is paused. The replacement
        // remains inactive while ordinary writes and the real WAL cleaner keep running.
        StackFrame frame = debugger.await(candidate -> true);
        ThreadReference thread = frame.thread();
        ObjectReference server = frame.thisObject();
        ObjectReference wal =
            (ObjectReference)
                server.getValue(server.referenceType().fieldByName("consensusReqReader"));
        for (int row = 1; row <= ROWS; row++) {
          stableStatement.execute(
              "insert into root.rebuild.d(time,s) values(" + row + "," + row + ")");
        }
        // More requests than the dispatcher pipeline/queue can retain: replay must read WAL.
        stableStatement.execute("flush");
        wal.invokeMethod(
            thread,
            wal.referenceType().methodsByName("rollWALFile").get(0),
            Collections.emptyList(),
            ObjectReference.INVOKE_SINGLE_THREADED);
        wal.invokeMethod(
            thread,
            wal.referenceType().methodsByName("deleteOutdatedFiles").get(0),
            Collections.emptyList(),
            ObjectReference.INVOKE_SINGLE_THREADED);
        debugger.resume();
        reconstruct.get(3, TimeUnit.MINUTES);
        Awaitility.await()
            .atMost(3, TimeUnit.MINUTES)
            .until(
                () -> {
                  try (ResultSet status = stableStatement.executeQuery("show regions")) {
                    while (status.next()) {
                      if (status.getInt("RegionId") == region
                          && status.getInt("DataNodeId") == targetId) {
                        return "Running".equals(status.getString("Status"));
                      }
                    }
                    return false;
                  }
                });
        // Isolate each data replica. Connecting to different coordinators alone does not guarantee
        // that the queries read different replicas.
        source.stopForcibly();
        assertRows(stableStatement);
        source.start();
        EnvFactory.getAbstractEnv().checkNodeInStatus(sourceId, NodeStatus.Running);
        target.stopForcibly();
        assertRows(stableStatement);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static void assertRows(Statement statement) {
    Awaitility.await()
        .ignoreExceptionsInstanceOf(SQLException.class)
        .atMost(1, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              try (ResultSet rows =
                  statement.executeQuery("select s from root.rebuild.d order by time")) {
                int expected = 0;
                while (rows.next()) {
                  assertEquals(expected, rows.getLong(1));
                  assertEquals(expected, rows.getLong(2));
                  expected++;
                }
                assertEquals(ROWS + 1, expected);
              }
            });
  }
}
