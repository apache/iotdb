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

package org.apache.iotdb.confignode.it.removeconfignode;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultAgent;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController.Node;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransport;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBRemoveConfigNodeSchemaPartitionIT {

  private static final String DATABASE = "cn_removal";
  private MetadataFaultController faultController;
  private Node controlledDataNode;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        // Keep the synchronous procedure wait longer than the bounded ConfigNode-removal wait.
        .setDnConnectionTimeoutMs(180_000)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Seed the only slot before removal, so this test exercises reads of existing partitions.
        .setSeriesSlotNum(1)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        // Keep the DDL's cache propagation active throughout the bounded ConfigNode-removal wait.
        .setMetadataLeaseFenceMs(180_000);
    faultController =
        new MetadataFaultController(
            (MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig());
    try {
      EnvFactory.getEnv().initClusterEnvironment(3, 1);
    } finally {
      faultController.restoreLaunchEnvironment();
    }
    controlledDataNode = faultController.connect(EnvFactory.getEnv().getDataNodeWrapper(0));
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("USE " + DATABASE);
      session.executeNonQueryStatement(
          "CREATE TABLE devices (device STRING TAG, value INT64 FIELD)");
      insertDevice(session, 0);
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (faultController != null) {
        faultController.reset();
      }
    } finally {
      try {
        EnvFactory.getEnv().cleanClusterEnvironment();
      } finally {
        if (faultController != null) {
          faultController.close();
        }
      }
    }
  }

  @Test
  public void testSchemaPartitionOnDeletedConfigRegion() throws Exception {
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    final int removedIndex = (leaderIndex + 1) % 3;
    final ConfigNodeWrapper removedNode = EnvFactory.getEnv().getConfigNodeWrapper(removedIndex);
    final TEndPoint removedEndpoint = new TEndPoint(removedNode.getIp(), removedNode.getPort());
    final Map<String, List<TSeriesPartitionSlot>> request =
        Collections.singletonMap(DATABASE, Collections.emptyList());

    try (SyncConfigNodeIServiceClient leader =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        SyncConfigNodeIServiceClient oldConnection =
            (SyncConfigNodeIServiceClient)
                EnvFactory.getEnv().getConfigNodeConnection(removedIndex)) {
      final List<TConfigNodeLocation> locations = leader.showCluster().getConfigNodeList();
      final TConfigNodeLocation removedLocation =
          locations.stream()
              .filter(location -> location.getInternalEndPoint().equals(removedEndpoint))
              .findFirst()
              .get();
      final TSchemaPartitionTableResp expected = leader.getSchemaPartitionTableWithSlots(request);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), expected.getStatus().getCode());
      assertFalse(expected.getSchemaPartitionTable().get(DATABASE).isEmpty());

      final List<TEndPoint> staleEndpoints = new ArrayList<>();
      staleEndpoints.add(removedEndpoint);
      locations.stream()
          .map(TConfigNodeLocation::getInternalEndPoint)
          .filter(endpoint -> !endpoint.equals(removedEndpoint))
          .forEach(staleEndpoints::add);
      // Use the actual DN retry client against real CN processes. This client is not pooled.
      try (ConfigNodeClient retryClient =
          new ConfigNodeClient(
              staleEndpoints,
              new ThriftClientProperty.Builder().setConnectionTimeoutMs(30_000).build(),
              null) {
            @Override
            public void close() {
              invalidate();
            }
          }) {
        assertEquals(removedEndpoint, retryClient.getConfigNode());
        final TTransport oldTransport = retryClient.getTransport();
        oldConnection.setTimeout(30_000);

        // Execute the same deletion RPC as RemoveConfigNodeProcedure, but keep the process alive.
        // This deterministically holds the delete-peer / stop-node window without racing its 5s
        // timer.
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            oldConnection.deleteConfigNodePeer(removedLocation).getCode());
        final TSchemaPartitionTableResp rejected =
            oldConnection.getSchemaPartitionTableWithSlots(request);
        assertEquals(
            rejected.getStatus().toString(),
            TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(),
            rejected.getStatus().getCode());
        assertFalse(rejected.getStatus().isSetRedirectNode());
        assertFalse(rejected.isSetSchemaPartitionTable());

        // The old TCP connection must receive a retryable response, then move to a live CN.
        assertTrue(oldTransport.isOpen());
        final TSchemaPartitionTableResp recovered =
            retryClient.getSchemaPartitionTableWithSlots(request);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), recovered.getStatus().getCode());
        assertEquals(expected.getSchemaPartitionTable(), recovered.getSchemaPartitionTable());
        assertNotSame(oldTransport, retryClient.getTransport());
        assertFalse(removedEndpoint.equals(retryClient.getConfigNode()));
      }
    }
  }

  @Test
  public void testTableReadWriteOnExistingPartitionsDuringLeaderRemoval() throws Exception {
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    final ConfigNodeWrapper removedNode = EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex);
    final int removedId;
    try (SyncConfigNodeIServiceClient leader =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      removedId =
          leader.showCluster().getConfigNodeList().stream()
              .filter(location -> location.getInternalEndPoint().getPort() == removedNode.getPort())
              .findFirst()
              .get()
              .getConfigNodeId();
    }

    final AtomicBoolean stopped = new AtomicBoolean();
    final CountDownLatch started = new CountDownLatch(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Integer> workload =
        executor.submit(
            () -> {
              try (ITableSession session =
                  EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
                int inserted = 0;
                while (!stopped.get()) {
                  // New devices exercise metadata fetching; an unrestricted query traverses
                  // devices.
                  insertDevice(session, ++inserted);
                  assertRowCount(session, inserted + 1);
                  started.countDown();
                  TimeUnit.MILLISECONDS.sleep(50);
                }
                return inserted;
              }
            });
    try {
      assertTrue("The table workload did not start", started.await(30, TimeUnit.SECONDS));
      try (ITableSession admin = EnvFactory.getEnv().getTableSessionConnection()) {
        admin.executeNonQueryStatement("REMOVE CONFIGNODE " + removedId);
      }
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (SyncConfigNodeIServiceClient leader =
                    (SyncConfigNodeIServiceClient)
                        EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
                  final List<TConfigNodeLocation> remaining =
                      leader.showCluster().getConfigNodeList();
                  assertEquals(2, remaining.size());
                  assertTrue(
                      remaining.stream().noneMatch(node -> node.getConfigNodeId() == removedId));
                }
                assertFalse(removedNode.getInstance().isAlive());
              });
      stopped.set(true);
      // Propagate every SELECT/insertTablet failure; do not hide failures with application retries.
      final int inserted = workload.get(60, TimeUnit.SECONDS);
      assertTrue(inserted > 0);
      try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
        insertDevice(session, inserted + 1);
        assertRowCount(session, inserted + 2);
      }
    } finally {
      stopped.set(true);
      workload.cancel(true);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testTableDdlThroughRemovedLeaderDuringConfigNodeRemoval() throws Exception {
    final int removedIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    final ConfigNodeWrapper removedNode = EnvFactory.getEnv().getConfigNodeWrapper(removedIndex);
    final int removedId;
    try (SyncConfigNodeIServiceClient leader =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      removedId =
          leader.showCluster().getConfigNodeList().stream()
              .filter(location -> location.getInternalEndPoint().getPort() == removedNode.getPort())
              .findFirst()
              .get()
              .getConfigNodeId();
    }

    final ExecutorService ddlExecutor = Executors.newFixedThreadPool(2);
    Future<?> ddl = null;
    Future<?> removal = null;
    try {
      controlledDataNode.arm(
          MetadataFaultAgent.UPDATE_TABLE,
          "HOLD_FIRST_DROP_RETRIES",
          MetadataFaultAgent.COMMIT_UPDATE_TABLE,
          "");
      ddl =
          ddlExecutor.submit(
              () -> {
                try (ITableSession ddlSession =
                    EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
                  ddlSession.executeNonQueryStatement(
                      "ALTER TABLE devices ADD COLUMN added INT64 FIELD");
                }
                return null;
              });
      controlledDataNode.awaitBlocking(MetadataFaultAgent.UPDATE_TABLE);
      assertDdlStillAtCommitRelease(ddl);
      try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
        TestUtils.tableResultSetEqualTest(
            "DESCRIBE devices",
            new String[] {"ColumnName", "DataType", "Category"},
            new String[] {
              "time,TIMESTAMP,TIME,",
              "device,STRING,TAG,",
              "value,INT64,FIELD,",
              "added,INT64,FIELD,"
            },
            DATABASE);

        removal =
            ddlExecutor.submit(
                () -> {
                  try (ITableSession removalSession =
                      EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
                    removalSession.executeNonQueryStatement("REMOVE CONFIGNODE " + removedId);
                  }
                  return null;
                });
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    assertTrue(
                        "The cluster should elect a different leader while the removal is running",
                        EnvFactory.getEnv().getLeaderConfigNodeIndex() != removedIndex));

        final Map<String, String> gateStatus =
            controlledDataNode.status(MetadataFaultAgent.UPDATE_TABLE);
        assertTrue(
            "The COMMIT_UPDATE_TABLE RPC should still be blocked after ConfigNode removal",
            Long.parseLong(gateStatus.get("blocking")) > 0);
        assertEquals("0", gateStatus.get("passed"));
        assertDdlStillAtCommitRelease(ddl);
        controlledDataNode.release(MetadataFaultAgent.UPDATE_TABLE);
        // A successful response here means the original ConfigNode procedure reached terminal
        // success.
        ddl.get(90, TimeUnit.SECONDS);
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  final Map<String, String> releasedStatus =
                      controlledDataNode.status(MetadataFaultAgent.UPDATE_TABLE);
                  assertEquals("0", releasedStatus.get("blocking"));
                  assertTrue(
                      "COMMIT_UPDATE_TABLE did not return successfully: " + releasedStatus,
                      Long.parseLong(releasedStatus.get("acknowledged")) > 0);
                });

        removal.get(90, TimeUnit.SECONDS);
        Awaitility.await()
            .atMost(90, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try (SyncConfigNodeIServiceClient currentLeader =
                      (SyncConfigNodeIServiceClient)
                          EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
                    final List<TConfigNodeLocation> remaining =
                        currentLeader.showCluster().getConfigNodeList();
                    assertEquals(2, remaining.size());
                    assertTrue(
                        remaining.stream().noneMatch(node -> node.getConfigNodeId() == removedId));
                  }
                  assertFalse(removedNode.getInstance().isAlive());
                });

        Awaitility.await()
            .atMost(90, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.tableResultSetEqualTest(
                        "DESCRIBE devices",
                        new String[] {"ColumnName", "DataType", "Category"},
                        new String[] {
                          "time,TIMESTAMP,TIME,",
                          "device,STRING,TAG,",
                          "value,INT64,FIELD,",
                          "added,INT64,FIELD,"
                        },
                        DATABASE));
        session.executeNonQueryStatement(
            "INSERT INTO devices(time, device, value, added) VALUES (1, 'd0', 1, 100)");
        session.executeNonQueryStatement(
            "ALTER TABLE devices ADD COLUMN after_removal INT32 FIELD");
        session.executeNonQueryStatement(
            "INSERT INTO devices(time, device, value, added, after_removal) "
                + "VALUES (2, 'd0', 2, 200, 7)");

        TestUtils.tableResultSetEqualTest(
            "DESCRIBE devices",
            new String[] {"ColumnName", "DataType", "Category"},
            new String[] {
              "time,TIMESTAMP,TIME,",
              "device,STRING,TAG,",
              "value,INT64,FIELD,",
              "added,INT64,FIELD,",
              "after_removal,INT32,FIELD,"
            },
            DATABASE);

        try (SessionDataSet counts =
            session.executeQueryStatement(
                "SELECT count(value), count(added), count(after_removal) FROM devices")) {
          assertTrue(counts.hasNext());
          final RowRecord countRow = counts.next();
          assertEquals(3L, countRow.getFields().get(0).getLongV());
          assertEquals(2L, countRow.getFields().get(1).getLongV());
          assertEquals(1L, countRow.getFields().get(2).getLongV());
          assertFalse(counts.hasNext());
        }
        try (SessionDataSet addedRow =
            session.executeQueryStatement(
                "SELECT value, added FROM devices WHERE device = 'd0' AND time = 1")) {
          assertTrue(addedRow.hasNext());
          final RowRecord record = addedRow.next();
          assertEquals(1L, record.getFields().get(0).getLongV());
          assertEquals(100L, record.getFields().get(1).getLongV());
          assertFalse(addedRow.hasNext());
        }
        try (SessionDataSet originalRow =
            session.executeQueryStatement(
                "SELECT value FROM devices WHERE device = 'd0' AND time = 0")) {
          assertTrue(originalRow.hasNext());
          assertEquals(0L, originalRow.next().getFields().get(0).getLongV());
          assertFalse(originalRow.hasNext());
        }
        try (SessionDataSet row =
            session.executeQueryStatement(
                "SELECT value, added, after_removal FROM devices "
                    + "WHERE device = 'd0' AND time = 2")) {
          assertTrue(row.hasNext());
          final RowRecord record = row.next();
          assertEquals(2L, record.getFields().get(0).getLongV());
          assertEquals(200L, record.getFields().get(1).getLongV());
          assertEquals(7L, record.getFields().get(2).getIntV());
          assertFalse(row.hasNext());
        }
      }
    } finally {
      try {
        controlledDataNode.release(MetadataFaultAgent.UPDATE_TABLE);
      } finally {
        if (ddl != null && !ddl.isDone()) {
          ddl.cancel(true);
        }
        if (removal != null && !removal.isDone()) {
          removal.cancel(true);
        }
        ddlExecutor.shutdownNow();
        assertTrue(ddlExecutor.awaitTermination(60, TimeUnit.SECONDS));
      }
    }
  }

  private static void assertDdlStillAtCommitRelease(Future<?> ddl) throws Exception {
    try {
      ddl.get(0, TimeUnit.NANOSECONDS);
      throw new AssertionError("The ALTER TABLE completed while COMMIT_UPDATE_TABLE was blocked");
    } catch (TimeoutException expected) {
      // The client is still waiting for the procedure.
    } catch (ExecutionException e) {
      throw new AssertionError(
          "ALTER TABLE failed while COMMIT_UPDATE_TABLE was blocked", e.getCause());
    }
  }

  private static void insertDevice(ITableSession session, int index) throws Exception {
    final Tablet tablet =
        new Tablet(
            "devices",
            Arrays.asList("device", "value"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, index);
    tablet.addValue("device", 0, "d" + index);
    tablet.addValue("value", 0, (long) index);
    session.insert(tablet);
  }

  private static void assertRowCount(ITableSession session, int expected) throws Exception {
    try (SessionDataSet result =
        session.executeQueryStatement("SELECT count(value) FROM devices")) {
      assertTrue(result.hasNext());
      assertEquals(expected, result.next().getFields().get(0).getLongV());
      assertFalse(result.hasNext());
    }
  }
}
