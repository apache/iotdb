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

package org.apache.iotdb.confignode.it;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.utils.KillPoint.MetadataTestKillPoints;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.util.MetadataRecoveryTestUtils.Gate;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.util.MetadataRecoveryTestUtils.assertRows;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.connection;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.execute;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.property;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.schemaRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** P1-8: restore an in-use catalog snapshot and its later DDL, beyond partition-only snapshots. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBMetadataCatalogSnapshotRecoveryIT {
  private static final String DATABASE = "root.p1_catalog";
  private static final String TABLE_DATABASE = "p1_catalog_table";

  @Before
  public void before() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaEngineMode("Memory")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3)
        .setConfigNodeRatisSnapshotTriggerThreshold(Integer.MAX_VALUE);
    property(
        EnvFactory.getEnv().getConfig().getConfigNodeCommonConfig(),
        "config_node_ratis_preserve_logs_num_when_purge",
        "10000");
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @After
  public void after() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCatalogSnapshotAndSubsequentDdlSurviveLeaderRecovery() throws Exception {
    long time = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1);
    int oldLeaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    ConfigNodeWrapper recovering = EnvFactory.getEnv().getConfigNodeWrapper(oldLeaderIndex);
    try (Connection tree =
            connection(EnvFactory.getEnv().getDataNodeWrapper(0), BaseEnv.TREE_SQL_DIALECT);
        Connection table =
            connection(EnvFactory.getEnv().getDataNodeWrapper(0), BaseEnv.TABLE_SQL_DIALECT)) {
      execute(
          tree,
          "CREATE DATABASE " + DATABASE,
          "CREATE DEVICE TEMPLATE p1_catalog_template (s INT64 ENCODING=PLAIN COMPRESSION=LZ4)",
          "SET DEVICE TEMPLATE p1_catalog_template TO " + DATABASE + ".templ",
          "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + DATABASE + ".templ.d",
          "INSERT INTO " + DATABASE + ".templ.d(time,s) VALUES(" + time + ",10)",
          "CREATE TIMESERIES " + DATABASE + ".source.d.s INT32",
          "INSERT INTO " + DATABASE + ".source.d(time,s) VALUES(" + time + ",11)",
          "CREATE DEVICE TEMPLATE p1_catalog_deleted (s INT64)",
          "SET DEVICE TEMPLATE p1_catalog_deleted TO " + DATABASE + ".unused",
          "SET TTL TO " + DATABASE + " 604800000");
      execute(
          table,
          "CREATE DATABASE " + TABLE_DATABASE,
          "USE " + TABLE_DATABASE,
          "CREATE TABLE catalog_table(device STRING TAG,owner STRING ATTRIBUTE,value INT32 FIELD,removed INT32 FIELD) WITH (ttl=604800000)",
          "INSERT INTO catalog_table(time,device,owner,value,removed) VALUES("
              + time
              + ",'d1','before',1,100)",
          "CREATE TABLE deleted_table(device STRING TAG,value INT32 FIELD)",
          "CREATE VIEW catalog_view(device STRING TAG) AS " + DATABASE + ".source.**",
          "CREATE VIEW deleted_view(device STRING TAG) AS " + DATABASE + ".source.**");
      int partition = schemaRegion(DATABASE, DATABASE + ".templ.d.s");
      TShowClusterResp cluster;
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        cluster = client.showCluster();
      }
      RaftPeerId recoveredPeer =
          Utils.fromNodeIdToRaftPeerId(
              cluster.getConfigNodeList().stream()
                  .filter(node -> node.getInternalEndPoint().getPort() == recovering.getPort())
                  .findFirst()
                  .orElseThrow(AssertionError::new)
                  .getConfigNodeId());
      try (RaftClient raft = raftClient(cluster)) {
        assertTrue(
            "Snapshot must complete before the following DDL",
            raft.getSnapshotManagementApi(recoveredPeer).create(true, 30000).isSuccess());
        recovering.stopForcibly();
        assertNotEquals(oldLeaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex());
        execute(
            tree,
            "ALTER DEVICE TEMPLATE p1_catalog_template ADD(extra INT64 ENCODING=PLAIN COMPRESSION=LZ4)",
            "INSERT INTO " + DATABASE + ".templ.d(time,extra) VALUES(" + (time + 1) + ",20)",
            "UNSET DEVICE TEMPLATE p1_catalog_deleted FROM " + DATABASE + ".unused",
            "DROP DEVICE TEMPLATE p1_catalog_deleted",
            "SET TTL TO " + DATABASE + " 86400000");
        execute(
            table,
            "ALTER TABLE catalog_table ADD COLUMN after_snapshot DOUBLE FIELD",
            "ALTER TABLE catalog_table RENAME COLUMN owner TO owner_after",
            "ALTER TABLE catalog_table DROP COLUMN removed",
            "ALTER TABLE catalog_table SET PROPERTIES ttl=86400000",
            "INSERT INTO catalog_table(time,device,owner_after,value,after_snapshot) VALUES("
                + (time + 1)
                + ",'d2','after',2,2.5)",
            "ALTER VIEW catalog_view RENAME TO current_view",
            "DROP VIEW deleted_view",
            "DROP TABLE deleted_table");
        try (Gate loaded =
            new Gate(recovering, MetadataTestKillPoints.AFTER_CONFIG_SNAPSHOT_LOAD, false)) {
          recovering.start();
          loaded.await();
          EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
        }
        // Keep the other two peers alive for quorum while this particular recovered peer takes
        // over.
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    assertTrue(raft.admin().transferLeadership(recoveredPeer, 10000).isSuccess()));
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () -> assertEquals(oldLeaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex()));
      }
      assertEquals(partition, schemaRegion(DATABASE, DATABASE + ".templ.d.s"));
      for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (Connection readTree = connection(node, BaseEnv.TREE_SQL_DIALECT);
            Connection readTable = connection(node, BaseEnv.TABLE_SQL_DIALECT)) {
          assertRows(readTree, "SHOW DEVICE TEMPLATES", "TemplateName,", "p1_catalog_template,");
          assertRows(
              readTree,
              "SHOW NODES IN DEVICE TEMPLATE p1_catalog_template",
              "ChildNodes,DataType,Encoding,Compression,",
              "s,INT64,PLAIN,LZ4,",
              "extra,INT64,PLAIN,LZ4,");
          assertRows(
              readTree,
              "SHOW PATHS SET DEVICE TEMPLATE p1_catalog_template",
              "Paths,",
              DATABASE + ".templ,");
          assertRows(
              readTree,
              "SHOW PATHS USING DEVICE TEMPLATE p1_catalog_template",
              "Paths,",
              DATABASE + ".templ.d,");
          assertRows(readTree, "SHOW TTL ON " + DATABASE, "Path,TTL(ms),", DATABASE + ",86400000,");
          assertRows(
              readTree,
              "SELECT s,extra FROM " + DATABASE + ".templ.d",
              "Time," + DATABASE + ".templ.d.s," + DATABASE + ".templ.d.extra,",
              time + ",10,null,",
              (time + 1) + ",null,20,");
          execute(readTable, "USE " + TABLE_DATABASE);
          assertRows(
              readTable,
              "DESCRIBE catalog_table",
              "ColumnName,DataType,Category,",
              "time,TIMESTAMP,TIME,",
              "device,STRING,TAG,",
              "owner_after,STRING,ATTRIBUTE,",
              "value,INT32,FIELD,",
              "after_snapshot,DOUBLE,FIELD,");
          assertRows(
              readTable,
              "SHOW TABLES",
              "TableName,TTL(ms),",
              "catalog_table,86400000,",
              "current_view,INF,");
          assertRows(
              readTable,
              "SELECT CAST(time AS INT64) AS ts,device,owner_after,value,after_snapshot FROM catalog_table",
              "ts,device,owner_after,value,after_snapshot,",
              time + ",d1,before,1,null,",
              (time + 1) + ",d2,after,2,2.5,");
          assertRows(
              readTable,
              "SELECT CAST(time AS INT64) AS ts,device,s FROM current_view",
              "ts,device,s,",
              time + ",d,11,");
        }
      }
      execute(
          table,
          "ALTER TABLE catalog_table ADD COLUMN continued BOOLEAN FIELD",
          "INSERT INTO catalog_table(time,device,continued) VALUES(" + (time + 2) + ",'d1',true)");
      assertRows(
          table,
          "SELECT CAST(time AS INT64) AS ts,device,continued FROM catalog_table WHERE continued IS NOT NULL",
          "ts,device,continued,",
          (time + 2) + ",d1,true,");
      execute(
          tree,
          "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + DATABASE + ".templ.fresh",
          "INSERT INTO " + DATABASE + ".templ.fresh(time,extra) VALUES(" + (time + 2) + ",30)");
      assertRows(
          tree,
          "SELECT extra FROM " + DATABASE + ".templ.fresh",
          "Time," + DATABASE + ".templ.fresh.extra,",
          (time + 2) + ",30,");
    } finally {
      if (!recovering.isAlive()) {
        recovering.start();
      }
    }
  }

  private RaftClient raftClient(TShowClusterResp cluster) {
    RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(
        properties, TimeDuration.valueOf(35, TimeUnit.SECONDS));
    return RaftClient.newBuilder()
        .setRaftGroup(
            RaftGroup.valueOf(
                Utils.fromConsensusGroupIdToRaftGroupId(
                    new ConfigRegionId(
                        ConfigNodeDescriptor.getInstance().getConf().getConfigRegionId())),
                cluster.getConfigNodeList().stream()
                    .map(
                        node ->
                            Utils.fromNodeInfoAndPriorityToRaftPeer(
                                node.getConfigNodeId(), node.getConsensusEndPoint(), 0))
                    .collect(Collectors.toList())))
        .setProperties(properties)
        .setRetryPolicy(RetryPolicies.noRetry())
        .build();
  }
}
