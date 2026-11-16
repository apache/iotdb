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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeAutoConflictIT extends AbstractPipeTreeModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  private ISession senderSession;
  private ISession receiverSession;
  private boolean clustersReady;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    clustersReady = true;
  }

  @Override
  protected void setupConfig() {

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  // Both IOT-consensus clusters must merge 600 disjoint rows despite automatic schema conflicts,
  // including the final 200 rows written after both clusters restart.
  @Test
  public void testDoubleLivingAutoConflict() throws Exception {
    // Double living is two clusters each with a pipe connecting to the other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    insertData(senderEnv, 0, 100, true);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      // Add this property to avoid making self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    insertData(senderEnv, 100, 200, true);

    insertData(receiverEnv, 200, 300, true);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      // Add this property to avoid to make self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderIp);
      sinkAttributes.put("sink.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    insertData(receiverEnv, 300, 400, true);

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);

    // Close pre-restart sessions, then overlap the required restarts. A restart failure must fail
    // the test rather than skip the assertions on the final 200 rows.
    closeWriteSessions();
    executeOnBothEnvs(TestUtils::restartCluster);

    insertData(senderEnv, 400, 500, true);

    insertData(receiverEnv, 500, 600, true);

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);
  }

  // Bidirectional pipes must auto-extend a shared template for s1 and merge 400 rows on both sides.
  @Test
  public void testDoubleLivingAutoConflictTemplate() throws Exception {
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderIp);
      sinkAttributes.put("sink.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            // Auto extend s1
            "create schema template t1 (s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            isolateDatabasePaths("create database root.db"),
            isolateDatabasePaths("set device template t1 to root.db")),
        null);

    // Prevent receiver writes from creating ordinary timeseries before the template binding
    // arrives.
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show paths set device template t1",
        "Paths,",
        Collections.singleton(isolateDatabasePaths("root.db,")));

    insertData(senderEnv, 0, 200, false);

    // SHOW PATHS can expose a pre-set binding before the DataNodes can use the template.
    // Retry the first idempotent write until the receiver can activate and extend the template.
    TestUtils.executeNonQueryWithRetry(
        receiverEnv, isolateDatabasePaths("insert into root.db.d1(time, s1) values (200, 1)"));

    insertData(receiverEnv, 201, 400, false);

    writeSession(senderEnv).executeNonQueryStatement("flush");

    writeSession(receiverEnv).executeNonQueryStatement("flush");

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        isolateDatabasePaths("select s1 from root.db.d1"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        isolateDatabasePaths("select s1 from root.db.d1"),
        isolateDatabasePaths("Time,root.db.d1.s1,"),
        expectedResSet);
  }

  // A manually created receiver series must acquire the sender tag and attribute when auto-created
  // schema events race with the existing series.
  @Test
  public void testAutoManualCreateRace() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.executeNonQuery(
          receiverEnv,
          isolateDatabasePaths("create timeSeries root.ln.wf01.wt01.status with datatype=BOOLEAN"),
          null);

      TestUtils.executeNonQuery(
          senderEnv,
          isolateDatabasePaths(
              "create timeSeries root.ln.wf01.wt01.status with datatype=BOOLEAN tags (tag3=v3) attributes (attr4=v4)"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("show timeSeries root.ln.**"),
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.singleton(
              isolateDatabasePaths(
                  "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,RLE,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,")));
    }
  }

  // Historical sync must preserve an aligned template binding and all three activated devices,
  // including a device activated by a negative timestamp and a later template extension.
  @Test
  public void testHistoricalActivationRace() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("create database root.sg_aligned"),
              "create device template aligned_template aligned (s0 int32, s1 int64, s2 float, s3 double, s4 boolean, s5 text)",
              isolateDatabasePaths(
                  "set device template aligned_template to root.sg_aligned.device_aligned"),
              isolateDatabasePaths(
                  "create timeseries using device template on root.sg_aligned.device_aligned.d10"),
              isolateDatabasePaths(
                  "create timeseries using device template on root.sg_aligned.device_aligned.d12"),
              isolateDatabasePaths(
                  "insert into root.sg_aligned.device_aligned.d10(time, s0, s1, s2,s3,s4,s5) values (1706659200,1706659200,10,20.245,25.24555,true,''),(1706662800,null,1706662800,20.241,25.24111,false,'2'),(1706666400,3,null,20.242,25.24222,true,'3'),(1706670000,4,40,null,35.5474,true,'4'),(1706670600,5,1706670600000,20.246,null,false,'5'),(1706671200,6,60,20.248,25.24888,null,'6'),(1706671800,7,1706671800,20.249,25.24999,false,null),(1706672400,8,80,1245.392,75.51234,false,'8'),(1706672600,9,90,2345.397,2285.58734,false,'9'),(1706673000,10,100,20.241,25.24555,false,'10'),(1706673600,11,110,3345.394,4105.544,false,'11'),(1706674200,12,1706674200,30.245,35.24555,false,'12'),(1706674800,13,130,5.39,125.51234,false,'13'),(1706675400,14,1706675400,5.39,135.51234,false,'14'),(1706676000,15,150,5.39,145.51234,false,'15'),(1706676600,16,160,5.39,155.51234,false,'16'),(1706677200,17,170,5.39,165.51234,false,'17'),(1706677600,18,180,5.39,175.51234,false,'18'),(1706677800,19,190,5.39,185.51234,false,'19'),(1706678000,20,200,5.39,195.51234,false,'20'),(1706678200,21,210,5.39,null,false,'21')"),
              isolateDatabasePaths(
                  "insert into root.sg_aligned.device_aligned.d10(time, s0, s1, s2,s3,s4,s5) values (-1,1,10,5.39,5.51234,false,'negative')"),
              isolateDatabasePaths(
                  "insert into root.sg_aligned.device_aligned.d11(time, s0, s1, s2,s3,s4,s5) values (-1,-11,-110,-5.39,-5.51234,false,'activate:1')"),
              isolateDatabasePaths(
                  "insert into root.sg_aligned.device_aligned.d10(time, s0, s1, s2,s3,s4,s5,s6) values(1706678800,1,1706678800,5.39,5.51234,false,'add:s6',32);")),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show paths set device template aligned_template",
          "Paths,",
          Collections.singleton(isolateDatabasePaths("root.sg_aligned.device_aligned,")));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("count devices root.sg_aligned.**"),
          "count(devices),",
          Collections.singleton("3,"));
    }
  }

  private String isolateDatabasePaths(final String sql) {
    // A stable short suffix isolates each method without changing device names or template scope.
    return sql.replaceAll(
        "root\\.(db|ln|sg_aligned)(?=[.,; ]|$)",
        "$0_" + Integer.toUnsignedString(testName.getMethodName().hashCode(), 16));
  }

  private ISession writeSession(final BaseEnv env) throws Exception {
    if (env == senderEnv) {
      if (senderSession == null) {
        senderSession = env.getSessionConnection();
      }
      return senderSession;
    }
    if (receiverSession == null) {
      receiverSession = env.getSessionConnection();
    }
    return receiverSession;
  }

  private void insertData(final BaseEnv env, final int start, final int end, final boolean flush)
      throws Exception {
    final ISession session = writeSession(env);
    // Keep the point count and phase boundaries, while reducing INSERT RPCs to one tenth.
    for (int batchStart = start; batchStart < end; batchStart += 10) {
      final StringBuilder sql =
          new StringBuilder(isolateDatabasePaths("insert into root.db.d1(time, s1) values "));
      for (int i = batchStart; i < Math.min(batchStart + 10, end); ++i) {
        if (i > batchStart) {
          sql.append(',');
        }
        sql.append('(').append(i).append(",1)");
      }
      session.executeNonQueryStatement(sql.toString());
    }
    if (flush) {
      session.executeNonQueryStatement("flush");
    }
  }

  private void closeWriteSessions() throws Exception {
    try {
      if (senderSession != null) {
        senderSession.close();
      }
    } finally {
      senderSession = null;
      try {
        if (receiverSession != null) {
          receiverSession.close();
        }
      } finally {
        receiverSession = null;
      }
    }
  }

  @Override
  protected void cleanEnvironment() {
    try {
      closeWriteSessions();
    } catch (final Exception e) {
      throw new AssertionError("Failed to close conflict test sessions", e);
    } finally {
      // Drop pipes before databases so neither side can recreate data while cleanup runs.
      super.cleanEnvironment();
      if (clustersReady) {
        // Deleting databases removes bindings/activations, but template definitions survive it.
        // Reset them explicitly to prevent one method's schema from changing another method.
        for (final BaseEnv env : new BaseEnv[] {senderEnv, receiverEnv}) {
          try (final Connection connection = env.getConnection();
              final Statement statement = connection.createStatement()) {
            final List<String> templates = new ArrayList<>();
            try (final ResultSet result = statement.executeQuery("show schema templates")) {
              while (result.next()) {
                templates.add(result.getString(1));
              }
            }
            for (final String template : templates) {
              statement.execute("drop schema template `" + template.replace("`", "``") + "`");
            }
          } catch (final Exception e) {
            throw new AssertionError("Failed to reset conflict test templates", e);
          }
        }
      }
    }
  }
}
