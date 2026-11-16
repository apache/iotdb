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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueriesWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.executeQueryWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeLifeCycleIT extends AbstractPipeTreeModelSharedClusterIT {

  private boolean permissionUserCreated;

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Override
  protected void cleanEnvironment() {
    super.cleanEnvironment();
    if (permissionUserCreated) {
      executeNonQueryWithRetry(senderEnv, "drop user test");
    }
  }

  @Test
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

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

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  // With history disabled, old data must stay excluded, new data/schema/database changes must be
  // replicated, and changes made while stopped must arrive only after restarting the pipe.
  @Test
  public void testLifeCycleWithHistoryDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");

      sourceAttributes.put("source.history.enable", "false");
      // start-time and end-time should not work
      sourceAttributes.put("source.history.start-time", "0001.01.01T00:00:00");
      sourceAttributes.put("source.history.end-time", "2100.01.01T00:00:00");
      sourceAttributes.put("user", "root");

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

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "create timeseries root.db.d1.s2 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (2, 2)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          Collections.singleton("2,2.0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("2,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.ln.**",
          "count(timeseries),",
          Collections.singleton("0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases root.db", "count,", Collections.singleton("1,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases root.ln", "count,", Collections.singleton("1,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create database root.ln0",
              "create timeseries root.db.d1.s3 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (3, 3)",
              "flush"),
          null);

      // Observe all stopped-pipe invariants in the same ten-second window, using one connection.
      // failFast preserves continuous checking: a transient leak must fail the test immediately.
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        Awaitility.await()
            .pollInSameThread()
            .pollDelay(0, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .during(10, TimeUnit.SECONDS)
            .atMost(15, TimeUnit.SECONDS)
            .failFast(
                () -> {
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("select s1 from root.db.d1"),
                      "Time,root.db.d1.s1,",
                      Collections.singleton("2,2.0,"));
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("count timeseries root.ln0.**"),
                      "count(timeseries),",
                      Collections.singleton("0,"));
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("count timeseries root.db.**"),
                      "count(timeseries),",
                      Collections.singleton("2,"));
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("count databases root.ln0"),
                      "count,",
                      Collections.singleton("0,"));
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("count databases root.db"),
                      "count,",
                      Collections.singleton("1,"));
                })
            .until(() -> true);
      }

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          new HashSet<>(Arrays.asList("2,2.0,", "3,3.0,")));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("3,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.ln0.**",
          "count(timeseries),",
          Collections.singleton("0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("3,"));
    }
  }

  @Test
  public void testLifeCycleLogMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "log");
      sourceAttributes.put("user", "root");

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

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      // A stopped pipe must keep the receiver data unchanged.
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleFileMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "file");
      sourceAttributes.put("user", "root");

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

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      // A stopped pipe must keep the receiver data unchanged.
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleHybridMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "hybrid");
      sourceAttributes.put("user", "root");

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

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      // A stopped pipe must keep the receiver data unchanged.
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  // A running pipe must recover after both clusters restart and replicate a third row in addition
  // to the history and realtime rows that were already received before the restart.
  @Test
  public void testLifeCycleWithClusterRestart() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Set<String> expectedResSet = new HashSet<>();
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

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

      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }

    executeOnBothEnvs(TestUtils::restartCluster);

    try (final SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  // Once realtime transfer is established, stop the receiver and enqueue the remaining rows during
  // its outage; restarting all receiver nodes must recover the pipe and deliver all 100 rows.
  @Test
  public void testReceiverRestartWhenTransferring() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

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

      insertData(senderEnv, 0, 1);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      // Explicitly enqueue events while the receiver is offline instead of relying on a writer's
      // sleep interval to overlap the restart. Failures propagate on the test thread.
      receiverEnv.shutdownAllDataNodes();
      receiverEnv.shutdownAllConfigNodes();
      insertData(senderEnv, 1, 100);
      receiverEnv.startAllConfigNodes();
      receiverEnv.startAllDataNodes();
      ((AbstractEnv) receiverEnv).checkClusterStatusWithoutUnknown();

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQuery(
          receiverEnv, "insert into root.db.d1(time, s1) values (1, 1)", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

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

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      // A stopped pipe must keep the receiver data unchanged.
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  // Bidirectional pipes with forwarding disabled must converge on all 400 history/realtime rows,
  // survive a restart of both clusters, then converge on 600 rows without a forwarding cycle.
  @Test
  public void testDoubleLiving() throws Exception {
    // Double living is two clusters with pipes connecting each other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    insertData(senderEnv, 0, 100);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

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
    insertData(senderEnv, 100, 200);
    insertData(receiverEnv, 200, 300);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

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
    insertData(receiverEnv, 300, 400);

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

    // Flush remotely received realtime rows before restart so recovery starts from persisted files.
    TestUtils.executeNonQuery(senderEnv, "flush", null);
    TestUtils.executeNonQuery(receiverEnv, "flush", null);

    // Keep one side of the bidirectional pipes online while the other cluster recovers.
    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);

    insertData(senderEnv, 400, 500);
    insertData(receiverEnv, 500, 600);

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
  }

  private static void insertData(final BaseEnv env, final int start, final int end)
      throws SQLException {
    // Preserve per-statement realtime events while avoiding a new JDBC session for every insert.
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      for (int i = start; i < end; ++i) {
        statement.execute(String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      statement.execute("flush");
    }
  }

  // A user without SYSTEM may list pipes but cannot manage pipes/plugins; granting SYSTEM allows
  // pipe lifecycle operations while an untrusted plugin URI must still be rejected.
  @Test
  public void testPermission() {
    createUser(senderEnv, "test", "IoTDB!Test2024");
    permissionUserCreated = true;

    assertNonQueryTestFail(
        senderEnv,
        "create pipe testPipe\n"
            + "with sink (\n"
            + "  'sink'='iotdb-thrift-sink',\n"
            + "  'sink.ip'='127.0.0.1',\n"
            + "  'sink.port'='6668'\n"
            + ")",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");
    // Will not throw exception
    executeQueryWithRetry(senderEnv, "show pipes", "test", "IoTDB!Test2024");
    assertNonQueryTestFail(
        senderEnv,
        "start pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");
    assertNonQueryTestFail(
        senderEnv,
        "stop pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipePlugin TestProcessor",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");
    assertTestFail(
        senderEnv,
        "show pipePlugins",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "IoTDB!Test2024");

    grantUserSystemPrivileges(senderEnv, "test", PrivilegeType.SYSTEM);

    executeNonQueryWithRetry(
        senderEnv,
        "create pipe testPipe\n" + "with sink (\n" + "  'sink'='write-back-sink'\n" + ")",
        "test",
        "IoTDB!Test2024");
    executeQueryWithRetry(senderEnv, "show pipes", "test", "IoTDB!Test2024");
    executeNonQueriesWithRetry(
        senderEnv,
        Arrays.asList("start pipe testPipe", "stop pipe testPipe", "drop pipe testPipe"),
        "test",
        "IoTDB!Test2024");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "701: Untrusted uri xxx",
        "test",
        "IoTDB!Test2024");
    executeQueryWithRetry(senderEnv, "show pipe plugins", "test", "IoTDB!Test2024");
  }
}
