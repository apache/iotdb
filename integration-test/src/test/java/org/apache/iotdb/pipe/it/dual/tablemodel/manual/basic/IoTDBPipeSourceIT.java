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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeSourceIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  @Override
  protected void setupConfig() {
    // Retain the shared fixture's single-Region database budget. Extra matching Regions can
    // otherwise mask excluded-only owners when completion is checked across multiple DataNodes.
    super.setupConfig();
    senderEnv
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("JMX"));
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Disable sender compaction for tsfile determination in loose range test
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
  }

  // Database-name filters and SQL model isolation must capture matching table data and reject
  // operations from the wrong dialect. Completion checks must ignore Region owners whose
  // databases are excluded by both model filters.
  @Test
  public void testMatchingMultipleDatabases() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("extractor.database-name", database("test"));
      extractorAttributes.put("extractor.table-name", "test");
      extractorAttributes.put("extractor.pattern", isolateDatabasePaths("root.db1"));
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      assertTimeseriesCountOnReceiver(receiverEnv, 0);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db1.d1 (time, at1) values (1, 10)"),
              isolateDatabasePaths("insert into root.db2.d1 (time, at1) values (1, 20)"),
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      extractorAttributes.replace("extractor.pattern", isolateDatabasePaths("root.db2"));
      extractorAttributes.replace("extractor.table-name", "test1");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());
      // Wait for each filtered pipe to commit its source-side flush barrier before checking the
      // matching tree rows. The schema count alone can become visible before the rows arrive.
      PipeTestUtils.awaitDataPipeCompletion(senderEnv, "p1");
      PipeTestUtils.awaitDataPipeCompletion(senderEnv, "p2");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db*.**"),
          Map.of(
              isolateDatabasePaths("count(root.db1.d1.at1)"), "1",
              isolateDatabasePaths("count(root.db2.d1.at1)"), "1"));
      assertTimeseriesCountOnReceiver(receiverEnv, 2);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("p1").setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("p1").setIsTableModel(true)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("p2").setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("p2").setIsTableModel(true)).getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db1.d1 (time, at1) values (2, 11)"),
              isolateDatabasePaths("insert into root.db2.d1 (time, at1) values (2, 21)"),
              "flush"),
          null);

      TableModelUtils.insertData(database("test"), "test", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 100, 200, senderEnv);

      extractorAttributes.remove("extractor.pattern"); // no pattern, will match all databases
      extractorAttributes.remove("extractor.table-name");
      extractorAttributes.remove("extractor.database-name");
      status =
          client.createPipe(
              new TCreatePipeReq("p3", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p3").getCode());

      // Renaming databases can reorder wildcard columns; compare each labelled count instead.
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db*.**"),
          Map.of(
              isolateDatabasePaths("count(root.db1.d1.at1)"), "2",
              isolateDatabasePaths("count(root.db2.d1.at1)"), "2"));

      TableModelUtils.assertCountData(database("test"), "test", 200, receiverEnv, handleFailure);
    }
  }

  // Source filters must replicate matching history and realtime events in both the tree and table
  // models.
  @Ignore
  @Test
  public void testHistoryAndRealtime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1) values (1, 10)"),
              isolateDatabasePaths("insert into root.db.d2 (time, at1) values (1, 20)"),
              isolateDatabasePaths("insert into root.db.d3 (time, at1) values (1, 30)"),
              isolateDatabasePaths("insert into root.db.d4 (time, at1) values (1, 40)"),
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
      TableModelUtils.insertData(database("test"), "test1", 0, 100, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
      TableModelUtils.insertData(database("test"), "test2", 0, 100, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test3", database("test"));
      TableModelUtils.insertData(database("test"), "test3", 0, 100, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test4", database("test"));
      TableModelUtils.insertData(database("test"), "test4", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("extractor.database-name", database("test"));
      extractorAttributes.put("extractor.table-name", "test2");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.pattern", isolateDatabasePaths("root.db.d2"));
      extractorAttributes.put("extractor.history.enable", "false");
      extractorAttributes.put("extractor.realtime.enable", "true");
      extractorAttributes.put("user", "root");
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      extractorAttributes.replace("extractor.table-name", "test3");
      extractorAttributes.replace("extractor.pattern", isolateDatabasePaths("root.db.d3"));
      extractorAttributes.replace("extractor.history.enable", "true");
      extractorAttributes.replace("extractor.realtime.enable", "false");
      status =
          client.createPipe(
              new TCreatePipeReq("p3", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p3").getCode());

      extractorAttributes.replace("extractor.table-name", "test4");
      extractorAttributes.replace("extractor.pattern", isolateDatabasePaths("root.db.d4"));
      extractorAttributes.replace("extractor.history.enable", "true");
      extractorAttributes.replace("extractor.realtime.enable", "true");
      status =
          client.createPipe(
              new TCreatePipeReq("p4", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p4").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1) values (2, 11)"),
              isolateDatabasePaths("insert into root.db.d2 (time, at1) values (2, 21)"),
              isolateDatabasePaths("insert into root.db.d3 (time, at1) values (2, 31)"),
              isolateDatabasePaths("insert into root.db.d4 (time, at1) values (2, 41), (3, 51)")),
          null);

      TableModelUtils.insertData(database("test"), "test2", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test"), "test3", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test"), "test4", 100, 200, senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.** where time <= 1"),
          isolateDatabasePaths(
              "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),"),
          Collections.singleton("1,0,1,"),
          handleFailure);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.** where time >= 2"),
          isolateDatabasePaths(
              "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),"),
          Collections.singleton("2,1,0,"),
          handleFailure);

      TableModelUtils.assertData(database("test"), "test2", 100, 200, receiverEnv, handleFailure);

      TableModelUtils.assertData(database("test"), "test3", 0, 100, receiverEnv, handleFailure);

      TableModelUtils.assertData(database("test"), "test4", 0, 200, receiverEnv, handleFailure);
    }
  }

  // History time bounds, with and without path/table filters, must filter old events while
  // allowing subsequent realtime rows.
  @Ignore
  @Test
  public void testHistoryStartTimeAndEndTimeWorkingWithOrWithoutPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              isolateDatabasePaths("insert into root.db.d2 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
      TableModelUtils.insertData(database("test"), "test1", 0, 10, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
      TableModelUtils.insertData(database("test"), "test2", 0, 10, senderEnv);

      // wait for flush to complete
      TestUtils.executeNonQueries(senderEnv, Collections.singletonList("flush"), null);
      Thread.sleep(10000);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("extractor.database-name", database("test"));
      extractorAttributes.put("extractor.table-name", "test1");
      extractorAttributes.put("extractor.pattern", isolateDatabasePaths("root.db.d1"));
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.history.enable", "true");
      // 1970-01-01T08:00:02+08:00
      extractorAttributes.put("extractor.history.start-time", "2");
      extractorAttributes.put("extractor.history.end-time", "3");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),"),
          Collections.singleton("2,"),
          handleFailure);

      extractorAttributes.remove("extractor.table-name");
      extractorAttributes.remove("extractor.pattern");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),count(root.db.d2.at1),"),
          Collections.singleton("2,2,"),
          handleFailure);

      TableModelUtils.assertData(database("test"), "test1", 2, 4, receiverEnv, handleFailure);
      TableModelUtils.assertData(database("test"), "test2", 2, 4, receiverEnv, handleFailure);
    }
  }

  // History and realtime events in [2, 4] must replicate in both models; out-of-range events must
  // remain excluded after all tree and table DataRegions have committed their completion barriers.
  @Test
  public void testExtractorTimeRangeMatch() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // insert history data
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              isolateDatabasePaths("insert into root.db.d2 (time, at1)")
                  + " values (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
      TableModelUtils.insertData(database("test"), "test1", 0, 10, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
      TableModelUtils.insertData(database("test"), "test2", 0, 10, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.start-time", "2");
      extractorAttributes.put("source.end-time", "4");
      extractorAttributes.put("user", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),"),
          Collections.singleton("3,"),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);

      // Insert realtime data that overlapped with time range
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d3 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"),
          null);
      TableModelUtils.createDataBaseAndTable(senderEnv, "test3", database("test"));
      TableModelUtils.insertData(database("test"), "test3", 0, 5, senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),count(root.db.d3.at1),"),
          Collections.singleton("3,3,"),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData(database("test"), "test3", 3, receiverEnv, handleFailure);

      // Insert realtime data that does not overlap with time range
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d4 (time, at1)")
                  + " values (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test4", database("test"));
      TableModelUtils.insertData(database("test"), "test4", 6, 10, senderEnv);

      TestUtils.executeNonQuery(
          senderEnv,
          isolateDatabasePaths("insert into root.db.d3 (time, at1) values (3, 30)"),
          null);
      PipeTestUtils.awaitDataPipeCompletion(senderEnv, "p1");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select at1 from root.db.d3 where time = 3"),
          isolateDatabasePaths("Time,root.db.d3.at1,"),
          Collections.singleton("3,30.0,"),
          handleFailure);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(at1) from root.db.** align by device"),
          "Device,count(at1),",
          new HashSet<>(
              Arrays.asList(
                  isolateDatabasePaths("root.db.d1,3,"), isolateDatabasePaths("root.db.d3,3,"))),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData(database("test"), "test3", 3, receiverEnv, handleFailure);
    }
  }

  // Source time bounds, with and without path/table filters, must restrict both history and
  // realtime events to the configured interval.
  @Test
  public void testSourceStartTimeAndEndTimeWorkingWithOrWithoutPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              isolateDatabasePaths("insert into root.db.d2 (time, at1)")
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
      TableModelUtils.insertData(database("test"), "test1", 0, 5, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
      TableModelUtils.insertData(database("test"), "test2", 0, 5, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("source.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("source.pattern", isolateDatabasePaths("root.db.d1"));
      extractorAttributes.put("source.table-name", "test1");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.start-time", "2");
      // 1970-01-01T08:00:04+08:00
      extractorAttributes.put("source.end-time", "4");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),"),
          Collections.singleton("3,"),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);

      extractorAttributes.remove("source.pattern");
      extractorAttributes.remove("source.table-name");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.at1),count(root.db.d2.at1),"),
          Collections.singleton("3,3,"),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData(database("test"), "test2", 3, receiverEnv, handleFailure);
    }
  }

  // Loose history filtering with and without path/table patterns must preserve the expected
  // matching data.
  @Ignore
  @Test
  public void testHistoryLooseRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
    TableModelUtils.insertData(database("test"), "test1", 0, 2, senderEnv);

    TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
    TableModelUtils.insertData(database("test"), "test2", 0, 2, senderEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              // TsFile 1, extracted without parse
              isolateDatabasePaths("insert into root.db.d1 (time, at1, at2)")
                  + " values (1, 1, 2), (2, 3, 4)",
              // TsFile 2, not extracted because pattern not overlapped
              isolateDatabasePaths("insert into root.db1.d1 (time, at1, at2)")
                  + " values (1, 1, 2), (2, 3, 4)",
              "flush"),
          null);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              // TsFile 3, not extracted because time range not overlapped
              isolateDatabasePaths("insert into root.db.d1 (time, at1, at2)")
                  + " values (3, 1, 2), (4, 3, 4)",
              "flush"),
          null);

      TableModelUtils.insertData(database("test"), "test1", 2, 5, senderEnv);

      // wait for flush to complete
      TestUtils.executeNonQueries(senderEnv, Collections.singletonList("flush"), null);
      Thread.sleep(10000);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("source.capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("source.table-name", "test1");
      extractorAttributes.put("source.path", isolateDatabasePaths("root.db.d1.at1"));
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.history.start-time", "1");
      extractorAttributes.put("source.history.end-time", "2");
      extractorAttributes.put("source.history.loose-range", "time, path");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.** group by level=0"),
          "count(root.*.*.*),",
          Collections.singleton("4,"),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test1", 3, receiverEnv, handleFailure);
    }
  }

  // Loose realtime filtering with and without path/table patterns must preserve the expected
  // matching data.
  @Ignore
  @Test
  public void testRealtimeLooseRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    TableModelUtils.insertData(database("test"), "test2", 0, 20, senderEnv);

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("source.capture.table", "true");
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("source.capture.tree", "true");
    extractorAttributes.put("mode.double-living", "true");
    extractorAttributes.put("source.table-name", "test1");
    extractorAttributes.put("source.path", isolateDatabasePaths("root.db.d1.at1"));
    extractorAttributes.put("source.inclusion", "data.insert");
    extractorAttributes.put("source.realtime.loose-range", "time, path");
    extractorAttributes.put("source.start-time", "2");
    extractorAttributes.put("source.end-time", "10");
    extractorAttributes.put("source.realtime.mode", "batch");
    extractorAttributes.put("user", "root");

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1, at2)")
                  + " values (1, 1, 2), (3, 3, 4)",
              "flush"),
          null);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1 (time, at1)")
                  + " values (5, 1), (16, 3)",
              isolateDatabasePaths("insert into root.db.d1 (time, at1, at2)")
                  + " values (5, 1, 2), (6, 3, 4)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(at1) from root.db.d1 where time >= 2 and time <= 10"),
          new HashMap<String, String>() {
            {
              put(isolateDatabasePaths("count(root.db.d1.at1)"), "3");
            }
          });

      TableModelUtils.insertData(database("test"), "test1", 10, 20, senderEnv);

      TableModelUtils.insertData(database("test"), "test2", 10, 20, senderEnv);

      TableModelUtils.assertCountData(database("test"), "test1", 20, receiverEnv, handleFailure);
    }
  }

  // Table-model CREATE/ALTER PIPE must accept now in prefixed and unprefixed source/history time
  // attributes; execute all six variants in order on the same connection.
  @Test
  public void testTableModeSQLSupportNowFunc() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final String p1 =
        String.format(
            "create pipe p1"
                + " with extractor ("
                + "'capture.table'='true',"
                + "'extractor.history.enable'='true',"
                + "'source.start-time'='now',"
                + "'source.end-time'='now',"
                + "'source.history.start-time'='now',"
                + "'source.history.end-time'='now')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(p1);

      final String p2 =
          String.format(
              "create pipe p2"
                  + " with extractor ("
                  + "'capture.table'='true',"
                  + "'extractor.history.enable'='true',"
                  + "'start-time'='now',"
                  + "'end-time'='now',"
                  + "'history.start-time'='now',"
                  + "'history.end-time'='now')"
                  + " with connector ("
                  + "'connector'='iotdb-thrift-connector',"
                  + "'connector.ip'='%s',"
                  + "'connector.port'='%s',"
                  + "'connector.batch.enable'='false')",
              receiverIp, receiverPort);
      statement.execute(p2);

      final String p3 =
          String.format(
              "create pipe p3"
                  + " with extractor ("
                  + "'capture.table'='true',"
                  + "'extractor.history.enable'='true',"
                  + "'extractor.start-time'='now',"
                  + "'extractor.end-time'='now',"
                  + "'extractor.history.start-time'='now',"
                  + "'extractor.history.end-time'='now')"
                  + " with connector ("
                  + "'connector'='iotdb-thrift-connector',"
                  + "'connector.ip'='%s',"
                  + "'connector.port'='%s',"
                  + "'connector.batch.enable'='false')",
              receiverIp, receiverPort);
      statement.execute(p3);

      String alterP3 =
          "alter pipe p3"
              + " modify extractor ("
              + "'history.enable'='true',"
              + "'start-time'='now',"
              + "'end-time'='now',"
              + "'history.start-time'='now',"
              + "'history.end-time'='now')";
      statement.execute(alterP3);

      alterP3 =
          "alter pipe p3"
              + " modify extractor ("
              + "'extractor.history.enable'='true',"
              + "'extractor.start-time'='now',"
              + "'extractor.end-time'='now',"
              + "'extractor.history.start-time'='now',"
              + "'extractor.history.end-time'='now')";
      statement.execute(alterP3);

      alterP3 =
          "alter pipe p3"
              + " modify source ("
              + "'extractor.history.enable'='true',"
              + "'source.start-time'='now',"
              + "'source.end-time'='now',"
              + "'source.history.start-time'='now',"
              + "'source.history.end-time'='now')";
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
  }

  private void assertTimeseriesCountOnReceiver(BaseEnv receiverEnv, int count) {
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        isolateDatabasePaths("count timeseries root.db*.**"),
        "count(timeseries),",
        Collections.singleton(count + ","));
  }

  private String isolateDatabasePaths(final String value) {
    // Prefix the database name so db* remains a legal wildcard at the end of the path node.
    return value.replace("root.db", "root." + databaseSuffix() + "_db");
  }

  private String database(final String name) {
    // Database deletion is asynchronous; never reuse another method's database incarnation.
    return name + "_" + databaseSuffix();
  }

  private String databaseSuffix() {
    // Stay below the receiver's 64-character database path limit, including root and the prefix.
    // Retain a hash when shortening names so methods with the same prefix use different databases.
    final String method = testName.getMethodName().toLowerCase(Locale.ROOT);
    return method.length() <= 40
        ? method
        : method.substring(0, 31) + "_" + Integer.toUnsignedString(method.hashCode(), 16);
  }
}
