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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeExtractorIT extends AbstractPipeTableModelTestIT {

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Disable sender compaction for tsfile determination in loose range test
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testMatchingMultipleDatabases() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    boolean insertResult = true;
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
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("extractor.table-name", "test");
      extractorAttributes.put("extractor.pattern", "root.db1");
      extractorAttributes.put("extractor.inclusion", "data.insert");

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db1.d1 (time, at1) values (1, 10)",
              "insert into root.db2.d1 (time, at1) values (1, 20)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      extractorAttributes.replace("extractor.pattern", "root.db2");
      extractorAttributes.replace("extractor.table-name", "test1");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());
      assertTimeseriesCountOnReceiver(receiverEnv, 2);

      Thread.sleep(10000);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p2").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db1.d1 (time, at1) values (2, 11)",
              "insert into root.db2.d1 (time, at1) values (2, 21)",
              "flush"))) {
        return;
      }

      insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
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

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db1.d1.at1),count(root.db2.d1.at1),",
          Collections.singleton("2,2,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test", 200, receiverEnv, handleFailure);
    }
  }

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

    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1) values (1, 10)",
              "insert into root.db.d2 (time, at1) values (1, 20)",
              "insert into root.db.d3 (time, at1) values (1, 30)",
              "insert into root.db.d4 (time, at1) values (1, 40)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
      insertResult = TableModelUtils.insertData("test", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test");
      insertResult = TableModelUtils.insertData("test", "test2", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test3", "test3");
      insertResult = TableModelUtils.insertData("test", "test3", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test4", "test");
      insertResult = TableModelUtils.insertData("test", "test4", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("extractor.database", "test");
      extractorAttributes.put("extractor.table", "test2");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.pattern", "root.db.d2");
      extractorAttributes.put("extractor.history.enable", "false");
      extractorAttributes.put("extractor.realtime.enable", "true");
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      extractorAttributes.replace("extractor.table-name", "test3");
      extractorAttributes.replace("extractor.pattern", "root.db.d3");
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
      extractorAttributes.replace("extractor.pattern", "root.db.d4");
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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1) values (2, 11)",
              "insert into root.db.d2 (time, at1) values (2, 21)",
              "insert into root.db.d3 (time, at1) values (2, 31)",
              "insert into root.db.d4 (time, at1) values (2, 41), (3, 51)"))) {
        return;
      }

      insertResult = TableModelUtils.insertData("test", "test2", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test", "test3", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test", "test4", 0, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.** where time <= 1",
          "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),",
          Collections.singleton("1,0,1,"),
          handleFailure);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.** where time >= 2",
          "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),",
          Collections.singleton("2,1,0,"),
          handleFailure);

      TableModelUtils.assertData("test", "test2", 100, 200, receiverEnv, handleFailure);

      TableModelUtils.assertData("test", "test3", 100, 200, receiverEnv, handleFailure);

      TableModelUtils.assertData("test", "test4", 100, 200, receiverEnv, handleFailure);
    }
  }

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

    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
      insertResult = TableModelUtils.insertData("test", "test1", 0, 10, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test");
      insertResult = TableModelUtils.insertData("test", "test2", 0, 10, senderEnv);
      if (!insertResult) {
        return;
      }

      // wait for flush to complete
      if (!TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList("flush"))) {
        return;
      }
      Thread.sleep(10000);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("extractor.table-name", "test1");
      extractorAttributes.put("extractor.pattern", "root.db.d1");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.history.enable", "true");
      // 1970-01-01T08:00:02+08:00
      extractorAttributes.put("extractor.history.start-time", "2");
      extractorAttributes.put("extractor.history.end-time", "3");

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
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
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
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d2.at1),",
          Collections.singleton("2,2,"),
          handleFailure);

      TableModelUtils.assertData("test", "test1", 2, 4, receiverEnv, handleFailure);
      TableModelUtils.assertData("test", "test2", 2, 4, receiverEnv, handleFailure);
    }
  }

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

    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // insert history data
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
      insertResult = TableModelUtils.insertData("test", "test1", 0, 10, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test");
      insertResult = TableModelUtils.insertData("test", "test2", 0, 10, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.start-time", "2");
      extractorAttributes.put("source.end-time", "4");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
          Collections.singleton("3,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);

      // Insert realtime data that overlapped with time range
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d3 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"))) {
        return;
      }
      TableModelUtils.createDataBaseAndTable(senderEnv, "test3", "test");
      insertResult = TableModelUtils.insertData("test", "test3", 0, 5, senderEnv);
      if (!insertResult) {
        return;
      }
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d3.at1),",
          Collections.singleton("3,3,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData("test", "test3", 3, receiverEnv, handleFailure);

      // Insert realtime data that does not overlap with time range
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d4 (time, at1)"
                  + " values (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test4", "test");
      insertResult = TableModelUtils.insertData("test", "test4", 6, 10, senderEnv);
      if (!insertResult) {
        return;
      }
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d3.at1),",
          Collections.singleton("3,3,"),
          600,
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData("test", "test3", 3, receiverEnv, handleFailure);
    }
  }

  @Test
  public void testSourceStartTimeAndEndTimeWorkingWithOrWithoutPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
              "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
      insertResult = TableModelUtils.insertData("test", "test1", 0, 5, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test");
      insertResult = TableModelUtils.insertData("test", "test2", 0, 5, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.capture.table", "true");
      extractorAttributes.put("source.capture.tree", "true");
      extractorAttributes.put("source.pattern", "root.db.d1");
      extractorAttributes.put("source.table-name", "test1");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.start-time", "2");
      // 1970-01-01T08:00:04+08:00
      extractorAttributes.put("source.end-time", "4");

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
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
          Collections.singleton("3,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);

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
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d2.at1),",
          Collections.singleton("3,3,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);
      TableModelUtils.assertCountData("test", "test2", 3, receiverEnv, handleFailure);
    }
  }

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
    boolean insertResult = true;

    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test");
    insertResult = TableModelUtils.insertData("test", "test1", 0, 2, senderEnv);
    if (!insertResult) {
      return;
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test");
    insertResult = TableModelUtils.insertData("test", "test2", 0, 2, senderEnv);
    if (!insertResult) {
      return;
    }
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              // TsFile 1, extracted without parse
              "insert into root.db.d1 (time, at1, at2)" + " values (1, 1, 2), (2, 3, 4)",
              // TsFile 2, not extracted because pattern not overlapped
              "insert into root.db1.d1 (time, at1, at2)" + " values (1, 1, 2), (2, 3, 4)",
              "flush"))) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              // TsFile 3, not extracted because time range not overlapped
              "insert into root.db.d1 (time, at1, at2)" + " values (3, 1, 2), (4, 3, 4)",
              "flush"))) {
        return;
      }

      insertResult = TableModelUtils.insertData("test", "test1", 2, 5, senderEnv);
      if (!insertResult) {
        return;
      }

      // wait for flush to complete
      if (!TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList("flush"))) {
        return;
      }
      Thread.sleep(10000);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.capture.table", "true");
      extractorAttributes.put("source.capture.tree", "true");
      extractorAttributes.put("source.table-name", "test1");
      extractorAttributes.put("source.path", "root.db.d1.at1");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.history.start-time", "1");
      extractorAttributes.put("source.history.end-time", "2");
      extractorAttributes.put("source.history.loose-range", "time, path");

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
          "select count(*) from root.** group by level=0",
          "count(root.*.*.*),",
          Collections.singleton("4,"),
          handleFailure);

      TableModelUtils.assertCountData("test", "test1", 3, receiverEnv, handleFailure);
    }
  }

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

    boolean insertResult = true;
    insertResult = TableModelUtils.insertData("test", "test2", 0, 20, senderEnv);
    if (!insertResult) {
      return;
    }
    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("source.capture.table", "true");
    extractorAttributes.put("source.capture.tree", "true");
    extractorAttributes.put("source.table-name", "test1");
    extractorAttributes.put("source.path", "root.db.d1.at1");
    extractorAttributes.put("source.inclusion", "data.insert");
    extractorAttributes.put("source.realtime.loose-range", "time, path");
    extractorAttributes.put("source.start-time", "2");
    extractorAttributes.put("source.end-time", "10");
    extractorAttributes.put("source.realtime.mode", "batch");

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1, at2)" + " values (1, 1, 2), (3, 3, 4)",
              "flush"))) {
        return;
      }

      if (!insertResult) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)" + " values (5, 1), (16, 3)",
              "insert into root.db.d1 (time, at1, at2)" + " values (5, 1, 2), (6, 3, 4)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(at1) from root.db.d1 where time >= 2 and time <= 10",
          new HashMap<String, String>() {
            {
              put("count(root.db.d1.at1)", "3");
            }
          });

      insertResult = TableModelUtils.insertData("test", "test1", 10, 20, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test", "test2", 10, 20, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertCountData("test", "test1", 20, receiverEnv, handleFailure);
    }
  }

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
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

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
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(p2);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

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
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(p3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    String alterP3 =
        "alter pipe p3"
            + " modify extractor ("
            + "'history.enable'='true',"
            + "'start-time'='now',"
            + "'end-time'='now',"
            + "'history.start-time'='now',"
            + "'history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    alterP3 =
        "alter pipe p3"
            + " modify extractor ("
            + "'extractor.history.enable'='true',"
            + "'extractor.start-time'='now',"
            + "'extractor.end-time'='now',"
            + "'extractor.history.start-time'='now',"
            + "'extractor.history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    alterP3 =
        "alter pipe p3"
            + " modify source ("
            + "'extractor.history.enable'='true',"
            + "'source.start-time'='now',"
            + "'source.end-time'='now',"
            + "'source.history.start-time'='now',"
            + "'source.history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
  }

  private void assertTimeseriesCountOnReceiver(BaseEnv receiverEnv, int count) {
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton(count + ","));
  }
}
