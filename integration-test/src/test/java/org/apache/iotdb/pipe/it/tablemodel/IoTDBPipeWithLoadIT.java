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
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeWithLoadIT extends AbstractPipeTableModelTestIT {

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
        // Disable sender compaction to test mods
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

  @Ignore // not support
  @Test
  public void testReceiverNotLoadDeletedTimeseries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };
    boolean insertResult = true;

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    // Enable mods transfer
    extractorAttributes.put("mods", "true");
    extractorAttributes.put("capture.table", "true");

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Generate TsFile
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.deleteData("test", "test", 50, 100, senderEnv);

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.assertCountData("test", "test", 50, receiverEnv, handleFailure);
    }
  }

  // Test that receiver will not load data when table exists but ID columns mismatch
  @Test
  public void testReceiverNotLoadWhenIdColumnMismatch() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("extractor.realtime.mode", "file");

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id1 STRING ID, id2 STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT)");
        statement.execute("INSERT INTO t1(time,id1,id2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,id1,id2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id3 STRING ID, id4 STRING ID, s3 TEXT MEASUREMENT, s4 INT32 MEASUREMENT)");
        statement.execute("INSERT INTO t1(time,id3,id4,s3,s4) values(1, 'd3', 'd4', 'red2', 10)");
        statement.execute("INSERT INTO t1(time,id3,id4,s3,s4) values(2, 'd3', 'd4', 'blue2', 20)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      try {
        // wait some time
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      Set<String> expectedResSet = new java.util.HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.002Z,d3,d4,blue2,20,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d3,d4,red2,10,");
      // make sure data are not transferred
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,id3,id4,s3,s4,",
          expectedResSet,
          "db",
          handleFailure);
    }
  }

  // Test that receiver can load data when table exists and existing ID columns are the prefix of
  // incoming ID columns
  @Test
  public void testReceiverAutoExtendIdColumn() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("extractor.realtime.mode", "file");

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id1 STRING ID, id2 STRING ID, id3 STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT)");
        statement.execute(
            "INSERT INTO t1(time,id1,id2,id3,s1,s2) values(1, 'd1', 'd2', 'd3', 'red', 1)");
        statement.execute(
            "INSERT INTO t1(time,id1,id2,id3,s1,s2) values(2, 'd1', 'd2', 'd3', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id1 STRING ID, id2 STRING ID, s3 TEXT MEASUREMENT, s4 INT32 MEASUREMENT)");
        statement.execute("INSERT INTO t1(time,id1,id2,s3,s4) values(1, 'd1', 'd2', 'red2', 10)");
        statement.execute("INSERT INTO t1(time,id1,id2,s3,s4) values(2, 'd1', 'd2', 'blue2', 20)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      Set<String> expectedResSet = new java.util.HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,null,null,d3,red,1,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,null,null,d3,blue,2,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,red2,10,null,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,blue2,20,null,null,null,");
      // make sure data are transferred and column "id3" is auto extended
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,id1,id2,s3,s4,id3,s1,s2,",
          expectedResSet,
          "db",
          handleFailure);
    }
  }

  // Test that receiver can load data when table exists and incoming ID columns are the prefix of
  // existing ID columns
  @Test
  public void testLoadWhenIncomingIdColumnsArePrefixOfExisting() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("extractor.realtime.mode", "file");

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id1 STRING ID, id2 STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT)");
        statement.execute("INSERT INTO t1(time,id1,id2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,id1,id2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists db");
        statement.execute("use db");
        statement.execute(
            "create table if not exists t1(id1 STRING ID, id2 STRING ID, id3 STRING ID,s3 TEXT MEASUREMENT, s4 INT32 MEASUREMENT)");
        statement.execute(
            "INSERT INTO t1(time,id1,id2,id3,s3,s4) values(1, 'd1', 'd2', 'd3', 'red2', 10)");
        statement.execute(
            "INSERT INTO t1(time,id1,id2,id3,s3,s4) values(2, 'd1', 'd2', 'd3', 'blue2', 20)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      Set<String> expectedResSet = new java.util.HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,d3,red2,10,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,d3,blue2,20,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,null,null,null,red,1,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,null,null,null,blue,2,");
      // make sure data are transferred and column "id3" is null in transferred data
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,id1,id2,id3,s3,s4,s1,s2,",
          expectedResSet,
          10,
          "db",
          handleFailure);
    }
  }
}
