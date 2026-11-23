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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeWithLoadIT extends AbstractPipeTableModelSharedClusterIT {

  @Override
  protected void setupConfig() {
    // Retain this suite's original load/compaction settings for the shared clusters.
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Disable sender compaction to test mods
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setEnforceStrongPassword(false);
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
        .setEnforceStrongPassword(false);
  }

  // Verify historical loading applies deletions; disabled until table mods transfer is supported.
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

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    // Enable mods transfer
    extractorAttributes.put("mods", "true");
    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Generate TsFile
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

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

  // Historical files whose TAG columns disagree with the receiver must fail schema validation
  // without changing the receiver's existing rows. Observe that failure before checking the rows.
  @Test
  public void testReceiverNotLoadWhenIdColumnMismatch() throws Exception {
    final String database = "load_tag_mismatch";
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
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("extractor.realtime.mode", "file");
    // Restrict capture to this method, including any historical files left in the shared cluster.
    extractorAttributes.put("extractor.database-name", database);
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag3 STRING TAG, tag4 STRING TAG, s3 TEXT FIELD, s4 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag3,tag4,s3,s4) values(1, 'd3', 'd4', 'red2', 10)");
        statement.execute(
            "INSERT INTO t1(time,tag3,tag4,s3,s4) values(2, 'd3', 'd4', 'blue2', 20)");
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

      awaitReceiverLoadFailure(
          database,
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_INCOMING_TABLE_HAS_NO_LESS_D3D33555,
              "tag3",
              0,
              -1));

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.002Z,d3,d4,blue2,20,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d3,d4,red2,10,");
      // make sure data are not transferred
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,tag3,tag4,s3,s4,",
          expectedResSet,
          database,
          handleFailure);
    }
  }

  // Test that receiver can load data when table exists and existing TAG columns are the prefix of
  // incoming TAG columns
  @Test
  public void testReceiverAutoExtendIdColumn() throws Exception {
    final String database = "load_extend_tag";
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
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("extractor.realtime.mode", "file");
    // Restrict capture to this method, including any historical files left in the shared cluster.
    extractorAttributes.put("extractor.database-name", database);
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, tag3 STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD)");
        statement.execute(
            "INSERT INTO t1(time,tag1,tag2,tag3,s1,s2) values(1, 'd1', 'd2', 'd3', 'red', 1)");
        statement.execute(
            "INSERT INTO t1(time,tag1,tag2,tag3,s1,s2) values(2, 'd1', 'd2', 'd3', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, s3 TEXT FIELD, s4 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s3,s4) values(1, 'd1', 'd2', 'red2', 10)");
        statement.execute(
            "INSERT INTO t1(time,tag1,tag2,s3,s4) values(2, 'd1', 'd2', 'blue2', 20)");
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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,null,null,d3,red,1,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,null,null,d3,blue,2,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,red2,10,null,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,blue2,20,null,null,null,");
      // make sure data are transferred and column "tag3" is auto extended
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,tag1,tag2,s3,s4,tag3,s1,s2,",
          expectedResSet,
          database,
          handleFailure);
    }
  }

  // Test that receiver can load data when table exists and incoming TAG columns are the prefix of
  // existing TAG columns
  @Test
  public void testLoadWhenIncomingIdColumnsArePrefixOfExisting() throws Exception {
    final String database = "load_prefix_tag";
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
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("extractor.realtime.mode", "file");
    // Restrict capture to this method, including any historical files left in the shared cluster.
    extractorAttributes.put("extractor.database-name", database);
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
      } catch (Exception e) {
        fail(e.getMessage());
      }

      try (Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, tag3 STRING TAG,s3 TEXT FIELD, s4 INT32 FIELD)");
        statement.execute(
            "INSERT INTO t1(time,tag1,tag2,tag3,s3,s4) values(1, 'd1', 'd2', 'd3', 'red2', 10)");
        statement.execute(
            "INSERT INTO t1(time,tag1,tag2,tag3,s3,s4) values(2, 'd1', 'd2', 'd3', 'blue2', 20)");
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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,d3,red2,10,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,d3,blue2,20,null,null,");
      expectedResSet.add("1970-01-01T00:00:00.001Z,d1,d2,null,null,null,red,1,");
      expectedResSet.add("1970-01-01T00:00:00.002Z,d1,d2,null,null,null,blue,2,");
      // make sure data are transferred and column "tag3" is null in transferred data
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from t1",
          "time,tag1,tag2,tag3,s3,s4,s1,s2,",
          expectedResSet,
          database,
          handleFailure);
    }
  }

  // Verify a table deleted before historical capture is not recreated on the receiver.
  @Test
  public void testLoadAutoCreateWithTableDeletion() throws Exception {
    final String database = "load_deleted_table";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("extractor.realtime.mode", "file");
    // Restrict capture to this method, including any historical files left in the shared cluster.
    extractorAttributes.put("extractor.database-name", database);
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
        // Start historical transfer only after the table and its data have been deleted.
        TestUtils.dropTable(connection, database, "t1");
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

      // Ensure the deleted table won't be created
      // Now the database will also be created at receiver
      TestUtils.assertAlwaysFail(receiverEnv, "describe " + database + ".t1");
    }
  }

  // A receiver user with CREATE but no INSERT privilege must reject the historical file and leave
  // its table absent. Wait for that specific denial before checking the catalog.
  @Test
  public void testLoadAutoCreateWithoutInsertPermission() throws Exception {
    final String database = "load_no_insert";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("__system.sql-dialect", "table");
    extractorAttributes.put("extractor.realtime.mode", "file");
    // Restrict capture to this method, including any historical files left in the shared cluster.
    extractorAttributes.put("extractor.database-name", database);
    extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));
    connectorAttributes.put("connector.user", "user01");
    connectorAttributes.put("connector.password", "IoTDB!Test2024");

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (final Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("create user user01 'IoTDB!Test2024'");
        statement.execute("grant create on any to user user01");
      } catch (final Exception e) {
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("create database if not exists " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table if not exists t1(tag1 STRING TAG, tag2 STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(1, 'd1', 'd2', 'red', 1)");
        statement.execute("INSERT INTO t1(time,tag1,tag2,s1,s2) values(2, 'd1', 'd2', 'blue', 2)");
        statement.execute("flush");
      } catch (final Exception e) {
        fail(e.getMessage());
      }

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      awaitReceiverLoadFailure(
          database,
          AccessDeniedException.PREFIX
              + AuthorityChecker.getTSStatus(false, PrivilegeType.INSERT, database, "t1")
                  .getMessage());
      try (final Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement();
          final ResultSet tables =
              statement.executeQuery(
                  "select table_name from information_schema.tables where database = '"
                      + database
                      + "' and table_name = 't1'")) {
        // The load may be rejected before even creating the database. The system catalog checks
        // table absence in either case without treating an unrelated query failure as success.
        Assert.assertFalse("The rejected load must not create the receiver table", tables.next());
      }
    }
  }

  private void awaitReceiverLoadFailure(final String database, final String expectedMessage) {
    // Rejected loads keep retrying and suppress runtime error reporting, so neither a successful
    // completion barrier nor an empty SHOW PIPES error proves that the receiver tried the file.
    // Each scenario has a distinct database in these fresh class-scoped clusters, so a preceding
    // scenario's log cannot satisfy the wait. Match the database and failure on the same line.
    await()
        .alias("Receiver load failure: " + expectedMessage)
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(120, TimeUnit.SECONDS)
        .until(
            () -> {
              for (final DataNodeWrapper node : receiverEnv.getDataNodeWrapperList()) {
                final Path log = Path.of(node.getLogDirPath(), node.getId() + ".log");
                if (Files.exists(log)) {
                  try (final Stream<String> lines = Files.lines(log)) {
                    if (lines.anyMatch(
                        line -> line.contains(database) && line.contains(expectedMessage))) {
                      return true;
                    }
                  }
                }
              }
              return false;
            });
  }
}
