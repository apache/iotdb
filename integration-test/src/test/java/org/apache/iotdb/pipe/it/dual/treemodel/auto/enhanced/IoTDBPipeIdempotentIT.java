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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeIdempotentIT extends AbstractPipeTreeModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  private boolean clustersReady;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    clustersReady = true;
  }

  @Override
  protected void setupConfig() {
    // All the schema operations must be under the same database to
    // be in the same region, therefore a non-idempotent operation can block the next one
    // and fail the IT
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        // Limit the schemaRegion number to 1 to guarantee the after sql executed on the same region
        // of the tested idempotent sql.
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @Override
  protected void cleanEnvironment() {
    super.cleanEnvironment();
    if (!clustersReady) {
      return;
    }
    // The base drops pipes before databases, removing template bindings and activations as well.
    // Templates and authorization outlive databases, so reset them before the next scenario.
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
        for (final String type : new String[] {"user", "role"}) {
          final List<String> names = new ArrayList<>();
          try (final ResultSet result = statement.executeQuery("list " + type)) {
            while (result.next()) {
              final String name = result.getString("user".equals(type) ? "User" : "Role");
              if (!"user".equals(type) || !SessionConfig.DEFAULT_USER.equals(name)) {
                names.add(name);
              }
            }
          }
          for (final String name : names) {
            statement.execute("drop " + type + " `" + name.replace("`", "``") + "`");
          }
        }
      } catch (final Exception e) {
        throw new AssertionError("Failed to reset idempotent test metadata", e);
      }
    }
  }

  private String databasePattern() {
    // Count only this method's suffixed databases, including its setup and follow-up events.
    return "root.*_" + Integer.toUnsignedString(testName.getMethodName().hashCode(), 16);
  }

  private String isolateDatabasePaths(final String sql) {
    // Keep all operations within each original database on one schema region. A short method hash
    // isolates different methods without exceeding the receiver's database path length limit.
    return sql.replaceAll(
        "root\\.(ln|sg[0-9]*|t1)(?=[.,; ]|$)",
        "$0_" + Integer.toUnsignedString(testName.getMethodName().hashCode(), 16));
  }

  // Creating an existing BOOLEAN series must allow the following series to sync (two total).
  @Test
  public void testCreateTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // Recreating an aligned pair must allow a later scalar series to sync (three total).
  @Test
  public void testCreateAlignedTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)",
        "create timeSeries root.ln.wf01.wt01.status1(status) with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("3,"));
  }

  // Recreating a series with the same alias must allow a second aliased series to sync.
  @Test
  public void testCreateTimeSeriesWithAliasIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create timeSeries root.ln.wf01.wt01.status0(status0) with datatype=BOOLEAN,encoding=PLAIN",
        "create timeSeries root.ln.wf01.wt01.status1(status1) with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // An insert that auto-creates an existing series must not block later schema replication.
  @Test
  public void testInternalCreateTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "insert into root.ln.wf01.wt01(time, status0) values(now(), false)",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // Adding a tag already present on the receiver must not block the following series creation.
  @Test
  public void testAlterTimeSeriesAddTagIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 ADD TAGS tag3=v3;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // Adding an existing attribute must allow the following series to sync (two total).
  @Test
  public void testAlterTimeSeriesAddAttrIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 ADD ATTRIBUTES attr1=newV1;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // Renaming an attribute already renamed on the receiver must allow later schema to sync.
  @Test
  public void testAlterTimeSeriesRenameIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
            "ALTER timeSeries root.ln.wf01.wt01.status0 ADD ATTRIBUTES attr1=newV1;"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 RENAME attr1 TO newAttr1;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries root.ln.**",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  // Deleting a series already absent on the receiver must allow the next database to sync.
  @Test
  public void testDeleteTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0(status0) with datatype=BOOLEAN,encoding=PLAIN"),
        "delete timeSeries root.ln.wf01.wt01.status0",
        "create database root.sg",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Creating an existing template must allow the following database creation to sync.
  @Test
  public void testCreateTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Extending a template with existing measurements must allow later database events to sync.
  @Test
  public void testExtendTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)"),
        "alter schema template t1 add (rest FLOAT encoding=RLE, FLOAT2 TEXT encoding=PLAIN compression=SNAPPY)",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Dropping an already absent template must allow the following database to sync.
  @Test
  public void testDropTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)"),
        "drop schema template t1",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Binding a template already bound on the receiver must allow a second database to sync.
  @Test
  public void testSetTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1"),
        "set schema template t1 to root.sg1",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Unbinding an already unbound template must allow a second database to sync.
  @Test
  public void testUnsetTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1"),
        "unset schema template t1 from root.sg1",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Activating an already active device must allow a second device activation (six series total).
  @Test
  public void testActivateTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1"),
        "create timeSeries using device template on root.sg1.d1",
        "create timeSeries using device template on root.sg1.d2",
        "count timeSeries root.sg1.**",
        "count(timeseries),",
        Collections.singleton("6,"));
  }

  // Deactivating template devices already inactive on the receiver must allow later database sync.
  @Test
  public void testDeactivateTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1",
            "create timeSeries using device template on root.sg1.d1",
            "create timeSeries using device template on root.sg1.d2"),
        "delete timeSeries of schema template t1 from root.sg1.*",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Creating an existing database must allow the second database to sync (two total).
  @Test
  public void testCreateDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create database root.sg1",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Reapplying schema/data region limits must allow a second database to sync.
  @Test
  public void testAlterDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create database root.sg1"),
        "ALTER DATABASE root.sg1 WITH MAX_SCHEMA_REGION_GROUP_NUM=2, MAX_DATA_REGION_GROUP_NUM=3;",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("2,"));
  }

  // Dropping an already absent database must allow the next creation to sync (one remaining).
  @Test
  public void testDropDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create database root.sg1"),
        "drop database root.sg1",
        "create database root.sg2",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Creating an existing user must not block the subsequent database event.
  @Test
  public void testCreateUserIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create user `ln_write_user` 'Write_pwd!123456'",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Creating an existing role must not block the subsequent database event.
  @Test
  public void testCreateRoleIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create role `test`",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Granting an already granted role membership must allow later database events to sync.
  @Test
  public void testGrantRoleToUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList("create user `ln_write_user` 'Write_pwd!123456'", "create role `test`"),
        "grant role test to ln_write_user",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Revoking already absent user path privileges must allow the next database event to sync.
  @Test
  public void testRevokeUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create user `ln_write_user` 'Write_pwd!123456'",
            "GRANT READ_DATA, WRITE_DATA ON root.t1.** TO USER ln_write_user;"),
        "REVOKE READ_DATA, WRITE_DATA ON root.t1.** FROM USER ln_write_user;",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Revoking already absent role privileges must allow the next database event to sync.
  @Test
  public void testRevokeRoleIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList("create role `test`", "GRANT READ ON root.** TO ROLE test;"),
        "REVOKE READ ON root.** FROM ROLE test;",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Revoking an already absent role membership must allow later database events to sync.
  @Test
  public void testRevokeRoleFromUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create user `ln_write_user` 'Write_pwd!123456'",
            "create role `test`",
            "grant role test to ln_write_user"),
        "revoke role test from ln_write_user",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Dropping an already absent user must not block the subsequent database event.
  @Test
  public void testDropUserIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create user `ln_write_user` 'Write_pwd!123456'"),
        "drop user `ln_write_user`",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  // Dropping an already absent role must not block the subsequent database event.
  @Test
  public void testDropRoleIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create role `test`"),
        "drop role test",
        "create database root.sg1",
        "count databases " + databasePattern(),
        "count,",
        Collections.singleton("1,"));
  }

  private void testIdempotent(
      final List<String> beforeSqlList,
      final String testSql,
      final String afterSql,
      final String afterSqlQuery,
      final String expectedHeader,
      final Set<String> expectedResSet)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

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
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    try (final Connection senderConnection = senderEnv.getConnection();
        final Connection receiverConnection = receiverEnv.getConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          beforeSqlList.stream().map(this::isolateDatabasePaths).collect(Collectors.toList()),
          SessionConfig.DEFAULT_USER,
          SessionConfig.DEFAULT_PASSWORD,
          senderConnection);

      TestUtils.tryExecuteNonQuery(receiverEnv, isolateDatabasePaths(testSql), receiverConnection);

      // Create an idempotent conflict; afterSql must run on the same region as testSql.
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(isolateDatabasePaths(testSql), isolateDatabasePaths(afterSql)),
          SessionConfig.DEFAULT_USER,
          SessionConfig.DEFAULT_PASSWORD,
          senderConnection);
    }

    // Assume that the afterSql is executed on receiverEnv
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        isolateDatabasePaths(afterSqlQuery),
        isolateDatabasePaths(expectedHeader),
        expectedResSet);
  }
}
