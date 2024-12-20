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

package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeIdempotentIT extends AbstractPipeDualAutoIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    // All the schema operations must be under the same database to
    // be in the same region, therefore a non-idempotent operation can block the next one
    // and fail the IT
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        // Limit the schemaRegion number to 1 to guarantee the after sql executed on the same region
        // of the tested idempotent sql.
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
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
  public void testCreateTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testCreateAlignedTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)",
        "create timeSeries root.ln.wf01.wt01.status1(status) with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("3,"));
  }

  @Test
  public void testCreateTimeSeriesWithAliasIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create timeSeries root.ln.wf01.wt01.status0(status0) with datatype=BOOLEAN,encoding=PLAIN",
        "create timeSeries root.ln.wf01.wt01.status1(status1) with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testInternalCreateTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "insert into root.ln.wf01.wt01(time, status0) values(now(), false); flush;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testAlterTimeSeriesAddTagIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 ADD TAGS tag3=v3;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testAlterTimeSeriesAddAttrIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 ADD ATTRIBUTES attr1=newV1;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testAlterTimeSeriesRenameIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create timeSeries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
            "ALTER timeSeries root.ln.wf01.wt01.status0 ADD ATTRIBUTES attr1=newV1;"),
        "ALTER timeSeries root.ln.wf01.wt01.status0 RENAME attr1 TO newAttr1;",
        "create timeSeries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testDeleteTimeSeriesIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create timeSeries root.ln.wf01.wt01.status0(status0) with datatype=BOOLEAN,encoding=PLAIN"),
        "delete timeSeries root.ln.wf01.wt01.status0",
        "create timeSeries root.ln.wf01.wt01.status2(status2) with datatype=BOOLEAN,encoding=PLAIN",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("1,"));
  }

  @Test
  public void testCreateTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testExtendTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)"),
        "alter schema template t1 add (rest FLOAT encoding=RLE, FLOAT2 TEXT encoding=PLAIN compression=SNAPPY)",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testDropTemplateIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)"),
        "drop schema template t1",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testSetTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1"),
        "set schema template t1 to root.sg1",
        "create database root.sg2",
        "count databases",
        "count,",
        Collections.singleton("2,"));
  }

  @Test
  public void testUnsetTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1"),
        "unset schema template t1 from root.sg1",
        "create database root.sg2",
        "count databases",
        "count,",
        Collections.singleton("2,"));
  }

  @Test
  public void testActivateTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1"),
        "create timeSeries using device template on root.sg1.d1",
        "create timeSeries using device template on root.sg1.d2",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("6,"));
  }

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
        "create timeSeries using device template on root.sg1.d3",
        "count timeSeries",
        "count(timeseries),",
        Collections.singleton("3,"));
  }

  @Test
  public void testCreateDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create database root.sg1",
        "create database root.sg2",
        "count databases",
        "count,",
        Collections.singleton("2,"));
  }

  @Test
  public void testAlterDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create database root.sg1"),
        "ALTER DATABASE root.sg1 WITH SCHEMA_REGION_GROUP_NUM=2, DATA_REGION_GROUP_NUM=3;",
        "create database root.sg2",
        "count databases",
        "count,",
        Collections.singleton("2,"));
  }

  @Test
  public void testDropDatabaseIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create database root.sg1"),
        "drop database root.sg1",
        "create database root.sg2",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testCreateUserIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create user `ln_write_user` 'write_pwd'",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testCreateRoleIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create role `test`",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testGrantRoleToUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList("create user `ln_write_user` 'write_pwd'", "create role `test`"),
        "grant role test to ln_write_user",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testRevokeUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create user `ln_write_user` 'write_pwd'",
            "GRANT READ_DATA, WRITE_DATA ON root.t1.** TO USER ln_write_user;"),
        "REVOKE READ_DATA, WRITE_DATA ON root.t1.** FROM USER ln_write_user;",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testRevokeRoleIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList("create role `test`", "GRANT READ ON root.** TO ROLE test;"),
        "REVOKE READ ON root.** FROM ROLE test;",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testRevokeRoleFromUserIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create user `ln_write_user` 'write_pwd'",
            "create role `test`",
            "grant role test to ln_write_user"),
        "revoke role test from ln_write_user",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testDropUserIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create user `ln_write_user` 'write_pwd'"),
        "drop user `ln_write_user`",
        "create database root.sg1",
        "count databases",
        "count,",
        Collections.singleton("1,"));
  }

  @Test
  public void testDropRoleIdempotent() throws Exception {
    testIdempotent(
        Collections.singletonList("create role `test`"),
        "drop role test",
        "create database root.sg1",
        "count databases",
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
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, beforeSqlList)) {
      return;
    }

    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, testSql)) {
      return;
    }

    // Create an idempotent conflict, after sql shall be executed on the same region as testSql
    if (!TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Arrays.asList(testSql, afterSql))) {
      return;
    }

    // Assume that the afterSql is executed on receiverEnv
    TestUtils.assertDataEventuallyOnEnv(receiverEnv, afterSqlQuery, expectedHeader, expectedResSet);
  }
}
