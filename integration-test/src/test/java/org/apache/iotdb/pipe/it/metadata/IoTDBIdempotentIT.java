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

package org.apache.iotdb.pipe.it.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Assume;
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
@Category({MultiClusterIT2.class})
public class IoTDBIdempotentIT extends AbstractPipeDualMetaIT {
  @Override
  @Before
  public void setUp() {
    try {
      MultiEnvFactory.createEnv(2);
      senderEnv = MultiEnvFactory.getEnv(0);
      receiverEnv = MultiEnvFactory.getEnv(1);

      senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
      receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

      // Limit the schemaRegion number to 1 to guarantee the after sql executed on the same region
      // of the tested idempotent sql.
      senderEnv.initClusterEnvironment(1, 1);
      receiverEnv.initClusterEnvironment();
    } catch (Exception | Error e) {
      Assume.assumeNoException(e);
    }
  }

  @Test
  public void testTimeseriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "create timeseries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeseries",
        "count(timeseries),",
        Collections.singleton("2,"));
  }

  @Test
  public void testInternalCreateTimeseriesIdempotent() throws Exception {
    testIdempotent(
        Collections.emptyList(),
        "insert into root.test.g1(time, speed) values(now(), 1.0);",
        "create timeseries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
        "count timeseries",
        "count(timeseries),",
        Collections.singleton("2,"));
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
  public void testActivateTemplateIdempotent() throws Exception {
    testIdempotent(
        Arrays.asList(
            "create schema template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set schema template t1 to root.sg1"),
        "create timeseries using device template on root.sg1.d1",
        "create timeseries using device template on root.sg1.d2",
        "count timeseries",
        "count(timeseries),",
        Collections.singleton("6,"));
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
      List<String> beforeSqlList,
      String testSql,
      String afterSql,
      String afterSqlQuery,
      String expectedHeader,
      Set<String> expectedResSet)
      throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      TSStatus status =
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
