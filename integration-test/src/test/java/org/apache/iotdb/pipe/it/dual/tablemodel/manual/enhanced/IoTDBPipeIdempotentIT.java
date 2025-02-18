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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeIdempotentIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testCreateTableIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create table test()");
  }

  @Test
  public void testAlterTableAddColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"), "alter table test add column a id");
  }

  @Test
  public void testAlterTableSetPropertiesIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"),
        "alter table test set properties ttl=100");
  }

  @Test
  public void testAlterTableDropColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a id, b attribute, c int32)"),
        "alter table test drop column b");
  }

  @Test
  public void testDropTableIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.singletonList("create table test()"), "drop table test");
  }

  @Test
  public void testTableCreateUserIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create user newUser 'password'");
  }

  @Test
  public void testTableDropUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'password'"), "drop user newUser");
  }

  @Test
  public void testTableCreateRoleIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create role newRole");
  }

  @Test
  public void testTableDropRoleIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create role newRole"), "drop role newRole");
  }

  @Test
  public void testTableAlterUserIdempotent3() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'password'"),
        "alter user newUser set password 'passwd'");
  }

  @Test
  public void testTableGrantRoleToUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList("create user newUser 'password'", "create role newRole"),
        "grant role newRole to newUser");
  }

  @Test
  public void testTableRevokeRoleFromUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'password'",
            "create role newRole",
            "grant role newRole to newUser"),
        "revoke role newRole from newUser");
  }

  @Test
  public void testTableGrantIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'password'"),
        "grant all to user newUser with grant option");
  }

  @Test
  public void testTableRevokeIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'password'", "grant all to user newUser with grant option"),
        "revoke all from user newUser");
  }

  private void testTableConfigIdempotent(final List<String> beforeSqlList, final String testSql)
      throws Exception {
    final String database = "test";
    TableModelUtils.createDatabase(senderEnv, database);
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
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.capture.tree", "false");

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
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, beforeSqlList)) {
      return;
    }

    if (!TestUtils.tryExecuteNonQueryWithRetry(
        database, BaseEnv.TABLE_SQL_DIALECT, receiverEnv, testSql)) {
      return;
    }

    // Create an idempotent conflict
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, testSql)) {
      return;
    }

    TableModelUtils.createDatabase(senderEnv, "test2");

    // Assume that the "database" is executed on receiverEnv
    TestUtils.assertDataSizeEventuallyOnEnv(receiverEnv, "show databases", 3, null);
  }
}
