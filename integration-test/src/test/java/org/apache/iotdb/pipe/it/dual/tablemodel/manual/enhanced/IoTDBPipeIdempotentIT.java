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
  public void testCreateTableViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.emptyList(), "create view test(s1 int32 field) restrict as root.a.**");
  }

  @Test
  public void testAlterTableAddColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"), "alter table test add column a tag");
  }

  @Test
  public void testAlterViewAddColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test add column a tag");
  }

  @Test
  public void testAlterTableSetPropertiesIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"),
        "alter table test set properties ttl=100");
  }

  @Test
  public void testAlterViewSetPropertiesIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test set properties ttl=100");
  }

  @Test
  public void testAlterTableDropColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag, b attribute, c int32)"),
        "alter table test drop column b");
  }

  @Test
  public void testAlterViewDropColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(a tag, s1 int32 field) restrict as root.a.**"),
        "alter view test drop column s1");
  }

  @Test
  public void testDropTableIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.singletonList("create table test()"), "drop table test");
  }

  @Test
  public void testDropViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "drop view test");
  }

  @Test
  public void testRenameViewColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(a tag, s1 int32 field) restrict as root.a.**"),
        "alter view test rename column a to b");
  }

  @Test
  public void testRenameViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test rename to test1");
  }

  @Test
  public void testTableCreateUserIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create user newUser 'password123456'");
  }

  @Test
  public void testTableDropUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'password123456'"), "drop user newUser");
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
        Collections.singletonList("create user newUser 'password123456'"),
        "alter user newUser set password 'passwd123456789'");
  }

  @Test
  public void testTableGrantRoleToUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList("create user newUser 'password123456'", "create role newRole"),
        "grant role newRole to newUser");
  }

  @Test
  public void testTableRevokeRoleFromUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'password123456'",
            "create role newRole",
            "grant role newRole to newUser"),
        "revoke role newRole from newUser");
  }

  @Test
  public void testTableGrantIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'password123456'"),
        "grant all to user newUser with grant option");
  }

  @Test
  public void testTableRevokeIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'password123456'", "grant all to user newUser with grant option"),
        "revoke all from user newUser");
  }

  @Test
  public void testSetTableCommentIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag)"), "COMMENT ON TABLE test is 'tag'");
  }

  @Test
  public void testSetTableColumnCommentIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag)"), "COMMENT ON COLUMN test.a IS 'tag'");
  }

  @Test
  public void testAlterColumnDataTypeIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList(
            "CREATE TABLE t1 (time TIMESTAMP TIME,dId STRING TAG,s1 INT32 FIELD)"),
        "ALTER TABLE t1 ALTER COLUMN s1 SET DATA TYPE INT64");
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
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("source.capture.table", "true");
      sourceAttributes.put("source.capture.tree", "false");
      sourceAttributes.put("user", "root");

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
    }

    TestUtils.executeNonQueries(
        database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, beforeSqlList, null);

    TestUtils.executeNonQuery(database, BaseEnv.TABLE_SQL_DIALECT, receiverEnv, testSql, null);

    // Create an idempotent conflict
    TestUtils.executeNonQuery(database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, testSql, null);

    TableModelUtils.createDatabase(senderEnv, "test2");

    // Assume that the "database" is executed on receiverEnv
    TestUtils.assertDataSizeEventuallyOnEnv(receiverEnv, "show databases", 3, null);
  }
}
