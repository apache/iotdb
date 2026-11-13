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
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeIdempotentIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  // Creating a table already present on the receiver must not block later database events.
  @Test
  public void testCreateTableIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create table test()");
  }

  // Creating a tree-backed view already present on the receiver must allow sync to continue.
  @Test
  public void testCreateTableViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.emptyList(), "create view test(s1 int32 field) restrict as root.a.**");
  }

  // Adding the same tag to both copies of a table must not block subsequent database events.
  @Test
  public void testAlterTableAddColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"), "alter table test add column a tag");
  }

  // Adding an existing tag to the receiver's view must be treated as an idempotent conflict.
  @Test
  public void testAlterViewAddColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test add column a tag");
  }

  // Reapplying TTL=100 to a table must preserve pipe progress after the duplicate ALTER.
  @Test
  public void testAlterTableSetPropertiesIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test()"),
        "alter table test set properties ttl=100");
  }

  // Reapplying TTL=100 to a view must preserve pipe progress after the duplicate ALTER.
  @Test
  public void testAlterViewSetPropertiesIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test set properties ttl=100");
  }

  // Dropping an attribute already removed on the receiver must not block subsequent events.
  @Test
  public void testAlterTableDropColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag, b attribute, c int32)"),
        "alter table test drop column b");
  }

  // Dropping a view field already removed on the receiver must allow sync to continue.
  @Test
  public void testAlterViewDropColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(a tag, s1 int32 field) restrict as root.a.**"),
        "alter view test drop column s1");
  }

  // Dropping a table already absent on the receiver must not stall later database creation.
  @Test
  public void testDropTableIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.singletonList("create table test()"), "drop table test");
  }

  // Dropping a view already absent on the receiver must not stall later database creation.
  @Test
  public void testDropViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "drop view test");
  }

  // Replaying a view-column rename from a to b must allow subsequent events to be applied.
  @Test
  public void testRenameViewColumnIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(a tag, s1 int32 field) restrict as root.a.**"),
        "alter view test rename column a to b");
  }

  // Replaying a view rename from test to test1 must allow subsequent events to be applied.
  @Test
  public void testRenameViewIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create view test(s1 int32 field) restrict as root.a.**"),
        "alter view test rename to test1");
  }

  // Creating the same user independently on both clusters must not block pipe progress.
  @Test
  public void testTableCreateUserIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create user newUser 'IoTDB!Test2024'");
  }

  // Replaying deletion of a user already absent on the receiver must allow sync to continue.
  @Test
  public void testTableDropUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'IoTDB!Test2024'"), "drop user newUser");
  }

  // Creating the same role independently on both clusters must not block pipe progress.
  @Test
  public void testTableCreateRoleIdempotent() throws Exception {
    testTableConfigIdempotent(Collections.emptyList(), "create role newRole");
  }

  // Replaying deletion of a role already absent on the receiver must allow sync to continue.
  @Test
  public void testTableDropRoleIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create role newRole"), "drop role newRole");
  }

  // Setting the same new password on both copies of a user must not block subsequent events.
  @Test
  public void testTableAlterUserIdempotent3() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'IoTDB!Test2024'"),
        "alter user newUser set password 'IoTDB!Alter2024'");
  }

  // Granting an existing role membership again must allow later database events to sync.
  @Test
  public void testTableGrantRoleToUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList("create user newUser 'IoTDB!Test2024'", "create role newRole"),
        "grant role newRole to newUser");
  }

  // Revoking an already removed role membership must allow later database events to sync.
  @Test
  public void testTableRevokeRoleFromUserIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'IoTDB!Test2024'",
            "create role newRole",
            "grant role newRole to newUser"),
        "revoke role newRole from newUser");
  }

  // Reapplying all privileges with grant option must not block subsequent database events.
  @Test
  public void testTableGrantIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create user newUser 'IoTDB!Test2024'"),
        "grant all to user newUser with grant option");
  }

  // Revoking all privileges a second time must not block subsequent database events.
  @Test
  public void testTableRevokeIdempotent() throws Exception {
    testTableConfigIdempotent(
        Arrays.asList(
            "create user newUser 'IoTDB!Test2024'", "grant all to user newUser with grant option"),
        "revoke all from user newUser");
  }

  // Replaying an identical table comment must allow subsequent database events to sync.
  @Test
  public void testSetTableCommentIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag)"), "COMMENT ON TABLE test is 'tag'");
  }

  // Replaying an identical tag-column comment must allow subsequent database events to sync.
  @Test
  public void testSetTableColumnCommentIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList("create table test(a tag)"), "COMMENT ON COLUMN test.a IS 'tag'");
  }

  // Replaying an INT32-to-INT64 field conversion must not block subsequent database events.
  @Test
  public void testAlterColumnDataTypeIdempotent() throws Exception {
    testTableConfigIdempotent(
        Collections.singletonList(
            "CREATE TABLE t1 (time TIMESTAMP TIME,dId STRING TAG,s1 INT32 FIELD)"),
        "ALTER TABLE t1 ALTER COLUMN s1 SET DATA TYPE INT64");
  }

  private void testTableConfigIdempotent(final List<String> beforeSqlList, final String testSql)
      throws Exception {
    final String database = database("test");
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
      sourceAttributes.put("__system.sql-dialect", "table");
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

    // Pipe creation does not wait for the historical database snapshot to be applied.
    TableModelUtils.hasDataBase(database, receiverEnv);

    TestUtils.executeNonQueries(
        database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, beforeSqlList, null);

    // Pipe transfers config events in order. Use a database event as a progress marker to ensure
    // that all preparation statements have been applied on receiverEnv.
    TableModelUtils.createDatabase(senderEnv, database("test1"));
    TableModelUtils.hasDataBase(database("test1"), receiverEnv);

    TestUtils.executeNonQuery(database, BaseEnv.TABLE_SQL_DIALECT, receiverEnv, testSql, null);

    // Create an idempotent conflict
    TestUtils.executeNonQuery(database, BaseEnv.TABLE_SQL_DIALECT, senderEnv, testSql, null);

    TableModelUtils.createDatabase(senderEnv, database("test2"));

    // The method-specific marker proves progress without counting other tests' databases.
    TableModelUtils.hasDataBase(database("test2"), receiverEnv);
  }

  private String database(final String name) {
    // Database deletion is asynchronous; never reuse another method's database incarnation.
    return name + "_" + testName.getMethodName().toLowerCase(Locale.ROOT);
  }
}
