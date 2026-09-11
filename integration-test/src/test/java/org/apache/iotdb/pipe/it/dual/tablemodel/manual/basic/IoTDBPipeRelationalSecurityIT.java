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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeRelationalSecurityIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
  }

  @Test
  public void testReceiverSecurityPrivilegeForRelationalAuthSnapshot() throws Exception {
    final String receiverUser = "pipe_receiver";
    final String replicatedUser = "pipe_replicated_user";
    final String password = "passwD@123456";
    final String databaseName = "pipe_auth_db";
    final String tableName = "pipe_auth_table";

    TestUtils.executeNonQueries(
        "information_schema",
        BaseEnv.TABLE_SQL_DIALECT,
        receiverEnv,
        Arrays.asList(
            String.format("create user %s '%s'", receiverUser, password),
            String.format("grant security to user %s", receiverUser),
            String.format("grant create, insert on any to user %s", receiverUser)),
        null);

    TestUtils.executeNonQueries(
        "information_schema",
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        Arrays.asList(
            String.format("create user %s '%s'", replicatedUser, password),
            String.format(
                "grant create, drop, alter, select, insert, delete on any to user %s",
                replicatedUser)),
        null);
    TableModelUtils.createDataBaseAndTable(senderEnv, tableName, databaseName);
    TableModelUtils.insertData(databaseName, tableName, 0, 100, senderEnv);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();
    sourceAttributes.put("source.inclusion", "all");
    sourceAttributes.put("source.capture.tree", "false");
    sourceAttributes.put("source.capture.table", "true");
    sourceAttributes.put("__system.sql-dialect", "table");
    sourceAttributes.put("user", "root");
    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put("sink.port", Integer.toString(receiverDataNode.getPort()));
    sinkAttributes.put("sink.user", receiverUser);
    sinkAttributes.put("sink.password", password);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("securityAuthPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .startPipeExtended(new TStartPipeReq("securityAuthPipe").setIsTableModel(true))
              .getCode());
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        String.format("list privileges of user %s", replicatedUser),
        "Role,Scope,Privileges,GrantOption,",
        new HashSet<>(
            Arrays.asList(
                ",*.*,CREATE,false,",
                ",*.*,DROP,false,",
                ",*.*,ALTER,false,",
                ",*.*,SELECT,false,",
                ",*.*,INSERT,false,",
                ",*.*,DELETE,false,")),
        "information_schema");
    TableModelUtils.assertCountData(
        databaseName,
        tableName,
        100,
        receiverEnv,
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        });
  }
}
