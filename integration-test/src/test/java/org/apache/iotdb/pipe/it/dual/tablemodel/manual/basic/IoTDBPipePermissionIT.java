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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipePermissionIT extends AbstractPipeTableModelDualManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testPermission() {
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "create user `thulab` 'passwd'")) {
      return;
    }

    // Shall fail if username is specified without password
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab'"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("When the 'user' or 'username' is specified, password must be specified too.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Shall fail if password is wrong
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab'"
                  + "'password'='hack'"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("Shall fail if password is wrong.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Use current session, user is root
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    // Alter to another user, shall fail because of lack of password
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('user'='thulab')");
      fail("Alter pipe shall fail if only user is specified");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Successfully alter
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('user'='thulab', 'password'='passwd')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Alter pipe shall not fail if user and password are specified");
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");

    // Shall not be transferred
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "count databases",
        "count,",
        Collections.singleton("1,"),
        "information_schema");

    // Grant some privilege
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        "test", BaseEnv.TABLE_SQL_DIALECT, senderEnv, "grant INSERT on any to user `thulab`")) {
      return;
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");

    // Shall be transferred
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show tables from test1",
        "TableName,TTL(ms),",
        Collections.singleton("test1,INF,"),
        "information_schema");

    // Alter pipe, throw exception if no privileges
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
         final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('skipif'='')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
