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

package org.apache.iotdb.session.it;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBConnectionsIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectionsIT.class);
  private static final String SHOW_DATANODES = "show datanodes";
  private static final int COLUMN_AMOUNT = 6;
  private static Set<Integer> allDataNodeId = new HashSet<>();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig();
    EnvFactory.getEnv().initClusterEnvironment(1, 2);
    createUser("test", "test123123456");
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      // Get all data nodes
      ResultSet result = statement.executeQuery(SHOW_DATANODES);
      while (result.next()) {
        allDataNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // Create two connections on the different datanode, validate normal test case.
  @Test
  public void testDifferentDataNodeGetConnections() {
    Connection conn = null;
    int dataNodeId = (int) allDataNodeId.toArray()[0];
    // Create the first connection on the datanode.
    try {
      Connection connection =
          EnvFactory.getEnv()
              .getConnection(
                  EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).get(),
                  CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                  CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                  BaseEnv.TABLE_SQL_DIALECT);
      Statement statement = connection.createStatement();
      statement.execute("USE information_schema");
      ResultSet resultSet = statement.executeQuery("SELECT * FROM connections");
      if (!resultSet.next()) {
        fail();
      }

      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(COLUMN_AMOUNT, metaData.getColumnCount());
      while (resultSet.next()) {
        LOGGER.info(
            "{}, {}, {}, {}, {}, {}",
            resultSet.getString(1),
            resultSet.getString(2),
            resultSet.getString(3),
            resultSet.getString(4),
            resultSet.getString(5),
            resultSet.getString(6));
      }

      conn = connection;
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }

    int anotherDataNodeId = (int) allDataNodeId.toArray()[1];
    // Create the second connection on the datanode.
    try (Connection connection1 =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().dataNodeIdToWrapper(anotherDataNodeId).get(),
                    CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                    CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement1 = connection1.createStatement()) {
      statement1.execute("USE information_schema");
      ResultSet resultSet1 = statement1.executeQuery("SELECT COUNT(*) FROM connections");
      if (!resultSet1.next()) {
        fail();
      }

      while (resultSet1.next()) {
        // Before close the first connection, the current record count must be two.
        Assert.assertEquals(2, resultSet1.getInt(1));
      }

      conn.close();

      ResultSet resultSet2 = statement1.executeQuery("SELECT COUNT(*) FROM connections");
      if (!resultSet2.next()) {
        fail();
      }

      while (resultSet2.next()) {
        // After close the first connection, the current record count change into one.
        Assert.assertEquals(1, resultSet2.getInt(1));
      }
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }
  }

  // Create two connections on the same datanode, validate normal test case.
  @Test
  public void testSameDataNodeGetConnections() {
    Connection conn = null;
    int dataNodeId = (int) allDataNodeId.toArray()[0];
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).get(),
                    CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                    CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE information_schema");

      ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM connections WHERE datanode_id = '" + dataNodeId + "'");
      if (!resultSet.next()) {
        fail();
      }

      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(COLUMN_AMOUNT, metaData.getColumnCount());
      while (resultSet.next()) {
        LOGGER.info(
            "{}, {}, {}, {}, {}, {}",
            resultSet.getString(1),
            resultSet.getString(2),
            resultSet.getString(3),
            resultSet.getString(4),
            resultSet.getTimestamp(5),
            resultSet.getString(6));
      }

      conn = connection;
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }

    // Create the second connection on the same datanode.
    try (Connection connection1 =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).get(),
                    CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                    CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement1 = connection1.createStatement()) {
      statement1.execute("USE information_schema");
      ResultSet resultSet1 = statement1.executeQuery("SELECT COUNT(*) FROM connections");
      if (!resultSet1.next()) {
        fail();
      }

      while (resultSet1.next()) {
        // Before close the first connection, the current record count must be two.
        Assert.assertEquals(2, resultSet1.getInt(1));
      }

      conn.close();

      ResultSet resultSet2 = statement1.executeQuery("SELECT COUNT(*) FROM connections");
      if (!resultSet2.next()) {
        fail();
      }

      while (resultSet2.next()) {
        // After close the first connection, the current record count change into one.
        Assert.assertEquals(1, resultSet2.getInt(1));
      }
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }
  }

  // Validate normal test case when close one datanode.
  @Test
  public void testClosedDataNodeGetConnections() throws Exception {
    if (allDataNodeId.size() <= 1) {
      return;
    }
    int closedDataNodeId = (int) allDataNodeId.toArray()[0];
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().dataNodeIdToWrapper(closedDataNodeId).get(),
                    CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                    CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE information_schema");

      ResultSet resultSet =
          statement.executeQuery(
              "SELECT COUNT(*) FROM connections WHERE datanode_id = '" + closedDataNodeId + "'");
      if (!resultSet.next()) {
        fail();
      }
      // All records corresponding the datanode exist Before close the datanode. Validate result
      // larger than zero.
      Assert.assertTrue(resultSet.getInt(1) > 0);
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }

    // close the number closedDataNodeId datanode
    EnvFactory.getEnv().dataNodeIdToWrapper(closedDataNodeId).get().stop();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Wait for shutdown check
      while (true) {
        AtomicBoolean containUnknown = new AtomicBoolean(false);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.Unknown.getStatus().equals(dataNodeInfo.getStatus())) {
                    containUnknown.set(true);
                  }
                });

        if (containUnknown.get()) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }

    int activeDataNodeId = (int) allDataNodeId.toArray()[1];
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().dataNodeIdToWrapper(activeDataNodeId).get(),
                    CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                    CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE information_schema");

      ResultSet resultSet =
          statement.executeQuery(
              "SELECT COUNT(*) FROM connections WHERE datanode_id = '" + closedDataNodeId + "'");
      if (!resultSet.next()) {
        fail();
      }
      // All records corresponding the datanode  will be cleared After close the datanode. Validate
      // result if it is zero.
      Assert.assertEquals(0, resultSet.getLong(1));
    } catch (Exception e) {
      LOGGER.error("{}", e.getMessage(), e);
      fail(e.getMessage());
    }

    // revert environment
    EnvFactory.getEnv().dataNodeIdToWrapper(closedDataNodeId).get().start();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Wait for restart check
      while (true) {
        AtomicBoolean containUnknown = new AtomicBoolean(false);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.Unknown.getStatus().equals(dataNodeInfo.getStatus())) {
                    containUnknown.set(true);
                  }
                });

        if (!containUnknown.get()) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }

  @Test
  public void testNoAuthUserGetConnections() {
    try (Connection connection =
            EnvFactory.getEnv().getConnection("test", "test123123456", TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE information_schema");
      ResultSet resultSet = statement.executeQuery("SELECT * FROM connections");
      if (!resultSet.next()) {
        fail();
      }
      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(COLUMN_AMOUNT, metaData.getColumnCount());
    } catch (SQLException e) {
      Assert.assertEquals(
          "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
          e.getMessage());
    }
  }
}
