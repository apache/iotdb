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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBDeletionTableClusterIT {

  private static final String DATABASE = "sc3";

  @BeforeClass
  public static void setUpClass() throws SQLException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(2)
        .setDataReplicationFactor(2);
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
    try (Connection connection = EnvFactory.getEnv().getConnection(TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * deleting time=2 from three flushed devices must leave exactly (1,d1,1) and (3,d3,3), through
   * every DataNode coordinator, both before and after another flush.
   */
  @Test
  public void testDeleteByTimeAfterFlush() throws SQLException {
    checkDeletionOnEveryDataNode("time_delete_", " WHERE time = 2", 1, 3);
  }

  /**
   * an unconditional DELETE must remove all three flushed rows through every DataNode coordinator,
   * and another flush must not make the deleted data visible again.
   */
  @Test
  public void testDeleteAllAfterFlush() throws SQLException {
    checkDeletionOnEveryDataNode("full_delete_", "");
  }

  private void checkDeletionOnEveryDataNode(
      String tablePrefix, String predicate, int... remainingTimes) throws SQLException {
    for (int i = 0; i < EnvFactory.getEnv().getDataNodeWrapperList().size(); i++) {
      String table = DATABASE + "." + tablePrefix + i;
      // Pin each DELETE to a different coordinator and use fresh data for every attempt.
      try (Connection connection =
              EnvFactory.getEnv()
                  .getWriteOnlyConnectionWithSpecifiedDataNode(
                      EnvFactory.getEnv().getDataNodeWrapper(i), TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE " + table + "(device_id STRING TAG, s1 INT32 FIELD)");
        statement.execute(
            "INSERT INTO "
                + table
                + "(time, device_id, s1) VALUES (1,'d1',1),(2,'d2',2),(3,'d3',3)");
        statement.execute("FLUSH");
        assertRowsOnEveryDataNode(table, 1, 2, 3);

        statement.execute("DELETE FROM " + table + predicate);
        // Successful execution alone cannot detect the silent no-op reported in TDB-449.
        // Allow replica propagation before concluding that the deletion did not take effect.
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> assertRowsOnEveryDataNode(table, remainingTimes));
        statement.execute("FLUSH");
        assertRowsOnEveryDataNode(table, remainingTimes);
      }
    }
  }

  private void assertRowsOnEveryDataNode(String table, int... expectedTimes) throws SQLException {
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      String context = table + " on " + dataNode.getIpAndPortString();
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      dataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + table)) {
          assertTrue(context, resultSet.next());
          assertEquals(context, expectedTimes.length, resultSet.getLong(1));
          assertFalse(context, resultSet.next());
        }
        try (ResultSet resultSet =
            statement.executeQuery("SELECT time, device_id, s1 FROM " + table + " ORDER BY time")) {
          for (int time : expectedTimes) {
            assertTrue(context, resultSet.next());
            assertEquals(context, time, resultSet.getLong("time"));
            assertEquals(context, "d" + time, resultSet.getString("device_id"));
            assertEquals(context, time, resultSet.getInt("s1"));
          }
          assertFalse(context, resultSet.next());
        }
      }
    }
  }
}
