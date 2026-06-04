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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * HA behavior of table-model DDL: a table DDL must broadcast a cache-invalidation to every
 * DataNode. Before the metadata-lease/fence change it hard-failed whenever any DataNode was
 * unreachable (so a single down DataNode broke CREATE TABLE, contradicting multi-replica HA). With
 * the change the ConfigNode proceeds once the unreachable DataNode is provably self-fenced (it
 * fails closed on its stale caches and resyncs on recovery, so it cannot serve dirty schema).
 *
 * <p>This test stops one DataNode and asserts CREATE TABLE still succeeds.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBTableDDLHAIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // Small fence threshold so the ConfigNode can prove the stopped DataNode is self-fenced quickly
    // (T_proceed = fence + ~5s internal margin), keeping the test fast. Live DataNodes keep
    // heartbeating (~1s), so they do not spuriously fence.
    EnvFactory.getEnv().getConfig().getCommonConfig().setMetadataLeaseFenceMs(2000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void createTableSucceedsWhileOneDataNodeIsDown() throws Exception {
    final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);

    // Pin the connection to a DataNode we will keep alive, so stopping the victim cannot break it.
    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection(liveDataNode, "root", "root", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test_ha");
      statement.execute("USE test_ha");

      // Sanity: with all DataNodes up the DDL broadcast acks everywhere and succeeds immediately.
      statement.execute("CREATE TABLE t_all_up (region STRING TAG, temperature FLOAT FIELD)");

      // Take one DataNode down. Its last successful ConfigNode contact is now frozen; after
      // T_proceed the ConfigNode can treat it as self-fenced and stop waiting for its ack.
      victimDataNode.stop();
      Assert.assertFalse("victim DataNode should be stopped", victimDataNode.isAlive());

      // The DDL broadcast can no longer reach the stopped DataNode. Previously this hard-failed;
      // now it must still succeed (after blocking ~T_proceed while the fence is proven).
      statement.execute("CREATE TABLE t_after_down (region STRING TAG, temperature FLOAT FIELD)");

      // Confirm the new table is really visible on the live DataNode.
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(() -> tableExists(statement, "t_after_down"));
      assertTrue(
          "CREATE TABLE must succeed with one DataNode down",
          tableExists(statement, "t_after_down"));
    }
  }

  private static boolean tableExists(final Statement statement, final String tableName)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW TABLES")) {
      while (resultSet.next()) {
        if (tableName.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }
}
