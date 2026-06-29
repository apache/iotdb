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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSetConfigurationIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSetConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"=\"false\"");
      statement.execute("set configuration \"enable_unseq_space_compaction\"=\"false\" on 0");
      statement.execute("set configuration \"enable_cross_space_compaction\"=\"false\" on 1");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "enable_seq_space_compaction=false",
                        "enable_unseq_space_compaction=false")));
    Assert.assertTrue(
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "enable_seq_space_compaction=false",
                        "enable_cross_space_compaction=false")));
  }

  /**
   * Regression test for V2-995: the topology-probing config items were left commented out in the
   * template, so {@code getConfigurationItemsFromTemplate} never recorded them as known defaults
   * and {@code set configuration} rejected them as "immutable or undefined". After uncommenting
   * them, {@code set configuration} must accept and persist them: {@code enable_topology_probing}
   * (hot_reload) and {@code topology_probing_base_interval_in_ms} / {@code
   * topology_probing_timeout_ratio} (restart).
   */
  @Test
  public void testSetTopologyProbingConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_topology_probing\"=\"true\"");
      statement.execute("set configuration \"topology_probing_base_interval_in_ms\"=\"3000\"");
      statement.execute("set configuration \"topology_probing_timeout_ratio\"=\"0.4\"");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "enable_topology_probing=true",
                        "topology_probing_base_interval_in_ms=3000",
                        "topology_probing_timeout_ratio=0.4")));
  }

  @Test
  public void testHotReloadHeartbeatInterval() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"heartbeat_interval_in_ms\"=\"500\"");
      assertAppliedConfiguration(0, "heartbeat_interval_in_ms", "500");
      assertAppliedConfiguration(
          EnvFactory.getEnv().getConfigNodeWrapperList().size(), "heartbeat_interval_in_ms", "500");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("set configuration \"heartbeat_interval_in_ms\"=\"1000\"");
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(nodeWrapper, "heartbeat_interval_in_ms=1000")));
  }

  @Test
  public void testRejectedHotReloadDoesNotUpdateAppliedConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"heartbeat_interval_in_ms\"=\"500\"");
      assertAppliedConfiguration(0, "heartbeat_interval_in_ms", "500");
      assertAppliedConfiguration(
          EnvFactory.getEnv().getConfigNodeWrapperList().size(), "heartbeat_interval_in_ms", "500");

      assertSetConfigurationFailed(
          statement,
          "set configuration \"heartbeat_interval_in_ms\"=\"-1\"",
          "heartbeat_interval_in_ms should be greater than 0");
      assertAppliedConfiguration(0, "heartbeat_interval_in_ms", "500");
      assertAppliedConfiguration(
          EnvFactory.getEnv().getConfigNodeWrapperList().size(), "heartbeat_interval_in_ms", "500");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("set configuration \"heartbeat_interval_in_ms\"=\"1000\"");
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(nodeWrapper, "heartbeat_interval_in_ms=1000")));
  }

  @Test
  public void testHotReloadContinuousQueryMinEveryInterval() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"continuous_query_min_every_interval_in_ms\"=\"50\"");
      assertAppliedConfiguration(0, "continuous_query_min_every_interval_in_ms", "50");
      assertAppliedConfiguration(
          EnvFactory.getEnv().getConfigNodeWrapperList().size(),
          "continuous_query_min_every_interval_in_ms",
          "50");
      statement.execute(
          "CREATE CQ hot_reload_cq\n"
              + "RESAMPLE EVERY 50ms\n"
              + "BEGIN \n"
              + "  SELECT count(s1)  \n"
              + "    INTO root.sg_count.d(count_s1)\n"
              + "    FROM root.sg.d\n"
              + "    GROUP BY(30m)\n"
              + "END");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("DROP CQ hot_reload_cq");
        } catch (SQLException ignored) {
          // The CQ may not exist if the hot-reload assertion failed before creation.
        }
        statement.execute(
            "set configuration \"continuous_query_min_every_interval_in_ms\"=\"1000\"");
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper, "continuous_query_min_every_interval_in_ms=1000")));
  }

  @Test
  public void testHotReloadRegionGroupExtensionConfiguration() {
    String database = "root.hot_reload_region";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"schema_region_group_extension_policy\"=\"CUSTOM\"");
      statement.execute("set configuration \"data_region_group_extension_policy\"=\"CUSTOM\"");
      statement.execute("set configuration \"default_schema_region_group_num_per_database\"=\"2\"");
      statement.execute("set configuration \"default_data_region_group_num_per_database\"=\"3\"");
      statement.execute("set configuration \"schema_region_per_data_node\"=\"2.0\"");
      statement.execute("set configuration \"data_region_per_data_node\"=\"2.0\"");

      assertAppliedConfiguration(0, "schema_region_group_extension_policy", "CUSTOM");
      assertAppliedConfiguration(0, "data_region_group_extension_policy", "CUSTOM");
      assertAppliedConfiguration(0, "default_schema_region_group_num_per_database", "2");
      assertAppliedConfiguration(0, "default_data_region_group_num_per_database", "3");
      assertShowVariable(statement, ColumnHeaderConstant.SCHEMA_REGION_PER_DATA_NODE, "2");
      assertShowVariable(statement, ColumnHeaderConstant.DATA_REGION_PER_DATA_NODE, "2");
      assertSetConfigurationFailed(
          statement,
          "set configuration \"schema_region_per_data_node\"=\"1.9\"",
          "schema_region_per_data_node should be an integer");
      assertShowVariable(statement, ColumnHeaderConstant.SCHEMA_REGION_PER_DATA_NODE, "2");
      assertSetConfigurationFailed(
          statement,
          "set configuration \"data_region_per_data_node\"=\"1.9\"",
          "data_region_per_data_node should be an integer");
      assertShowVariable(statement, ColumnHeaderConstant.DATA_REGION_PER_DATA_NODE, "2");

      statement.execute("CREATE DATABASE " + database);
      statement.execute("INSERT INTO " + database + ".d(timestamp, s1) VALUES (1, 1)");
      assertRegionGroupNum(statement, database, 2, 3);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("DELETE DATABASE " + database);
        } catch (SQLException ignored) {
          // The database may not exist if the hot-reload assertion failed before creation.
        }
        statement.execute("set configuration \"schema_region_group_extension_policy\"=\"AUTO\"");
        statement.execute("set configuration \"data_region_group_extension_policy\"=\"AUTO\"");
        statement.execute(
            "set configuration \"default_schema_region_group_num_per_database\"=\"1\"");
        statement.execute("set configuration \"default_data_region_group_num_per_database\"=\"2\"");
        statement.execute("set configuration \"schema_region_per_data_node\"=\"1\"");
        statement.execute("set configuration \"data_region_per_data_node\"=\"0\"");
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "schema_region_group_extension_policy=AUTO",
                        "data_region_group_extension_policy=AUTO",
                        "default_schema_region_group_num_per_database=1",
                        "default_data_region_group_num_per_database=2",
                        "schema_region_per_data_node=1",
                        "data_region_per_data_node=0")));
  }

  @Test
  public void testHotReloadReadConsistencyLevel() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      assertSetConsistentClusterConfigurationOnSpecificNodeFailed(statement);
      statement.execute("set configuration \"read_consistency_level\"=\"weak\"");
      assertAppliedConfiguration(0, "read_consistency_level", "weak");
      assertAppliedConfiguration(
          EnvFactory.getEnv().getConfigNodeWrapperList().size(), "read_consistency_level", "weak");
      assertShowVariable(statement, ColumnHeaderConstant.READ_CONSISTENCY_LEVEL, "weak");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("set configuration \"read_consistency_level\"=\"strong\"");
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(nodeWrapper, "read_consistency_level=strong")));
  }

  @Test
  public void testSetClusterName() throws Exception {
    // set cluster name on cn and dn
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"cluster_name\"=\"xx\"");
      ResultSet variables = statement.executeQuery("show variables");
      variables.next();
      Assert.assertEquals("xx", variables.getString(2));
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(nodeWrapper -> checkConfigFileContains(nodeWrapper, "cluster_name=xx")));
    // restart successfully
    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    EnvFactory.getEnv().getDataNodeWrapper(0).start();
    // set cluster name on datanode
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollDelay(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement()) {
                statement.execute("set configuration \"cluster_name\"=\"yy\" on 1");
              } catch (Exception e) {
                return false;
              }
              return true;
            });
    // cannot restart
    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    EnvFactory.getEnv().getDataNodeWrapper(0).start();
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> !EnvFactory.getEnv().getDataNodeWrapper(0).isAlive());
    Assert.assertTrue(
        checkConfigFileContains(EnvFactory.getEnv().getDataNodeWrapper(0), "cluster_name=yy"));

    EnvFactory.getEnv().cleanClusterEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  private static boolean checkConfigFileContains(
      AbstractNodeWrapper nodeWrapper, String... contents) {
    try {
      String systemPropertiesPath =
          nodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String fileContent = new String(Files.readAllBytes(f.toPath()));
      return Arrays.stream(contents).allMatch(fileContent::contains);
    } catch (IOException ignore) {
      return false;
    }
  }

  private static void assertAppliedConfiguration(int nodeId, String key, String value)
      throws Exception {
    try (ITableSession tableSessionConnection = EnvFactory.getEnv().getTableSessionConnection();
        SessionDataSet sessionDataSet =
            tableSessionConnection.executeQueryStatement("show configuration on " + nodeId)) {
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        if (key.equals(iterator.getString(1))) {
          Assert.assertEquals(value, iterator.isNull(2) ? null : iterator.getString(2));
          return;
        }
      }
    }
    Assert.fail("Cannot find applied configuration: " + key);
  }

  private static void assertSetConsistentClusterConfigurationOnSpecificNodeFailed(
      Statement statement) throws SQLException {
    try {
      statement.execute("set configuration \"read_consistency_level\"=\"weak\" on 0");
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "must be consistent across the entire cluster and only one can be set at a time"));
      return;
    }
    Assert.fail("Set consistent cluster configuration on a specific node should fail.");
  }

  private static void assertSetConfigurationFailed(
      Statement statement, String sql, String expectedMessage) throws SQLException {
    try {
      statement.execute(sql);
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains(expectedMessage));
      return;
    }
    Assert.fail("Set configuration should fail: " + sql);
  }

  private static void assertShowVariable(Statement statement, String key, String value)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("show variables")) {
      while (resultSet.next()) {
        if (key.equals(resultSet.getString(1))) {
          Assert.assertEquals(value, resultSet.getString(2));
          return;
        }
      }
    }
    Assert.fail("Cannot find variable: " + key);
  }

  private static void assertRegionGroupNum(
      Statement statement,
      String database,
      int expectedSchemaRegionGroupNum,
      int expectedDataRegionGroupNum)
      throws SQLException {
    int schemaRegionGroupNum = 0;
    int dataRegionGroupNum = 0;
    try (ResultSet resultSet = statement.executeQuery("show regions of database " + database)) {
      while (resultSet.next()) {
        String regionType = resultSet.getString(ColumnHeaderConstant.TYPE);
        if ("SchemaRegion".equals(regionType)) {
          schemaRegionGroupNum++;
        } else if ("DataRegion".equals(regionType)) {
          dataRegionGroupNum++;
        }
      }
    }
    Assert.assertEquals(expectedSchemaRegionGroupNum, schemaRegionGroupNum);
    Assert.assertEquals(expectedDataRegionGroupNum, dataRegionGroupNum);
  }

  @Test
  public void testSetDefaultSGLevel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // legal value
      statement.execute("set configuration \"default_database_level\"=\"3\"");
      statement.execute("INSERT INTO root.a.b.c.d1(timestamp, s1) VALUES (1, 1)");
      ResultSet databases = statement.executeQuery("show databases root.a.**");
      databases.next();
      Assert.assertEquals("root.a.b.c", databases.getString(1));
      assertFalse(databases.next());

      // path too short
      try {
        statement.execute("INSERT INTO root.fail(timestamp, s1) VALUES (1, 1)");
      } catch (SQLException e) {
        assertEquals(
            "509: An error occurred when executing getDeviceToDatabase():root.fail is not a legal path, because it is no longer than default sg level: 3",
            e.getMessage());
      }

      // illegal value
      try {
        statement.execute("set configuration \"default_database_level\"=\"-1\"");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Illegal defaultDatabaseLevel: -1, should >= 1"));
      }

      // Failed updates will not change the files.
      // Failed updates will not change the files.
      assertFalse(
          checkConfigFileContains(
              EnvFactory.getEnv().getDataNodeWrapper(0), "default_database_level=-1"));
      assertTrue(
          checkConfigFileContains(
              EnvFactory.getEnv().getDataNodeWrapper(0), "default_database_level=3"));
    }

    // can start with an illegal value
    EnvFactory.getEnv().cleanClusterEnvironment();
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultDatabaseLevel(-1);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.a.b.c.d1(timestamp, s1) VALUES (1, 1)");
      ResultSet databases = statement.executeQuery("show databases root.a");
      databases.next();
      // the default value should take effect
      Assert.assertEquals("root.a", databases.getString(1));
      assertFalse(databases.next());

      // create timeseries with an illegal path
      try {
        statement.execute("CREATE TIMESERIES root.db1.s3 WITH datatype=INT32");
      } catch (SQLException e) {
        assertEquals(
            "509: An error occurred when executing getDeviceToDatabase():root.db1 is not a legal path, because it is no longer than default sg level: 3",
            e.getMessage());
      }
    } finally {
      // This test leaves default_database_level=-1 baked into the cluster's config file. Because
      // tests in this class share one cluster and run in random order, a later 'set configuration'
      // test would otherwise fail its hot-reload while re-validating the leftover illegal value
      // ("Illegal defaultDatabaseLevel: -1, should >= 1"). Restore a clean cluster before leaving.
      EnvFactory.getEnv().cleanClusterEnvironment();
      EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultDatabaseLevel(1);
      EnvFactory.getEnv().initClusterEnvironment();
    }
  }
}
