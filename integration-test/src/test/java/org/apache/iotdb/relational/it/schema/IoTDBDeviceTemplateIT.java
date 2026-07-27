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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBDeviceTemplateIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDeviceTemplateIT.class);

  private static final String DATABASE = "root.device_template_ha";
  private static final String TEMPLATE1 = "temp1";
  private static final String TEMPLATE2 = "temp2";
  private static final String DEVICE1 = DATABASE + ".dev01";
  private static final String DEVICE2 = DATABASE + ".dev02";

  private static void initCluster() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(20000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  private static void cleanCluster() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void preSetup(final Statement statement) throws Exception {
    statement.execute("CREATE DATABASE " + DATABASE);
    statement.execute("CREATE DEVICE TEMPLATE " + TEMPLATE1 + " (s1 INT32, s2 INT64)");
    statement.execute("SET DEVICE TEMPLATE " + TEMPLATE1 + " TO " + DATABASE);
    statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON " + DEVICE1);
    statement.execute("INSERT INTO " + DEVICE1 + "(time, s1, s2) VALUES(1, 1, 1)");
  }

  @Test
  public void testHAWithOneDataNodeIsDown() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TREE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {

        preSetup(statement);

        victimDataNode.stop();
        Assert.assertFalse("victim DataNode should be stopped", victimDataNode.isAlive());

        executeDeviceTemplateHATests(statement);
      }
    } finally {
      cleanCluster();
    }
  }

  @Test
  public void testHAWithOneDataNodeIsReadOnly() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);

      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TREE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {

        preSetup(statement);

        try (final Connection victimConn =
                EnvFactory.getEnv()
                    .getConnection(
                        victimDataNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TABLE_SQL_DIALECT);
            final Statement victimStmt = victimConn.createStatement()) {

          victimStmt.execute("SET SYSTEM TO READONLY ON LOCAL");

          EnvFactory.getEnv()
              .ensureNodeStatus(
                  Collections.singletonList(victimDataNode),
                  Collections.singletonList(NodeStatus.ReadOnly));
        }

        executeDeviceTemplateHATests(statement);
      }
    } finally {
      cleanCluster();
    }
  }

  private void executeDeviceTemplateHATests(final Statement statement) throws Exception {
    // 1. SHOW — verify all SHOW operations work
    LOGGER.info("1. start to test high availability of show device template operations");
    assertTrue(
        "SHOW DEVICE TEMPLATES must include " + TEMPLATE1, templateExists(statement, TEMPLATE1));
    assertTrue(
        "SHOW NODES IN DEVICE TEMPLATE must include s1",
        templateHasMeasurement(statement, TEMPLATE1, "s1"));
    assertTrue(
        "SHOW PATHS SET DEVICE TEMPLATE must include " + DATABASE,
        pathSetTemplate(statement, TEMPLATE1, DATABASE));
    assertTrue(
        "SHOW PATHS USING DEVICE TEMPLATE must include " + DEVICE1,
        pathUsingTemplate(statement, TEMPLATE1, DEVICE1));

    // 2. ALTER — add a measurement to t1
    LOGGER.info("2. start to test high availability of alter device template procedure");
    assertStatementEffect(
        statement,
        "ALTER DEVICE TEMPLATE " + TEMPLATE1 + " ADD (s3 FLOAT)",
        () -> templateHasMeasurement(statement, TEMPLATE1, "s3"),
        "ALTER DEVICE TEMPLATE must succeed with one DataNode down");

    // 3. DEACTIVATE — deactivate t1 from dev01
    LOGGER.info("3. start to test high availability of deactivate device template procedure");
    assertStatementEffect(
        statement,
        "DEACTIVATE DEVICE TEMPLATE " + TEMPLATE1 + " FROM " + DEVICE1,
        () -> !pathUsingTemplate(statement, TEMPLATE1, DEVICE1),
        "DEACTIVATE DEVICE TEMPLATE must succeed with one DataNode down");

    // 4. UNSET — unset t1 from database
    LOGGER.info("4. start to test high availability of unset device template procedure");
    assertStatementEffect(
        statement,
        "UNSET DEVICE TEMPLATE " + TEMPLATE1 + " FROM " + DATABASE,
        () -> !pathSetTemplate(statement, TEMPLATE1, DATABASE),
        "UNSET DEVICE TEMPLATE must succeed with one DataNode down");

    // 5. DROP — drop t1
    LOGGER.info("5. start to test high availability of drop device template procedure");
    assertStatementEffect(
        statement,
        "DROP DEVICE TEMPLATE " + TEMPLATE1,
        () -> !templateExists(statement, TEMPLATE1),
        "DROP DEVICE TEMPLATE must succeed with one DataNode down");

    // 6. CREATE — create a new aligned template t2
    LOGGER.info("6. start to test high availability of create device template procedure");
    assertStatementEffect(
        statement,
        "CREATE DEVICE TEMPLATE " + TEMPLATE2 + " ALIGNED (lat FLOAT, lon FLOAT)",
        () -> templateExists(statement, TEMPLATE2),
        "CREATE DEVICE TEMPLATE must succeed with one DataNode down");
    assertTrue(
        "SHOW NODES IN DEVICE TEMPLATE must include lat",
        templateHasMeasurement(statement, TEMPLATE2, "lat"));
    assertTrue(
        "SHOW NODES IN DEVICE TEMPLATE must include lon",
        templateHasMeasurement(statement, TEMPLATE2, "lon"));

    // 7. SET — set t2 to database
    LOGGER.info("7. start to test high availability of set device template procedure");
    assertStatementEffect(
        statement,
        "SET DEVICE TEMPLATE " + TEMPLATE2 + " TO " + DATABASE,
        () -> pathSetTemplate(statement, TEMPLATE2, DATABASE),
        "SET DEVICE TEMPLATE must succeed with one DataNode down");

    // 8. ACTIVATE — activate t2 on dev02
    LOGGER.info("8. start to test high availability of activate device template procedure");
    assertStatementEffect(
        statement,
        "CREATE TIMESERIES OF DEVICE TEMPLATE ON " + DEVICE2,
        () -> pathUsingTemplate(statement, TEMPLATE2, DEVICE2),
        "CREATE TIMESERIES USING DEVICE TEMPLATE must succeed with one DataNode down");
  }

  private void assertStatementEffect(
      final Statement statement,
      final String sql,
      final Callable<Boolean> effect,
      final String message)
      throws Exception {
    statement.execute(sql);
    assertTrue(message, effect.call());
  }

  private boolean templateExists(final Statement statement, final String templateName)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
      while (resultSet.next()) {
        if (templateName.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean templateHasMeasurement(
      final Statement statement, final String templateName, final String measurement)
      throws Exception {
    try (final ResultSet resultSet =
        statement.executeQuery("SHOW NODES IN DEVICE TEMPLATE " + templateName)) {
      while (resultSet.next()) {
        if (measurement.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean pathSetTemplate(
      final Statement statement, final String templateName, final String path) throws Exception {
    try (final ResultSet resultSet =
        statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE " + templateName)) {
      while (resultSet.next()) {
        if (path.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean pathUsingTemplate(
      final Statement statement, final String templateName, final String path) throws Exception {
    try (final ResultSet resultSet =
        statement.executeQuery("SHOW PATHS USING DEVICE TEMPLATE " + templateName)) {
      while (resultSet.next()) {
        if (path.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }
}
