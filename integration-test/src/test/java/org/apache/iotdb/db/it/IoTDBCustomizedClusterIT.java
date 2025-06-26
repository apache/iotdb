/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.exception.PortOccupiedException;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests that may not be satisfied with the default cluster settings. */
@RunWith(IoTDBTestRunner.class)
public class IoTDBCustomizedClusterIT {

  private final Logger logger = LoggerFactory.getLogger(IoTDBCustomizedClusterIT.class);

  @FunctionalInterface
  private interface RestartAction {
    void act(Statement statement, int round, AbstractEnv env) throws Exception;
  }

  @Category(DailyIT.class)
  @Test
  public void testRepeatedlyRestartWholeClusterWithWrite() throws Exception {
    testRepeatedlyRestartWholeCluster(
        (s, i, env) -> {
          if (i != 0) {
            ResultSet resultSet = s.executeQuery("SELECT last s1 FROM root.**");
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertEquals(4, metaData.getColumnCount());
            int cnt = 0;
            while (resultSet.next()) {
              cnt++;
              StringBuilder result = new StringBuilder();
              for (int j = 0; j < metaData.getColumnCount(); j++) {
                result
                    .append(metaData.getColumnName(j + 1))
                    .append(":")
                    .append(resultSet.getString(j + 1))
                    .append(",");
              }
              System.out.println(result);
            }
          }
          s.execute("INSERT INTO root.db1.d1 (time, s1) VALUES (1, 1)");
          s.execute("INSERT INTO root.db2.d1 (time, s1) VALUES (1, 1)");
          s.execute("INSERT INTO root.db3.d1 (time, s1) VALUES (1, 1)");
        });
  }

  @Category(DailyIT.class)
  @Test
  public void testRepeatedlyRestartWholeClusterWithPipeCreation() throws Exception {
    SimpleEnv receiverEnv = new SimpleEnv();
    receiverEnv.initClusterEnvironment(1, 1);
    try {
      testRepeatedlyRestartWholeCluster(
          (s, i, env) -> {
            // use another thread to make creating and restart concurrent
            // otherwise, all tasks will be done before restart and the cluster will not attempt to
            // recover tasks
            s.execute(
                String.format(
                    "CREATE PIPE p%d_1 WITH SINK ('sink'='iotdb-thrift-sink', 'node-urls' = '%s')",
                    i, receiverEnv.getDataNodeWrapper(0).getIpAndPortString()));
            s.execute(
                String.format(
                    "CREATE PIPE p%d_2 WITH SINK ('sink'='iotdb-thrift-sink', 'node-urls' = '%s')",
                    i, receiverEnv.getDataNodeWrapper(0).getIpAndPortString()));
            s.execute(
                String.format(
                    "CREATE PIPE p%d_3 WITH SINK ('sink'='iotdb-thrift-sink', 'node-urls' = '%s')",
                    i, receiverEnv.getDataNodeWrapper(0).getIpAndPortString()));
          });
    } finally {
      receiverEnv.cleanClusterEnvironment();
    }
  }

  private void testRepeatedlyRestartWholeCluster(RestartAction restartAction) throws Exception {
    SimpleEnv simpleEnv = new SimpleEnv();
    try {
      simpleEnv
          .getConfig()
          .getCommonConfig()
          .setDataReplicationFactor(3)
          .setSchemaReplicationFactor(3)
          .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus")
          .setConfigNodeConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus")
          .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.iot.IoTConsensus");
      simpleEnv.initClusterEnvironment(3, 3);

      int repeat = 100;
      for (int i = 0; i < repeat; i++) {
        logger.info("Round {} restart", i);
        try (Connection connection = simpleEnv.getConnection();
            Statement statement = connection.createStatement()) {
          ResultSet resultSet = statement.executeQuery("SHOW CLUSTER");
          ResultSetMetaData metaData = resultSet.getMetaData();
          int columnCount = metaData.getColumnCount();
          while (resultSet.next()) {
            StringBuilder row = new StringBuilder();
            for (int j = 0; j < columnCount; j++) {
              row.append(metaData.getColumnName(j + 1))
                  .append(":")
                  .append(resultSet.getString(j + 1))
                  .append(",");
            }
            System.out.println(row);
          }

          restartAction.act(statement, i, simpleEnv);
        }

        simpleEnv.shutdownAllConfigNodes();
        simpleEnv.shutdownAllDataNodes();

        simpleEnv.startAllConfigNodes();
        simpleEnv.startAllDataNodes();

        simpleEnv.clearClientManager();

        try {
          simpleEnv.checkClusterStatusWithoutUnknown();
        } catch (PortOccupiedException e) {
          logger.info(
              "Some ports are occupied during restart, which cannot be processed, just pass the test.");
          return;
        }
      }

    } finally {
      simpleEnv.cleanClusterEnvironment();
    }
  }

  /**
   * When the wal size exceeds `walThrottleSize` * 0.8, the timed wal-delete-thread will try
   * deleting wal forever, which will block the DataNode from exiting, because task of deleting wal
   * submitted by the ShutdownHook cannot be executed. This test ensures that this blocking is
   * fixed.
   */
  @Test
  public void testWalThrottleStuck()
      throws SQLException,
          IoTDBConnectionException,
          StatementExecutionException,
          InterruptedException {
    SimpleEnv simpleEnv = new SimpleEnv();
    simpleEnv
        .getConfig()
        .getDataNodeConfig()
        .setWalThrottleSize(1)
        .setDeleteWalFilesPeriodInMs(100);
    simpleEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(3)
        .setSchemaReplicationFactor(3)
        .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus")
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.iot.IoTConsensus");
    try {
      simpleEnv.initClusterEnvironment(1, 3);

      int leaderIndex = -1;
      try (Connection connection = simpleEnv.getConnection();
          Statement statement = connection.createStatement()) {
        // write the first data
        statement.execute("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
        // find the leader of the data region
        int port = -1;

        ResultSet resultSet = statement.executeQuery("SHOW REGIONS");
        while (resultSet.next()) {
          String regionType = resultSet.getString("Type");
          if (regionType.equals("DataRegion")) {
            String role = resultSet.getString("Role");
            if (role.equals("Leader")) {
              port = resultSet.getInt("RpcPort");
              break;
            }
          }
        }

        if (port == -1) {
          fail("Leader not found");
        }

        for (int i = 0; i < simpleEnv.getDataNodeWrapperList().size(); i++) {
          if (simpleEnv.getDataNodeWrapperList().get(i).getPort() == port) {
            leaderIndex = i;
            break;
          }
        }
      }

      // stop a follower
      int followerIndex = (leaderIndex + 1) % simpleEnv.getDataNodeWrapperList().size();
      simpleEnv.getDataNodeWrapperList().get(followerIndex).stop();
      System.out.println(
          new Date()
              + ":Stopping data node "
              + simpleEnv.getDataNodeWrapperList().get(followerIndex).getIpAndPortString());

      DataNodeWrapper leader = simpleEnv.getDataNodeWrapperList().get(leaderIndex);
      // write to the leader to generate wal that cannot be synced
      try (Session session = new Session(leader.getIp(), leader.getPort())) {
        session.open();

        session.executeNonQueryStatement("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
        session.executeNonQueryStatement("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
        session.executeNonQueryStatement("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
        session.executeNonQueryStatement("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
        session.executeNonQueryStatement("INSERT INTO root.db1.d1 (time, s1) values (1,1)");
      }

      // wait for wal-delete thread to be scheduled
      Thread.sleep(1000);

      // stop the leader
      leader.getInstance().destroy();
      System.out.println(new Date() + ":Stopping data node " + leader.getIpAndPortString());
      // confirm the death of the leader
      long startTime = System.currentTimeMillis();
      while (leader.isAlive()) {
        if (System.currentTimeMillis() - startTime > 30000) {
          fail("Leader does not exit after 30s");
        }
      }
    } finally {
      simpleEnv.cleanClusterEnvironment();
    }
  }
}
