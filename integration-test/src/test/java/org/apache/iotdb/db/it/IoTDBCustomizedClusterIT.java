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

import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import static org.junit.Assert.fail;

/** Tests that may not be satisfied with the default cluster settings. */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBCustomizedClusterIT {

  private final Logger logger = LoggerFactory.getLogger(IoTDBCustomizedClusterIT.class);

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
