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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterStartIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBClusterStartIT.class);

  private static final int testConfigNodeNum = 3, testDataNodeNum = 1;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS);

    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void clusterIdTest()
      throws ClientManagerException, IOException, InterruptedException, SQLException {
    final long maxTestTime = TimeUnit.SECONDS.toMillis(30);
    final long testInterval = TimeUnit.SECONDS.toMillis(1);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long startTime = System.currentTimeMillis();
      boolean testPass = false;
      String clusterIdFromConfigNode = null;
      while (System.currentTimeMillis() - startTime < maxTestTime) {
        try {
          TGetClusterIdResp resp = client.getClusterId();
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
            Assert.assertNotNull(resp.getClusterId());
            Assert.assertNotEquals("", resp.getClusterId());
            clusterIdFromConfigNode = resp.getClusterId();
            testPass = true;
            break;
          }
        } catch (TException e) {
          logger.error("TException:", e);
        }
        Thread.sleep(testInterval);
      }
      if (!testPass) {
        String errorMessage = String.format("Cluster ID failed to generate in %d ms.", maxTestTime);
        Assert.fail(errorMessage);
      }
      ResultSet resultSet = statement.executeQuery("show clusterid");
      resultSet.next();
      Assert.assertEquals(clusterIdFromConfigNode, resultSet.getString(1));
    }
  }
}
