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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBTableAggregationQueryWithNetworkPartitionIT {

  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final String IoTConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;
  private static final int testDataRegionGroupPerDatabase = 4;
  protected static final String DATABASE_NAME = "test";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO table1 (time, device, s1) VALUES (1, 'd1', '1')",
        "INSERT INTO table1 (time, device, s1) VALUES (-1, 'd2', '1')",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(IoTConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setDefaultDataRegionGroupNumPerDatabase(testDataRegionGroupPerDatabase);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    ensureAllDataNodeRunning();
  }

  @Test
  public void test1() throws IoTDBConnectionException, StatementExecutionException, SQLException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select count(s1) from table1 where device = 'd1' and time < -1 group by device");
      Assert.assertFalse(sessionDataSet.hasNext());
    }
    EnvFactory.getEnv().shutdownAllDataNodes();
    List<DataNodeWrapper> dataNodeWrapperList = EnvFactory.getEnv().getDataNodeWrapperList();
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapper),
              Collections.singletonList(NodeStatus.Unknown));
    }

    List<String> otherNodes = new ArrayList<>();
    for (int i = 1; i < dataNodeWrapperList.size(); i++) {
      EnvFactory.getEnv().startDataNode(i);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapperList.get(i)),
              Collections.singletonList(NodeStatus.Running));
      DataNodeWrapper dataNodeWrapper = dataNodeWrapperList.get(i);
      otherNodes.add(dataNodeWrapper.getIpAndPortString());
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection(otherNodes)) {
      session.executeNonQueryStatement("use " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select count(s1) from table1 where device = 'd1' and time < -1 group by device");
      Assert.assertFalse(sessionDataSet.hasNext());
    }
  }

  @Test
  public void test2()
      throws IoTDBConnectionException,
          StatementExecutionException,
          SQLException,
          InterruptedException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select device, count(s1) from table1 where (device = 'd1' or device = 'd2') and time < -1 group by device");
      Assert.assertFalse(sessionDataSet.hasNext());
    }
    EnvFactory.getEnv().shutdownAllDataNodes();
    List<DataNodeWrapper> dataNodeWrapperList = EnvFactory.getEnv().getDataNodeWrapperList();
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapper),
              Collections.singletonList(NodeStatus.Unknown));
    }

    List<String> otherNodes = new ArrayList<>();
    for (int i = 1; i < dataNodeWrapperList.size(); i++) {
      EnvFactory.getEnv().startDataNode(i);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapperList.get(i)),
              Collections.singletonList(NodeStatus.Running));
      DataNodeWrapper dataNodeWrapper = dataNodeWrapperList.get(i);
      otherNodes.add(dataNodeWrapper.getIpAndPortString());
    }

    // Until the data is synchronized by IoTConsensus, query results may not be available.
    int maxTestCount = 30;
    for (int i = 0; i < maxTestCount; i++) {
      try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection(otherNodes)) {
        session.executeNonQueryStatement("use " + DATABASE_NAME);
        SessionDataSet sessionDataSet =
            session.executeQueryStatement(
                "select device, count(s1) from table1 where (device = 'd1' or device = 'd2') and time <= -1 group by device");
        int count = 0;
        while (sessionDataSet.hasNext()) {
          sessionDataSet.next();
          count++;
        }
        if (count == 1) {
          return;
        }
      }
      Thread.currentThread().sleep(TimeUnit.SECONDS.toMillis(1));
    }
    Assert.fail();
  }

  private void ensureAllDataNodeRunning() {
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (dataNodeWrapper.isAlive()) {
        continue;
      }
      dataNodeWrapper.start();
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapper),
              Collections.singletonList(NodeStatus.Running));
    }
  }
}
