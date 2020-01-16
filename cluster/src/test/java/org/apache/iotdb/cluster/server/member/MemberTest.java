/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.junit.After;
import org.junit.Before;

public class MemberTest {

  MetaGroupMember testMetaMember;
  LogManager metaLogManager;
  PartitionTable partitionTable;
  PartitionGroup partitionGroup;
  QueryProcessExecutor queryProcessExecutor;

  private List<String> prevUrls;
  private List<Node> allNodes;

  @Before
  public void setUp() throws Exception {
    prevUrls = ClusterDescriptor.getINSTANCE().getConfig().getSeedNodeUrls();
    List<String> testUrls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      testUrls.add(node.getIp() + ":" + node.getMetaPort() + ":" + node.getDataPort());
    }
    ClusterDescriptor.getINSTANCE().getConfig().setSeedNodeUrls(testUrls);

    partitionGroup = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      partitionGroup.add(TestUtils.getNode(i));
    }

    metaLogManager = new TestLogManager();
    testMetaMember = new TestMetaGroupMember() {
      @Override
      public LogManager getLogManager() {
        return metaLogManager;
      }

      @Override
      public PartitionTable getPartitionTable() {
        return partitionTable;
      }
    };

    allNodes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      allNodes.add(TestUtils.getNode(i));
    }
    partitionTable = new SlotPartitionTable(allNodes, TestUtils.getNode(0), 100);
    EnvironmentUtils.envSetUp();
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().setStorageGroupToMTree(TestUtils.getTestSg(i));
      SchemaUtils.registerTimeseries(TestUtils.getTestSchema(0, i));
    }
    queryProcessExecutor = new QueryProcessExecutor();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    ClusterDescriptor.getINSTANCE().getConfig().setSeedNodeUrls(prevUrls);
  }
}
