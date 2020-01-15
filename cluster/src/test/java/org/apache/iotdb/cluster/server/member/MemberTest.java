/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestPartitionTable;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.junit.After;
import org.junit.Before;

public class MemberTest {

  MetaGroupMember metaGroupMember;
  LogManager metaLogManager;
  private PartitionTable partitionTable;
  PartitionGroup partitionGroup;

  @Before
  public void setUp() throws Exception {
    partitionGroup = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      partitionGroup.add(TestUtils.getNode(i));
    }

    metaLogManager = new TestLogManager();
    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public LogManager getLogManager() {
        return metaLogManager;
      }

      @Override
      public PartitionTable getPartitionTable() {
        return partitionTable;
      }
    };

    partitionTable = new TestPartitionTable() {
      @Override
      public List<Integer> getNodeSlots(Node header) {
        List<Integer> ret = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          ret.add(i);
        }
        return ret;
      }

      @Override
      public PartitionGroup getHeaderGroup(Node header) {
        return partitionGroup;
      }

      @Override
      public Map<Integer, Node> getPreviousNodeMap(Node node) {
        Map<Integer, Node> ret = new HashMap<>();
        for (int i = 0; i < 10; i++) {
          ret.put(i, TestUtils.getNode(i));
        }
        return ret;
      }
    };
    EnvironmentUtils.envSetUp();
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().setStorageGroupToMTree(TestUtils.getTestSg(i));
      SchemaUtils.registerTimeseries(TestUtils.getTestSchema(0, i));
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }
}
