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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotManager;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;

import java.util.Collections;

public class TestDataGroupMember extends DataGroupMember {

  public TestDataGroupMember() {
    super(new PartitionGroup(Collections.singletonList(TestUtils.getNode(0))));
    setQueryManager(new ClusterQueryManager());
    this.slotManager = new SlotManager(ClusterConstant.SLOT_NUM, null, "");
  }

  public TestDataGroupMember(Node thisNode, PartitionGroup allNodes) {
    super(allNodes);
    this.thisNode = thisNode;
    this.slotManager = new SlotManager(ClusterConstant.SLOT_NUM, null, "");
    setQueryManager(new ClusterQueryManager());
  }
}
