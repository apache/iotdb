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
package org.apache.iotdb.cluster.log.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SlotPartitionTableTest {

  SlotPartitionTable table;
  int replica_size = 5;

  @Before
  public void setUp() {
    List<Node> nodes = new ArrayList<>();
    IntStream.range(0, 20).forEach(i -> nodes.add(getNode(i)));
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(replica_size);
    table = new SlotPartitionTable(nodes, nodes.get(3));
  }

  private Node getNode(int i) {
    return new Node("localhost", 30000 + i, i, 40000 + i);
  }

  @After
  public void tearDown() {
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(3);
  }

  @Test
  public void getHeaderGroup() {
    Arrays.stream(new int[]{10, 15, 19}).forEach( i -> {
      int last = (i + replica_size - 1) % 20;
      assertGetHeaderGroup(i, last);
    });
  }

  private void assertGetHeaderGroup(int start, int last) {
    PartitionGroup group = table.getHeaderGroup(new Node("localhost", 30000 + start, start, 40000 + start));
    assertEquals(replica_size, group.size());
    assertEquals(new Node("localhost", 30000 + start, start, 40000 + start), group.getHeader());
    assertEquals(
        new Node("localhost", 30000 + last,  last, 40000 +  last),
        group.get(replica_size - 1));
  }

  @Test
  public void route() {
    table.route("root.sg1", 1);
  }

  @Test
  public void addNode() {
    String a = "中国";
    System.out.println(a.length());
  }

  @Test
  public void getLocalGroups() {
  }

  @Test
  public void serialize() {
  }

  @Test
  public void deserialize() {
  }

  @Test
  public void getAllNodes() {
  }

  @Test
  public void getPreviousNodeMap() {
  }

  @Test
  public void getNodeSlots() {
  }

  @Test
  public void getAllNodeSlots() {
  }

  @Test
  public void testRemoveNode() {
    List<Integer> nodeSlots = table.getNodeSlots(getNode(0));
    NodeRemovalResult nodeRemovalResult = table.removeNode(getNode(0));
    assertFalse(table.getAllNodes().contains(getNode(0)));
    PartitionGroup removedGroup = nodeRemovalResult.getRemovedGroup();
    for (int i = 0; i < 5; i++) {
      assertTrue(removedGroup.contains(getNode(i)));
    }
    PartitionGroup newGroup = nodeRemovalResult.getNewGroup();
    for (int i : new int[] {18, 19, 1, 2, 3}) {
      assertTrue(newGroup.contains(getNode(i)));
    }
    // the slots owned by the removed one should be redistributed to other nodes
    Map<Node, List<Integer>> newSlotOwners = nodeRemovalResult.getNewSlotOwners();
    for (List<Integer> slots : newSlotOwners.values()) {
      assertTrue(nodeSlots.containsAll(slots));
      nodeSlots.removeAll(slots);
    }
    assertTrue(nodeSlots.isEmpty());
  }
}
