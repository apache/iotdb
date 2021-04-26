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

package org.apache.iotdb.cluster.partition.slot;

import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SlotNodeRemovalResult stores the removed partition group and who will take over its slots. */
public class SlotNodeRemovalResult extends NodeRemovalResult {

  private Map<RaftNode, List<Integer>> newSlotOwners = new HashMap<>();

  public Map<RaftNode, List<Integer>> getNewSlotOwners() {
    return newSlotOwners;
  }

  public void addNewSlotOwners(Map<RaftNode, List<Integer>> newSlotOwners) {
    this.newSlotOwners = newSlotOwners;
  }

  @Override
  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    super.serialize(dataOutputStream);
    dataOutputStream.writeInt(newSlotOwners.size());
    for (Map.Entry<RaftNode, List<Integer>> entry : newSlotOwners.entrySet()) {
      RaftNode raftNode = entry.getKey();
      NodeSerializeUtils.serialize(raftNode.getNode(), dataOutputStream);
      dataOutputStream.writeInt(raftNode.getRaftId());
      dataOutputStream.writeInt(entry.getValue().size());
      for (Integer slot : entry.getValue()) {
        dataOutputStream.writeInt(slot);
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      Node node = new Node();
      NodeSerializeUtils.deserialize(node, buffer);
      RaftNode raftNode = new RaftNode(node, buffer.getInt());
      List<Integer> slots = new ArrayList<>();
      int slotSize = buffer.getInt();
      for (int j = 0; j < slotSize; j++) {
        slots.add(buffer.getInt());
      }
      newSlotOwners.put(raftNode, slots);
    }
  }
}
