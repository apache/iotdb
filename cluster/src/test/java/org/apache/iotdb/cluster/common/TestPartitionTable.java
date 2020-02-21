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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.metadata.MManager;

public class TestPartitionTable implements PartitionTable {

  @Override
  public PartitionGroup route(String storageGroupName, long timestamp) {
    return null;
  }

  @Override
  public int getPartitionKey(String storageGroupName, long timestamp) {
    //TODO
    return 0;
  }

  @Override
  public PartitionGroup route(int hashkey) {
    //TODO
    return null;
  }

  @Override
  public Node routeToHeader(String storageGroupName, long timestamp) {
    return null;
  }

  @Override
  public PartitionGroup addNode(Node node) {
    return null;
  }

  @Override
  public NodeRemovalResult removeNode(Node node) {
    return null;
  }

  @Override
  public List<PartitionGroup> getLocalGroups() {
    return null;
  }

  @Override
  public PartitionGroup getHeaderGroup(Node header) {
    return null;
  }

  @Override
  public ByteBuffer serialize() {
    return null;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

  }

  @Override
  public List<Node> getAllNodes() {
    return null;
  }

  @Override
  public Map<Integer, Node> getPreviousNodeMap(Node node) {
    return null;
  }

  @Override
  public List<Integer> getNodeSlots(Node header) {
    return null;
  }

  @Override
  public Map<Node, List<Integer>> getAllNodeSlots() {
    return null;
  }

  @Override
  public int getTotalSlotNumbers() {
    return 100;
  }

  @Override
  public MManager getMManager() {
    return MManager.getInstance();
  }
}
