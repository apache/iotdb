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
package org.apache.iotdb.cluster.utils.nodetool;

import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.List;
import java.util.Map;

public interface ClusterMonitorMBean {

  /** Show the character of meta raft group. */
  List<Pair<Node, NodeCharacter>> getMetaGroup();

  /** Show the character of target data raft group whose header is this node. */
  List<Pair<Node, NodeCharacter>> getDataGroup(int raftId) throws Exception;

  /**
   * Query how many slots are still PULLING or PULLING_WRITABLE, it means whether user can
   * add/remove a node.
   *
   * @return key: group, value: slot num that still in the process of data migration
   */
  Map<PartitionGroup, Integer> getSlotNumInDataMigration() throws Exception;

  /**
   * Get data partition information of input path and time range.
   *
   * @param path input path
   * @return data partition information: ((start time, end time), PartitionGroup)
   */
  MultiKeyMap<Long, PartitionGroup> getDataPartition(String path, long startTime, long endTime);

  /**
   * Get metadata partition information of input path
   *
   * @param path input path
   * @return metadata partition information
   */
  PartitionGroup getMetaPartition(String path);

  /**
   * Get all data partition groups and the slot number in each partition group.
   *
   * @return key: the partition group, value: the slot number
   */
  Map<PartitionGroup, Integer> getSlotNumOfAllNode();

  /**
   * Get status of all nodes
   *
   * @return key: node, value: 0(live), 1(offline), 2(joining), 3(leaving)
   */
  Map<Node, Integer> getAllNodeStatus();

  /**
   * @return A multi-line string with each line representing the total time consumption, invocation
   *     number, and average time consumption.
   */
  String getInstrumentingInfo();

  /** Reset all instrumenting statistics in Timer. */
  void resetInstrumenting();
}
