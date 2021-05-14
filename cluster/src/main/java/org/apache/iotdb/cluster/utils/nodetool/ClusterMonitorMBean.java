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

import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.List;
import java.util.Map;

public interface ClusterMonitorMBean {

  /**
   * Get physical hash ring
   *
   * @return Node list
   */
  List<Node> getRing();

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
   * Get data partition groups that input node belongs to and the slot number in each partition
   * group.
   *
   * @return key: the partition group, value: the slot number
   */
  Map<PartitionGroup, Integer> getSlotNumOfCurNode();

  /**
   * Get all data partition groups and the slot number in each partition group.
   *
   * @return key: the partition group, value: the slot number
   */
  Map<PartitionGroup, Integer> getSlotNumOfAllNode();

  /**
   * Get status of all nodes
   *
   * @return key: node, value: live or not
   */
  Map<Node, Boolean> getAllNodeStatus();

  /**
   * @return A multi-line string with each line representing the total time consumption, invocation
   *     number, and average time consumption.
   */
  String getInstrumentingInfo();

  /** Reset all instrumenting statistics in Timer. */
  void resetInstrumenting();
}
