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

package org.apache.iotdb.cluster.partition;

import java.util.List;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * PartitionTable manages the map whose key is the StorageGroupName with a time interval and the
 * value is a PartitionGroup with contains all nodes that manage the corresponding data.
 */
public interface PartitionTable {

  /**
   * Given the storageGroupName and the timestamp, return the list of nodes on which the storage
   * group and the corresponding time interval is managed.
   * @param storageGroupName
   * @param timestamp
   * @return
   */
  PartitionGroup route(String storageGroupName, long timestamp);

  /**
   * Add a new node to update the partition table.
   * @param node
   */
  void addNode(Node node);

  /**
   * Get the partition groups that node belongs to. Each group corresponds to a data raft group.
   * @param node
   * @return
   */
  List<PartitionGroup> getSubordinateGroups(Node node);
}
