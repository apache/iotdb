/**
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;

public interface ClusterMonitorMBean {

  /**
   * Get physical hash ring
   *
   * @return key: hash value, value: node ip
   */
  List<Node> getRing();

  /**
   * Get data partition information of input storage group in String format. The node ips are split
   * by ',', and the first ip is the currnt leader.
   *
   * @param sg input storage group path
   * @return data partition information in String format
   */
  String getDataPartitionOfSG(String sg);

  /**
   * Get all storage groups
   *
   * @return Set of all storage groups
   */
  Set<String> getAllStorageGroupsLocally();

  /**
   * Get data partitions that input node belongs to.
   *
   * @return key: node ips of one data partition, value: storage group paths that belong to this
   * data partition
   */
  Map<PartitionGroup, Integer> getSlotNumOfCurNode();

  Map<PartitionGroup, Integer> getSlotNumOfAllNode();

  /**
   * Get status of all nodes
   *
   * @return key: node ip, value: live or not
   */
  Map<String, Boolean> getStatusMap();
}
