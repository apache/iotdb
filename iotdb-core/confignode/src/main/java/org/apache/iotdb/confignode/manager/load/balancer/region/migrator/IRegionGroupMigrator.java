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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import java.util.List;
import java.util.Map;

public interface IRegionGroupMigrator {

  /**
   * Auto balance the RegionReplicas' distribution for cluster RegionGroups.
   *
   * @param availableDataNodeMap DataNodes that can be used for allocation
   * @param regionGroupStatisticsMap Statistics of RegionGroups
   * @param allocatedRegionGroups Allocated RegionGroups
   * @param replicationFactor Replication factor of TRegionReplicaSet
   * @param targetNodeIds Optional target node IDs. If null or empty, automatically select the node
   *     with lowest disk usage
   * @return The optimal TRegionReplicaSet derived by the specified algorithm
   */
  Map<TConsensusGroupId, TRegionReplicaSet> autoBalanceRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor,
      List<Integer> targetNodeIds);
}
