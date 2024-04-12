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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;

public class PipeLeaderChangeHandler implements IClusterStatusSubscriber {

  private final ConfigManager configManager;

  PipeLeaderChangeHandler(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public void onConfigRegionGroupLeaderChanged() {
    Map<TConsensusGroupId, Pair<ConsensusStatistics, ConsensusStatistics>> virtualChangeMap =
        new HashMap<>();
    // The old leader id can be arbitrarily assigned because pipe task do not need this
    virtualChangeMap.put(
        new TConsensusGroupId(TConsensusGroupType.ConfigRegion, Integer.MIN_VALUE),
        new Pair<>(
            new ConsensusStatistics(Long.MIN_VALUE, Integer.MIN_VALUE),
            new ConsensusStatistics(
                System.nanoTime(),
                ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId())));

    onConsensusStatisticsChanged(new ConsensusStatisticsChangeEvent(virtualChangeMap));
  }

  @Override
  public void onConsensusStatisticsChanged(ConsensusStatisticsChangeEvent event) {
    // If no pipe tasks, return
    if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()) {
      return;
    }

    final Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap =
        new HashMap<>();
    event
        .getDifferentConsensusStatisticsMap()
        .forEach(
            (regionGroupId, pair) -> {
              final String databaseName =
                  configManager.getPartitionManager().getRegionStorageGroup(regionGroupId);
              // Pipe only collect user's data, filter metric database here.
              // DatabaseName may be null for config region group
              if (!SchemaConstant.SYSTEM_DATABASE.equals(databaseName)) {
                // null or -1 means empty origin leader
                final int oldLeaderNodeId = (pair.left == null ? -1 : pair.left.getLeaderId());
                final int newLeaderNodeId = (pair.right == null ? -1 : pair.right.getLeaderId());

                if (oldLeaderNodeId != newLeaderNodeId) {
                  regionGroupToOldAndNewLeaderPairMap.put(
                      regionGroupId, new Pair<>(oldLeaderNodeId, newLeaderNodeId));
                }
              }
            });

    // If no region leaders change, return
    if (regionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return;
    }

    // Synchronized to ensure that the newest leader is put into the procedure at last
    configManager.getProcedureManager().pipeHandleLeaderChange(regionGroupToOldAndNewLeaderPairMap);
  }
}
