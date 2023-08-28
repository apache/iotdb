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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;

public class PipeLeaderChangeHandler implements IClusterStatusSubscriber {

  private final ConfigManager configManager;

  PipeLeaderChangeHandler(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    // do nothing, because pipe task is not related to statistics
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    // if no pipe task, return
    if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()) {
      return;
    }

    // we only care about data region leader change
    final Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap =
        new HashMap<>();
    event
        .getLeaderMap()
        .forEach(
            (regionGroupId, pair) -> {
              if (regionGroupId.getType().equals(TConsensusGroupType.DataRegion)) {
                final String databaseName =
                    configManager.getPartitionManager().getRegionStorageGroup(regionGroupId);
                // pipe only collect user's data, filter metric database here.
                if (databaseName != null && !databaseName.equals(SchemaConstant.SYSTEM_DATABASE)) {
                  // null or -1 means empty origin leader
                  final int oldLeaderDataNodeId = (pair.left == null ? -1 : pair.left);
                  final int newLeaderDataNodeId = (pair.right == null ? -1 : pair.right);

                  if (oldLeaderDataNodeId != newLeaderDataNodeId) {
                    dataRegionGroupToOldAndNewLeaderPairMap.put(
                        regionGroupId, new Pair<>(oldLeaderDataNodeId, newLeaderDataNodeId));
                  }
                }
              }
            });

    // if no data region leader change, return
    if (dataRegionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return;
    }

    // submit procedure in an async way to avoid blocking the caller
    configManager
        .getPipeManager()
        .getPipeRuntimeCoordinator()
        .getProcedureSubmitter()
        .submit(
            () ->
                configManager
                    .getProcedureManager()
                    .pipeHandleLeaderChange(dataRegionGroupToOldAndNewLeaderPairMap));
  }
}
