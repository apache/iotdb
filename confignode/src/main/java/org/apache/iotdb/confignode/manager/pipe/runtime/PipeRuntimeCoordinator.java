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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.metrics.utils.IoTDBMetricsUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeRuntimeCoordinator implements IClusterStatusSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeCoordinator.class);

  private final ConfigManager configManager;

  private final PipeMetaSyncer pipeMetaSyncer;

  public PipeRuntimeCoordinator(ConfigManager configManager) {
    this.configManager = configManager;
    this.pipeMetaSyncer = new PipeMetaSyncer(configManager);
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    // do nothing, because pipe task is not related to statistics
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    // if no pipe task, return
    if (configManager.getPipeManager().getPipeTaskCoordinator().getPipeTaskInfo().isEmpty()) {
      return;
    }

    // we only care about data region leader change
    final Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap =
        new HashMap<>();
    event
        .getLeaderMap()
        .forEach(
            (regionId, pair) -> {
              if (regionId.getType().equals(TConsensusGroupType.DataRegion)
                  && !configManager
                      .getPartitionManager()
                      .getRegionStorageGroup(regionId)
                      .equals(IoTDBMetricsUtils.DATABASE)) {
                // pipe only collect user's data, filter metric database here.
                dataRegionGroupToOldAndNewLeaderPairMap.put(
                    regionId,
                    new Pair<>( // null or -1 means empty origin leader
                        pair.left == null ? -1 : pair.left, pair.right == null ? -1 : pair.right));
              }
            });

    // if no data region leader change, return
    if (dataRegionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return;
    }

    final TSStatus result =
        configManager
            .getProcedureManager()
            .pipeHandleLeaderChange(dataRegionGroupToOldAndNewLeaderPairMap);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "PipeRuntimeCoordinator meets error in handling data region leader change, status: ({})",
          result);
    }
  }

  public void startPipeMetaSync() {
    pipeMetaSyncer.start();
  }

  public void stopPipeMetaSync() {
    pipeMetaSyncer.stop();
  }

  /**
   * parse heartbeat from data node.
   *
   * @param dataNodeId data node id
   * @param pipeMetaByteBufferListFromDataNode pipe meta byte buffer list collected from data node
   */
  public void parseHeartbeat(
      int dataNodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromDataNode) {
    final TSStatus result =
        configManager
            .getProcedureManager()
            .pipeHandleMetaChange(dataNodeId, pipeMetaByteBufferListFromDataNode);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "PipeTaskCoordinator meets error in handling pipe meta change, status: ({})", result);
    }
  }
}
