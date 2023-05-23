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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeCriticalException;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
              if (regionId.getType().equals(TConsensusGroupType.DataRegion)) {
                dataRegionGroupToOldAndNewLeaderPairMap.put(regionId, pair);
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

  // when the pipeMetaKeeper in ConfigNode is different from that in DataNode, update it.
  public void updatePipeMetaKeeper(List<PipeMeta> pipeMetaListFromDataNode) {
    PipeTaskInfo pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().getPipeTaskInfo();
    List<PipeMeta> pipeMetaListOnConfigNode =
        StreamSupport.stream(pipeTaskInfo.getPipeMetaList().spliterator(), false)
            .collect(Collectors.toList());
    List<PipeMeta> oldMetaListOnConfigNode = new ArrayList<>(pipeMetaListOnConfigNode);

    Map<String, PipeMeta> metaMapOnConfigNode =
        pipeMetaListOnConfigNode.stream()
            .collect(Collectors.toMap(p -> p.getStaticMeta().getPipeName(), Function.identity()));

    boolean needPushPipeMetaToDataNodes = false;
    boolean needWriteConsensusOnConfigNodes = false;

    for (PipeMeta metaOnDataNode : pipeMetaListFromDataNode) {
      String pipeNameOnDataNode = metaOnDataNode.getStaticMeta().getPipeName();
      PipeMeta metaOnConfigNode = metaMapOnConfigNode.get(pipeNameOnDataNode);

      if (metaOnConfigNode == null) {
        LOGGER.warn(
            "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                + "because pipeMetaKeeper on ConfigNode doesn't contain pipe {}",
            pipeNameOnDataNode);
        return;
      }
      Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapOnDataNode =
          metaOnDataNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();
      Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapOnConfigNode =
          metaOnConfigNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();

      for (Map.Entry<TConsensusGroupId, PipeTaskMeta> entry :
          consensusGroupIdToTaskMetaMapOnDataNode.entrySet()) {
        TConsensusGroupId consensusGroupId = entry.getKey();
        PipeTaskMeta taskMetaOnDataNode = entry.getValue();
        PipeTaskMeta taskMetaOnConfigNode =
            consensusGroupIdToTaskMetaMapOnConfigNode.get(consensusGroupId);

        if (taskMetaOnConfigNode == null) {
          LOGGER.warn(
              "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                  + "because pipeMetaKeeper on ConfigNode doesn't contain consensus group {}",
              consensusGroupId);
          return;
        }

        // 1. compare progress index, and set progress index if datanode's is bigger
        if (taskMetaOnDataNode.getRegionLeader() == taskMetaOnConfigNode.getRegionLeader()) {
          if (taskMetaOnDataNode.getProgressIndex() > taskMetaOnConfigNode.getProgressIndex()) {
            taskMetaOnConfigNode.setProgressIndex(taskMetaOnDataNode.getProgressIndex());
            needWriteConsensusOnConfigNodes = true;
          }
        }

        Set<PipeRuntimeException> exceptionMessagesOnDataNode = new HashSet<>();
        taskMetaOnDataNode.getExceptionMessages().forEach(exceptionMessagesOnDataNode::add);

        Set<PipeRuntimeException> exceptionMessagesOnConfigNode = new HashSet<>();
        taskMetaOnConfigNode.getExceptionMessages().forEach(exceptionMessagesOnConfigNode::add);

        if (!exceptionMessagesOnDataNode.equals(exceptionMessagesOnConfigNode)) {
          taskMetaOnConfigNode.setExceptionMessages(exceptionMessagesOnDataNode);
          needWriteConsensusOnConfigNodes = true;
          // If there are critical exceptions, set status to STOPPED
          if (exceptionMessagesOnDataNode.stream()
              .anyMatch(exception -> exception instanceof PipeRuntimeCriticalException)) {
            metaOnDataNode.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
            needPushPipeMetaToDataNodes = true;
          }
        }
      }
    }

    // if exceptionMessages has changed, write consensus and push PipeMetaList to datanodes.
    // And if only progress index has changed, write consensus.
    if (needPushPipeMetaToDataNodes || needWriteConsensusOnConfigNodes) {
      final TSStatus result =
          configManager
              .getProcedureManager()
              .pipeHandleMetaChange(
                  (List<PipeMeta>) pipeTaskInfo.getPipeMetaList(),
                  oldMetaListOnConfigNode,
                  needPushPipeMetaToDataNodes);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "PipeTaskCoordinator meets error in handling pipe meta keeper change, status: ({})",
            result);
      }
    }
  }

  public void startPipeMetaSync() {
    pipeMetaSyncer.start();
  }

  public void stopPipeMetaSync() {
    pipeMetaSyncer.stop();
  }
}
