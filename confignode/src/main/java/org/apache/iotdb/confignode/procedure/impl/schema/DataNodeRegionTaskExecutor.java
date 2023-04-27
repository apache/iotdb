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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getAllReplicaDataNodeRegionGroupMap;
import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap;

public abstract class DataNodeRegionTaskExecutor<Q, R> {

  protected final ConfigNodeProcedureEnv env;
  protected final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup;
  protected final boolean executeOnAllReplicaset;

  protected final DataNodeRequestType dataNodeRequestType;
  protected final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q>
      dataNodeRequestGenerator;

  private boolean isInterrupted = false;

  protected DataNodeRegionTaskExecutor(
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
      boolean executeOnAllReplicaset,
      DataNodeRequestType dataNodeRequestType,
      BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
    this.env = env;
    this.targetSchemaRegionGroup = targetSchemaRegionGroup;
    this.executeOnAllReplicaset = executeOnAllReplicaset;
    this.dataNodeRequestType = dataNodeRequestType;
    this.dataNodeRequestGenerator = dataNodeRequestGenerator;
  }

  void execute() {
    // organize region by dataNode
    Map<TConsensusGroupId, Set<TDataNodeLocation>> failedHistory = new HashMap<>();
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        executeOnAllReplicaset
            ? getAllReplicaDataNodeRegionGroupMap(targetSchemaRegionGroup)
            : getLeaderDataNodeRegionGroupMap(
                env.getConfigManager().getLoadManager().getLatestRegionLeaderMap(),
                targetSchemaRegionGroup);
    while (!dataNodeConsensusGroupIdMap.isEmpty()) {
      AsyncClientHandler<Q, R> clientHandler = prepareRequestHandler(dataNodeConsensusGroupIdMap);
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
      Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap =
          checkDataNodeExecutionResult(clientHandler.getResponseMap(), dataNodeConsensusGroupIdMap);

      if (isInterrupted) {
        // some dataNode execution failure
        return;
      }

      if (currentFailedDataNodeMap.isEmpty()) {
        // all succeeded
        break;
      }

      // retry failed dataNode requests caused by unexpected error on other replicas on other
      // dataNodes
      dataNodeConsensusGroupIdMap =
          getAvailableDataNodeLocationForRetry(currentFailedDataNodeMap, failedHistory);
      if (isInterrupted) {
        // some consensus group has no available dataNode
        return;
      }
    }
  }

  private AsyncClientHandler<Q, R> prepareRequestHandler(
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
    AsyncClientHandler<Q, R> clientHandler = new AsyncClientHandler<>(dataNodeRequestType);
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      clientHandler.putDataNodeLocation(entry.getKey().getDataNodeId(), entry.getKey());
      clientHandler.putRequest(
          entry.getKey().getDataNodeId(),
          dataNodeRequestGenerator.apply(entry.getKey(), entry.getValue()));
    }
    return clientHandler;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> checkDataNodeExecutionResult(
      Map<Integer, R> executionResult,
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap = new HashMap<>();
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      R response = executionResult.get(entry.getKey().getDataNodeId());
      List<TConsensusGroupId> failedRegionList =
          processResponseOfOneDataNode(entry.getKey(), entry.getValue(), response);
      if (failedRegionList.isEmpty()) {
        continue;
      }
      currentFailedDataNodeMap.put(entry.getKey(), failedRegionList);
    }
    return currentFailedDataNodeMap;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getAvailableDataNodeLocationForRetry(
      Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeConsensusGroupIdMap,
      Map<TConsensusGroupId, Set<TDataNodeLocation>> failedHistory) {

    failedDataNodeConsensusGroupIdMap.forEach(
        (k, v) -> {
          for (TConsensusGroupId consensusGroupId : v) {
            failedHistory.computeIfAbsent(consensusGroupId, o -> new HashSet<>()).add(k);
          }
        });

    Map<TDataNodeLocation, List<TConsensusGroupId>> availableDataNodeLocation = new HashMap<>();

    Map<TConsensusGroupId, Integer> leaderMap =
        env.getConfigManager().getLoadManager().getLatestRegionLeaderMap();
    for (List<TConsensusGroupId> consensusGroupIdList :
        failedDataNodeConsensusGroupIdMap.values()) {
      for (TConsensusGroupId consensusGroupId : consensusGroupIdList) {
        TRegionReplicaSet regionReplicaSet = targetSchemaRegionGroup.get(consensusGroupId);
        TDataNodeLocation selectedDataNode = null;
        Integer leaderId = leaderMap.get(consensusGroupId);
        Set<TDataNodeLocation> failedDataNodeSet;
        if (leaderId == null || leaderId == -1) {
          for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
            if ((failedDataNodeSet = failedHistory.get(consensusGroupId)) != null
                && failedDataNodeSet.contains(candidateDataNode)) {
              continue;
            }
            // since leader of this group is unknown, take the first available one
            selectedDataNode = candidateDataNode;
            break;
          }
        } else {
          for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
            if ((failedDataNodeSet = failedHistory.get(consensusGroupId)) != null
                && failedDataNodeSet.contains(candidateDataNode)) {
              continue;
            }
            if (leaderId == candidateDataNode.getDataNodeId()) {
              // retry on the new leader as possible
              selectedDataNode = candidateDataNode;
              break;
            }
            if (selectedDataNode == null) {
              selectedDataNode = candidateDataNode;
            }
          }
        }

        if (selectedDataNode == null) {
          onAllReplicasetFailure(consensusGroupId, failedHistory.get(consensusGroupId));
        } else {
          availableDataNodeLocation
              .computeIfAbsent(selectedDataNode, k -> new ArrayList<>())
              .add(consensusGroupId);
        }
      }
    }
    return availableDataNodeLocation;
  }

  protected final void interruptTask() {
    this.isInterrupted = true;
  }

  /**
   * The subclass could process response of given DataNode and should return the group id of region
   * with execution failure.
   */
  protected abstract List<TConsensusGroupId> processResponseOfOneDataNode(
      TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList, R response);

  /**
   * When all replicas failed on executing given task, the process defined by subclass will be
   * executed.
   */
  protected abstract void onAllReplicasetFailure(
      TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet);
}
