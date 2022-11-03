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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getAllReplicaDataNodeRegionGroupMap;
import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap;

/**
 * This class takes the responsibility of sending region related requests to DataNode for execution,
 * mainly focus on DataNode selection and retry on failure.
 *
 * @param <T>
 */
abstract class DataNodeRegionTask<T> {

  protected final ConfigNodeProcedureEnv env;
  protected final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup;
  protected final boolean executeOnAllReplicaset;

  // all the success response of dataNodes
  private final Map<Integer, List<T>> responseMap = new ConcurrentHashMap<>();

  protected DataNodeRegionTask(
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
      boolean executeOnAllReplicaset) {
    this.env = env;
    this.targetSchemaRegionGroup = targetSchemaRegionGroup;
    this.executeOnAllReplicaset = executeOnAllReplicaset;
  }

  void execute() {
    // organize region by dataNode
    Set<TDataNodeLocation> allFailedDataNodeSet = new HashSet<>();
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        executeOnAllReplicaset
            ? getAllReplicaDataNodeRegionGroupMap(targetSchemaRegionGroup)
            : getLeaderDataNodeRegionGroupMap(
                env.getConfigManager().getLoadManager().getLatestRegionLeaderMap(),
                targetSchemaRegionGroup);
    while (!dataNodeConsensusGroupIdMap.isEmpty()) {
      Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap =
          sendRegionRequest(dataNodeConsensusGroupIdMap);
      if (hasFailure()) {
        // some dataNode execution failure
        return;
      }

      if (currentFailedDataNodeMap.isEmpty()) {
        // all succeeded
        break;
      }

      // retry failed dataNode requests caused by unexpected error on other replicas on other
      // dataNodes
      currentFailedDataNodeMap.forEach(dataNodeConsensusGroupIdMap::remove);
      // remove dataNodes that successfully executed request
      allFailedDataNodeSet.removeAll(dataNodeConsensusGroupIdMap.keySet());
      dataNodeConsensusGroupIdMap =
          getAvailableDataNodeLocationForRetry(currentFailedDataNodeMap, allFailedDataNodeSet);
      if (hasFailure()) {
        // some consensus group has no available dataNode
        return;
      }
    }
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> sendRegionRequest(
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
    // send request to each dataNode
    Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeMap = new HashMap<>();
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      // process response
      Map<Integer, TSStatus> dataNodeResponseMap = sendRequest(entry.getKey(), entry.getValue());
      TSStatus currentDataNodeResponse = dataNodeResponseMap.get(entry.getKey().getDataNodeId());
      if (currentDataNodeResponse.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (currentDataNodeResponse.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          // dataNode execution error
          onExecutionFailure(entry.getKey());
          break;
        } else {
          // unexpected error, retry on other replicates on other dataNodes
          failedDataNodeMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return failedDataNodeMap;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getAvailableDataNodeLocationForRetry(
      Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeConsensusGroupIdMap,
      Set<TDataNodeLocation> allFailedDataNodeSet) {
    Map<TConsensusGroupId, Integer> leaderMap =
        env.getConfigManager().getLoadManager().getLatestRegionLeaderMap();
    Map<TDataNodeLocation, List<TConsensusGroupId>> availableDataNodeLocation = new HashMap<>();
    for (List<TConsensusGroupId> consensusGroupIdList :
        failedDataNodeConsensusGroupIdMap.values()) {
      for (TConsensusGroupId consensusGroupId : consensusGroupIdList) {
        TRegionReplicaSet regionReplicaSet = targetSchemaRegionGroup.get(consensusGroupId);
        TDataNodeLocation selectedDataNode = null;
        Integer leaderId = leaderMap.get(consensusGroupId);
        if (leaderId == null || leaderId == -1) {
          for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
            if (!allFailedDataNodeSet.contains(candidateDataNode)) {
              // since leader of this group is unknown, take the first available one
              selectedDataNode = candidateDataNode;
              break;
            }
          }
        } else {
          for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
            if (!allFailedDataNodeSet.contains(candidateDataNode)) {
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
        }

        if (selectedDataNode == null) {
          onAllReplicasetFailure(consensusGroupId);
          return availableDataNodeLocation;
        } else {
          availableDataNodeLocation
              .computeIfAbsent(selectedDataNode, k -> new ArrayList<>())
              .add(consensusGroupId);
        }
      }
    }
    return availableDataNodeLocation;
  }

  protected void saveDataNodeResponse(Integer dataNodeId, T response) {
    responseMap.computeIfAbsent(dataNodeId, k -> new ArrayList<>()).add(response);
  }

  Map<Integer, List<T>> getResponseMap() {
    return responseMap;
  }

  /**
   * This method should be implemented as constructing and send custom DataNode requests and collect
   * response of each DataNode.
   *
   * @param dataNodeLocation the location of target DataNode
   * @param consensusGroupIdList the target region group to execute request
   * @return the execution result of each DataNode
   */
  protected abstract Map<Integer, TSStatus> sendRequest(
      TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList);

  protected abstract boolean hasFailure();

  protected abstract void onExecutionFailure(TDataNodeLocation dataNodeLocation);

  protected abstract void onAllReplicasetFailure(TConsensusGroupId consensusGroupId);
}
