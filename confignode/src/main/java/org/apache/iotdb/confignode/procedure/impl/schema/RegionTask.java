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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getAllReplicaDataNodeRegionGroupMap;
import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap;

/**
 * This class provides the common logic execution of selecting DataNode, executing request on
 * DataNode and retry on other DataNodes on failure.
 *
 * @param <T> The response of RegionTask
 */
abstract class RegionTask<T> {

  protected final String taskName;
  protected final ConfigNodeProcedureEnv env;
  protected final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup;

  private boolean executeOnAllReplicaset = false;

  protected Map<Integer, T> responseMap = new ConcurrentHashMap<>();

  RegionTask(
      String taskName,
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
    this.taskName = taskName;
    this.env = env;
    this.targetSchemaRegionGroup = targetSchemaRegionGroup;
  }

  void execute() {
    // organize schema region by dataNode
    Set<TDataNodeLocation> allFailedDataNodeSet = new HashSet<>();
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        executeOnAllReplicaset
            ? getAllReplicaDataNodeRegionGroupMap(targetSchemaRegionGroup)
            : getLeaderDataNodeRegionGroupMap(
                env.getConfigManager().getPartitionManager().getAllLeadership(),
                targetSchemaRegionGroup);
    while (!dataNodeConsensusGroupIdMap.isEmpty()) {
      Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap =
          sendRegionRequest(dataNodeConsensusGroupIdMap);
      if (isProcedureFailed()) {
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
      if (isProcedureFailed()) {
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
          setProcedureFailureOnDataNodeExecutionFailure(entry.getKey());
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
        env.getConfigManager().getPartitionManager().getAllLeadership();
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
          setProcedureFailureOnAllReplicasetFailure(consensusGroupId);
          return availableDataNodeLocation;
        } else {
          availableDataNodeLocation
              .compute(selectedDataNode, (k, v) -> new ArrayList<>())
              .add(consensusGroupId);
        }
      }
    }
    return availableDataNodeLocation;
  }

  Map<Integer, T> getResponseMap() {
    return responseMap == null ? Collections.emptyMap() : responseMap;
  }

  final void setExecuteOnAllReplicaset(boolean executeOnAllReplicaset) {
    this.executeOnAllReplicaset = executeOnAllReplicaset;
  }

  abstract Map<Integer, TSStatus> sendRequest(
      TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList);

  abstract boolean isProcedureFailed();

  abstract void setProcedureFailureOnDataNodeExecutionFailure(TDataNodeLocation failedDataNode);

  abstract void setProcedureFailureOnAllReplicasetFailure(TConsensusGroupId failedRegionGroup);
}
