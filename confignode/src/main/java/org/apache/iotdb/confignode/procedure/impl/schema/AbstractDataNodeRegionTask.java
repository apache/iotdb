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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getAllReplicaDataNodeRegionGroupMap;
import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap;

public abstract class AbstractDataNodeRegionTask<Q, R> {

  protected final ConfigNodeProcedureEnv env;
  protected final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup;
  protected final boolean executeOnAllReplicaset;

  protected AbstractDataNodeRegionTask(
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
      AsyncClientHandler<Q, R> clientHandler = prepareRequestHandler(dataNodeConsensusGroupIdMap);
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
      Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap =
          checkOutDataNodeExecutionResult(
              clientHandler.getResponseMap(), dataNodeConsensusGroupIdMap);

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

  private AsyncClientHandler<Q, R> prepareRequestHandler(
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
    AsyncClientHandler<Q, R> clientHandler = new AsyncClientHandler<>(getDataNodeRequestType());
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      clientHandler.putDataNodeLocation(entry.getKey().getDataNodeId(), entry.getKey());
      clientHandler.putRequest(
          entry.getKey().getDataNodeId(), prepareRequestForOneDataNode(entry.getValue()));
    }
    return clientHandler;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> checkOutDataNodeExecutionResult(
      Map<Integer, R> executionResult,
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap = new HashMap<>();
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      R response = executionResult.get(entry.getKey().getDataNodeId());
      List<TConsensusGroupId> failedRegionList = processResponseOfOneDataNode(response);
      if (failedRegionList.isEmpty()) {
        continue;
      }
      currentFailedDataNodeMap.put(entry.getKey(), failedRegionList);
    }
    return currentFailedDataNodeMap;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getAvailableDataNodeLocationForRetry(
      Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeConsensusGroupIdMap,
      Set<TDataNodeLocation> allFailedDataNodeSet) {
    return null;
  }

  protected abstract DataNodeRequestType getDataNodeRequestType();

  protected abstract Q prepareRequestForOneDataNode(List<TConsensusGroupId> consensusGroupIdList);

  protected abstract List<TConsensusGroupId> processResponseOfOneDataNode(R response);

  protected abstract boolean hasFailure();
}
