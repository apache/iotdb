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
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class DataNodeTSStatusTaskExecutor<Q>
    extends DataNodeRegionTaskExecutor<Q, TSStatus> {
  protected List<TSStatus> successResult = new ArrayList<>();

  protected DataNodeTSStatusTaskExecutor(
      final ConfigNodeProcedureEnv env,
      final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
      final boolean executeOnAllReplicaset,
      final CnToDnAsyncRequestType dataNodeRequestType,
      final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
    super(
        env,
        targetRegionGroup,
        executeOnAllReplicaset,
        dataNodeRequestType,
        dataNodeRequestGenerator);
  }

  @Override
  protected List<TConsensusGroupId> processResponseOfOneDataNode(
      final TDataNodeLocation dataNodeLocation,
      final List<TConsensusGroupId> consensusGroupIdList,
      final TSStatus response) {
    final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
    if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      failureMap.remove(dataNodeLocation);
      return failedRegionList;
    }

    if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      final List<TSStatus> subStatus = response.getSubStatus();
      for (int i = 0; i < subStatus.size(); i++) {
        if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failedRegionList.add(consensusGroupIdList.get(i));
        }
      }
    } else {
      failedRegionList.addAll(consensusGroupIdList);
    }
    if (!failedRegionList.isEmpty()) {
      failureMap.put(dataNodeLocation, RpcUtils.extractFailureStatues(response));
    } else {
      failureMap.remove(dataNodeLocation);
    }
    return failedRegionList;
  }

  protected List<TConsensusGroupId> processResponseOfOneDataNodeWithSuccessResult(
      final TDataNodeLocation dataNodeLocation,
      final List<TConsensusGroupId> consensusGroupIdList,
      final TSStatus response) {
    final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
    if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      successResult.add(response);
    } else if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      List<TSStatus> subStatusList = response.getSubStatus();
      for (int i = 0; i < subStatusList.size(); i++) {
        if (subStatusList.get(i).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          successResult.add(subStatusList.get(i));
        } else {
          failedRegionList.add(consensusGroupIdList.get(i));
        }
      }
    } else {
      failedRegionList.addAll(consensusGroupIdList);
    }
    if (!failedRegionList.isEmpty()) {
      failureMap.put(dataNodeLocation, RpcUtils.extractFailureStatues(response));
    } else {
      failureMap.remove(dataNodeLocation);
    }
    return failedRegionList;
  }

  public List<TSStatus> getSuccessResult() {
    return successResult;
  }
}
