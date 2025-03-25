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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TreeDeviceViewUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeviceViewUpdater.class);
  private final ConfigManager configManager;
  private Map<String, Byte> currentType;
  private TSStatus result;

  public TreeDeviceViewUpdater(final ConfigManager configManager) {
    this.configManager = configManager;
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getLatestSchemaRegionMap() {
    final PartitionManager partitionManager = configManager.getPartitionManager();
    return partitionManager
        .getAllReplicaSetsMap(TConsensusGroupType.SchemaRegion)
        .entrySet()
        .stream()
        .filter(
            entry ->
                !PathUtils.isTableModelDatabase(partitionManager.getRegionDatabase(entry.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private class TreeDeviceUpdateTaskExecutor
      extends DataNodeRegionTaskExecutor<List<TConsensusGroupId>, TDeviceViewResp> {

    protected TreeDeviceUpdateTaskExecutor(
        final ConfigManager configManager,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup) {
      super(
          configManager,
          targetRegionGroup,
          false,
          CnToDnAsyncRequestType.GET_TREE_DEVICE_VIEW_INFO,
          ((dataNodeLocation, consensusGroupIdList) -> consensusGroupIdList));
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        TDeviceViewResp response) {
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        mergeDeviceViewResp(response);
        return Collections.emptyList();
      }

      // If some regions have failed, the "maxSegmentNum" of database is still usable
      // Though the measurement num may not be correct, we still assume that the difference
      // of the measurement types is so large that the failure won't affect much
      // We still apply the regions without failure to make the update more likely to
      // succeed in large cluster with weak network
      if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = response.getStatus().getSubStatus();
        for (int i = 0; i < subStatus.size(); i++) {
          if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedRegionList.add(consensusGroupIdList.get(i));
          } else if (Objects.nonNull(response)) {
            mergeDeviceViewResp(response);
            response = null;
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    private void mergeDeviceViewResp(final TDeviceViewResp resp) {
      if (Objects.isNull(currentType)) {
        currentType = resp.getDeviewViewUpdateMap();
        return;
      }

      // The map is always nonnull in the resp
      resp.getDeviewViewUpdateMap()
          .forEach(
              (measurement, type) -> {
                if (!currentType.containsKey(measurement)) {
                  currentType.put(measurement, type);
                } else {
                  result =
                      RpcUtils.getStatus(
                          TSStatusCode.DATA_TYPE_MISMATCH,
                          String.format(
                              "Multiple types encountered when auto detecting type of measurement '%s', please check",
                              measurement));
                }
              });
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      final String errorMsg = "Failed to update device view on region {}, skip this round";
      LOGGER.warn(errorMsg, consensusGroupId);
      interruptTask();
    }
  }
}
