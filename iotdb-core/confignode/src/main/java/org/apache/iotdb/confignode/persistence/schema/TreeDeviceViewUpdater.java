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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.AbstractPeriodicalServiceWithAdvance;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionViewInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TreeDeviceViewUpdater extends AbstractPeriodicalServiceWithAdvance {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeviceViewUpdater.class);
  private final TreeDeviceUpdateTaskExecutor executor;
  private TDeviceViewResp currentResp;

  public TreeDeviceViewUpdater(final ConfigManager configManager) {
    super(
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.TREE_DEVICE_VIEW_UPDATER.getName()),
        ConfigNodeDescriptor.getInstance().getConf().getTreeDeviceViewUpdateIntervalInMs());
    this.executor = new TreeDeviceUpdateTaskExecutor(configManager, Collections.emptyMap());
  }

  @Override
  protected void executeTask() {
    executor.execute();
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
      if (Objects.isNull(currentResp)) {
        currentResp = resp;
        return;
      }

      // The map is always nonnull in the resp
      resp.getDeviewViewUpdateMap()
          .forEach(
              (db, info) -> {
                final Map<String, TSchemaRegionViewInfo> currentUpdateMap =
                    currentResp.getDeviewViewUpdateMap();
                if (!currentUpdateMap.containsKey(db)) {
                  currentUpdateMap.put(db, info);
                  return;
                }
                final TSchemaRegionViewInfo currentInfo = currentUpdateMap.get(db);
                currentInfo.setMaxLength(Math.max(currentInfo.getMaxLength(), info.getMaxLength()));

                final Map<String, Map<Byte, Integer>> currentMeasurementTypeCountMap =
                    currentInfo.getMeasurementsDataTypeCountMap();
                info.getMeasurementsDataTypeCountMap()
                    .forEach(
                        (measurement, typeCountMap) -> {
                          if (!currentMeasurementTypeCountMap.containsKey(measurement)) {
                            currentMeasurementTypeCountMap.put(measurement, typeCountMap);
                            return;
                          }

                          final Map<Byte, Integer> currentTypeCountMap =
                              currentMeasurementTypeCountMap.get(measurement);
                          typeCountMap.forEach(
                              (type, count) ->
                                  currentTypeCountMap.put(
                                      type,
                                      currentTypeCountMap.containsKey(type)
                                          ? currentTypeCountMap.get(type) + count
                                          : count));
                        });
              });
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      LOGGER.warn("Failed to update device view on region {}, skip this round", consensusGroupId);
      interruptTask();
    }
  }
}
