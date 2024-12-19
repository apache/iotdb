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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.service.AbstractPeriodicalServiceWithAdvance;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class TreeDeviceViewUpdater extends AbstractPeriodicalServiceWithAdvance {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeviceViewUpdater.class);
  private final Map<String, TsTable> newViewTables = new ConcurrentHashMap<>();

  public TreeDeviceViewUpdater() {
    super(
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.TREE_DEVICE_VIEW_UPDATER.getName()),
        ConfigNodeDescriptor.getInstance().getConf().getTreeDeviceViewUpdateIntervalInMs());
  }

  @Override
  protected void executeTask() {}

  public Map<String, TsTable> getNewViewTables() {
    return newViewTables;
  }

  protected class TreeDeviceUpdateTaskExecutor<Q>
      extends DataNodeRegionTaskExecutor<Q, TDeviceViewResp> {

    protected TreeDeviceUpdateTaskExecutor(
        final ConfigManager configManager,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(
          configManager,
          targetRegionGroup,
          false,
          CnToDnAsyncRequestType.GET_TREE_DEVICE_VIEW_INFO,
          dataNodeRequestGenerator);
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        final TDeviceViewResp response) {
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = response.getStatus().getSubStatus();
        for (int i = 0; i < subStatus.size(); i++) {
          if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedRegionList.add(consensusGroupIdList.get(i));
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
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
