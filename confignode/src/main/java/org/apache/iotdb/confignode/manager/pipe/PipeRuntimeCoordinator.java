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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRuntimeCoordinator implements IClusterStatusSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);

  private final ConfigManager configManager;

  public PipeRuntimeCoordinator(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    // Do nothing
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    event
        .getLeaderMap()
        .keySet()
        .removeIf(regionId -> !regionId.getType().equals(TConsensusGroupType.DataRegion));
    if (event.getLeaderMap().isEmpty()) {
      return;
    }
    TSStatus result = configManager.getProcedureManager().handleLeaderChange(event.getLeaderMap());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "PipeRuntimeCoordinator meets error in handle leader change, status: ({})", result);
    }
  }
}
