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

package org.apache.iotdb.db.trigger.service;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TriggerInformationUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerInformationUpdater.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private final ScheduledExecutorService triggerInformationUpdateExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          "Stateful-Trigger-Information-Updater");

  private Future<?> updateFuture;

  private static final long UPDATE_INTERVAL = 1000L * 60;

  public void startTriggerInformationUpdater() {
    if (updateFuture == null) {
      updateFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              triggerInformationUpdateExecutor,
              this::updateTask,
              UPDATE_INTERVAL,
              UPDATE_INTERVAL,
              TimeUnit.MILLISECONDS);
      LOGGER.info("Stateful-Trigger-Information-Updater is successfully started.");
    }
  }

  public void stopTriggerInformationUpdater() {
    if (updateFuture != null) {
      updateFuture.cancel(false);
      updateFuture = null;
      LOGGER.info("Stateful-Trigger-Information-Updater is successfully stopped.");
    }
  }

  public void updateTask() {
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetTriggerTableResp getStatefulTriggerTableResp = client.getStatefulTriggerTable();
      if (getStatefulTriggerTableResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            getStatefulTriggerTableResp.getStatus().getMessage(),
            getStatefulTriggerTableResp.getStatus().getCode());
      }
      List<TriggerInformation> statefulTriggerInformationList =
          getStatefulTriggerTableResp.getAllTriggerInformation().stream()
              .map(TriggerInformation::deserialize)
              .collect(Collectors.toList());
      for (TriggerInformation triggerInformation : statefulTriggerInformationList) {
        TriggerManagementService.getInstance()
            .updateLocationOfStatefulTrigger(
                triggerInformation.getTriggerName(), triggerInformation.getDataNodeLocation());
      }
    } catch (Exception e) {
      LOGGER.warn(String.format("Meet error when updating trigger information: %s", e));
    }
  }
}
