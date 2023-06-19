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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The RetryFailedTasksThread executed periodically to retry failed tasks in Trigger, Template and
 * CQ
 */
public class RetryFailedTasksThread {

  // TODO: Replace this class by cluster events

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryFailedTasksThread.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();
  private final IManager configManager;
  private final NodeManager nodeManager;
  private final LoadManager loadManager;
  private final ScheduledExecutorService retryFailTasksExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_HEART_BEAT_SERVICE.getName());
  private final Object scheduleMonitor = new Object();
  private Future<?> currentFailedTasksRetryThreadFuture;

  /** Trigger */
  private final Set<TDataNodeLocation> oldUnknownNodes;

  public RetryFailedTasksThread(IManager configManager) {
    this.configManager = configManager;
    this.nodeManager = configManager.getNodeManager();
    this.loadManager = configManager.getLoadManager();
    this.oldUnknownNodes = new HashSet<>();
  }

  public void startRetryFailedTasksService() {
    synchronized (scheduleMonitor) {
      if (currentFailedTasksRetryThreadFuture == null) {
        currentFailedTasksRetryThreadFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                retryFailTasksExecutor,
                this::retryFailedTasks,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("RetryFailMissions service is started successfully.");
      }
    }
  }

  /** Stop the retry fail missions service */
  public void stopRetryFailedTasksService() {
    synchronized (scheduleMonitor) {
      if (currentFailedTasksRetryThreadFuture != null) {
        currentFailedTasksRetryThreadFuture.cancel(false);
        currentFailedTasksRetryThreadFuture = null;
        LOGGER.info("RetryFailMissions service is stopped successfully.");
      }
    }
  }

  private void retryFailedTasks() {
    // trigger
    triggerDetectTask();
  }

  /**
   * The triggerDetectTask executed periodically to find newest UnknownDataNodes
   *
   * <p>1.If one DataNode is continuing Unknown, we shouldn't always activate Transfer of this Node.
   *
   * <p>2.The selected DataNodes may not truly need to transfer, so you should ensure safety of the
   * Data when implement transferMethod in Manager.
   */
  private void triggerDetectTask() {
    List<TDataNodeLocation> newUnknownNodes = new ArrayList<>();

    nodeManager
        .getRegisteredDataNodes()
        .forEach(
            DataNodeConfiguration -> {
              TDataNodeLocation dataNodeLocation = DataNodeConfiguration.getLocation();
              NodeStatus nodeStatus = loadManager.getNodeStatus(dataNodeLocation.getDataNodeId());
              if (nodeStatus == NodeStatus.Running) {
                oldUnknownNodes.remove(dataNodeLocation);
              } else if (!oldUnknownNodes.contains(dataNodeLocation)
                  && nodeStatus == NodeStatus.Unknown) {
                newUnknownNodes.add(dataNodeLocation);
              }
            });

    if (!newUnknownNodes.isEmpty()) {
      TSStatus transferResult = configManager.transfer(newUnknownNodes);
      if (transferResult.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        oldUnknownNodes.addAll(newUnknownNodes);
      }
    }
  }
}
