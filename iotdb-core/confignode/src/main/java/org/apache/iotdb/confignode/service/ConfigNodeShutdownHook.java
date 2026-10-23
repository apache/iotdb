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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConfigNodeShutdownHook extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeShutdownHook.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SHUTDOWN_REPORT_RETRY_NUM = 2;

  private final ConfigNode configNode;
  private final SyncConfigNodeClientPool clientPool;

  public ConfigNodeShutdownHook() {
    this(ConfigNode.getInstance(), SyncConfigNodeClientPool.getInstance());
  }

  ConfigNodeShutdownHook(ConfigNode configNode, SyncConfigNodeClientPool clientPool) {
    this.configNode = configNode;
    this.clientPool = clientPool;
  }

  @Override
  public void run() {
    LOGGER.info(ConfigNodeMessages.CONFIGNODE_EXITING);

    TConfigNodeLocation localNode =
        new TConfigNodeLocation(
            CONF.getConfigNodeId(),
            new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
            new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort()));
    // Save the same peer list for leadership transfer and the later shutdown report.
    ConfigManager configManager = configNode.getConfigManager();
    List<TConfigNodeLocation> otherNodes =
        configManager == null
            ? List.of()
            : configManager.getNodeManager().getRegisteredConfigNodes().stream()
                .filter(node -> !node.getInternalEndPoint().equals(localNode.getInternalEndPoint()))
                .toList();
    List<TEndPoint> reportTargets =
        otherNodes.stream().map(TConfigNodeLocation::getInternalEndPoint).toList();
    TEndPoint seed = CONF.getSeedConfigNode();
    if (reportTargets.isEmpty() && seed != null && !seed.equals(localNode.getInternalEndPoint())) {
      reportTargets = List.of(seed);
    }
    if (configManager != null) {
      transferLeader(configManager, otherNodes);
    }

    try {
      configNode.deactivate();
    } catch (IOException e) {
      LOGGER.error(ConfigNodeMessages.MEET_ERROR_WHEN_DEACTIVATE_CONFIGNODE, e);
    }

    // Set and report shutdown to the cluster ConfigNode-leader best-effort, regardless of
    // leadership: a leader that just stepped down may still reach the newly elected leader via
    // redirect. If no leader is reachable, the new leader will mark this node Unknown by
    // heartbeat timeout instead.
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Stopped);
    if (!reportShutdown(localNode, reportTargets)) {
      LOGGER.error(
          ConfigNodeMessages.REPORTING_CONFIGNODE_SHUTDOWN_FAILED_THE_CLUSTER_WILL_STILL_TAKE_THE);
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          ConfigNodeConstant.GLOBAL_NAME
              + ConfigNodeMessages.LOG_EXITS_JVM_MEMORY_USAGE_ARG_0BCD1CCF,
          MemUtils.bytesCntToStr(
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
  }

  private void transferLeader(ConfigManager configManager, List<TConfigNodeLocation> otherNodes) {
    ConsensusManager consensusManager = configManager.getConsensusManager();
    if (consensusManager == null || !consensusManager.isLeader()) {
      return;
    }
    TConfigNodeLocation target =
        otherNodes.stream()
            .filter(
                node ->
                    configManager.getLoadManager().getNodeStatus(node.getConfigNodeId())
                        == NodeStatus.Running)
            .findFirst()
            .orElse(null);
    if (target == null) {
      return;
    }
    try {
      // The local consensus service must remain open until the transfer attempt finishes.
      consensusManager
          .getConsensusImpl()
          .transferLeader(
              consensusManager.getConsensusGroupId(),
              new Peer(
                  consensusManager.getConsensusGroupId(),
                  target.getConfigNodeId(),
                  target.getConsensusEndPoint()));
    } catch (ConsensusException e) {
      LOGGER.warn(
          ConfigNodeMessages
              .LOG_FAILED_TO_TRANSFER_CONFIGNODE_LEADERSHIP_BEFORE_SHUTDOWN_CONTINUING_SHUTDOWN_2B9364D5,
          e);
    }
  }

  private boolean reportShutdown(TConfigNodeLocation localNode, List<TEndPoint> reportTargets) {
    if (reportTargets.isEmpty()) {
      return false;
    }
    long deadline =
        System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(
                CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS());
    while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
      for (TEndPoint target : reportTargets) {
        for (int retry = 0; retry < SHUTDOWN_REPORT_RETRY_NUM; retry++) {
          if (System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
            return false;
          }
          TSStatus result =
              (TSStatus)
                  clientPool.sendSyncRequestToConfigNodeWithRetry(
                      target, localNode, CnToCnNodeRequestType.REPORT_CONFIG_NODE_SHUTDOWN);
          if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return true;
          }
          if (result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
              && result.isSetRedirectNode()) {
            target = result.getRedirectNode();
            if (target.equals(localNode.getInternalEndPoint())) {
              break;
            }
          }
        }
      }
      // A newly elected leader may still be initializing its services after the transfer.
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      try {
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.SECONDS.toNanos(1)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(ConfigNodeMessages.RETRY_WAIT_FAILED, e);
        return false;
      }
    }
    return false;
  }
}
