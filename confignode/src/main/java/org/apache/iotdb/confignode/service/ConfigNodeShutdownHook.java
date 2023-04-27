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
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfigNodeShutdownHook extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeShutdownHook.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SHUTDOWN_REPORT_RETRY_NUM = 2;

  @Override
  public void run() {
    boolean isLeader = ConfigNode.getInstance().getConfigManager().getConsensusManager().isLeader();

    try {
      ConfigNode.getInstance().deactivate();
    } catch (IOException e) {
      LOGGER.error("Meet error when deactivate ConfigNode", e);
    }

    if (!isLeader) {
      // Set and report shutdown to cluster ConfigNode-leader
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Unknown);
      boolean isReportSuccess = false;
      TEndPoint targetConfigNode = CONF.getTargetConfigNode();
      for (int retry = 0; retry < SHUTDOWN_REPORT_RETRY_NUM; retry++) {
        TSStatus result =
            (TSStatus)
                SyncConfigNodeClientPool.getInstance()
                    .sendSyncRequestToConfigNodeWithRetry(
                        targetConfigNode,
                        new TConfigNodeLocation(
                            CONF.getConfigNodeId(),
                            new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
                            new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort())),
                        ConfigNodeRequestType.REPORT_CONFIG_NODE_SHUTDOWN);

        if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // Report success
          isReportSuccess = true;
          break;
        } else if (result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          // Redirect
          targetConfigNode = result.getRedirectNode();
        }
      }
      if (!isReportSuccess) {
        LOGGER.error(
            "Reporting ConfigNode shutdown failed. The cluster will still take the current ConfigNode as Running for a few seconds.");
      }
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          ConfigNodeConstant.GLOBAL_NAME + " exits. Jvm memory usage: {}",
          MemUtils.bytesCntToStr(
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
  }
}
