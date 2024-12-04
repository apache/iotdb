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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsensusPipeDataNodeDispatcher implements ConsensusPipeDispatcher {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusPipeDataNodeDispatcher.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  @Override
  public void createPipe(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes,
      boolean needManuallyStart)
      throws Exception {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TCreatePipeReq req =
          new TCreatePipeReq()
              .setPipeName(pipeName)
              .setNeedManuallyStart(needManuallyStart)
              .setExtractorAttributes(extractorAttributes)
              .setProcessorAttributes(processorAttributes)
              .setConnectorAttributes(connectorAttributes);
      TSStatus status = configNodeClient.createPipe(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
        LOGGER.warn("Failed to create consensus pipe-{}, status: {}", pipeName, status);
        throw new PipeException(status.getMessage());
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create consensus pipe-{}", pipeName, e);
      throw new PipeException("Failed to create consensus pipe", e);
    }
  }

  @Override
  public void startPipe(String pipeName) throws Exception {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus status = configNodeClient.startPipe(pipeName);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
        LOGGER.warn("Failed to start consensus pipe-{}, status: {}", pipeName, status);
        throw new PipeException(status.getMessage());
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to start consensus pipe-{}", pipeName, e);
      throw new PipeException("Failed to start consensus pipe", e);
    }
  }

  @Override
  public void stopPipe(String pipeName) throws Exception {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus status = configNodeClient.stopPipe(pipeName);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
        LOGGER.warn("Failed to stop consensus pipe-{}, status: {}", pipeName, status);
        throw new PipeException(status.getMessage());
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to stop consensus pipe-{}", pipeName, e);
      throw new PipeException("Failed to stop consensus pipe", e);
    }
  }

  // Use ConsensusPipeName instead of String to provide information for receiverAgent to release
  // corresponding resource
  @Override
  public void dropPipe(ConsensusPipeName pipeName) throws Exception {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus status = configNodeClient.dropPipe(pipeName.toString());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
        LOGGER.warn("Failed to drop consensus pipe-{}, status: {}", pipeName, status);
        throw new PipeException(status.getMessage());
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to drop consensus pipe-{}", pipeName, e);
      throw new PipeException("Failed to drop consensus pipe", e);
    }
    // Release corresponding receiver's resource
    PipeDataNodeAgent.receiver().pipeConsensus().handleDropPipeConsensusTask(pipeName);
  }
}
