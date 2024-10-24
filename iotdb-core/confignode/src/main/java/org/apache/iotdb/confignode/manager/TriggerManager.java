/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTransferringTriggersPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggersOnTransferNodesPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TransferringTriggersResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerLocationResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerTableResp;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTriggerLocationReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.trigger.api.enums.TriggerType;

import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TriggerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManager.class);

  private final ConfigManager configManager;
  private final TriggerInfo triggerInfo;

  public TriggerManager(ConfigManager configManager, TriggerInfo triggerInfo) {
    this.configManager = configManager;
    this.triggerInfo = triggerInfo;
  }

  public TriggerInfo getTriggerInfo() {
    return triggerInfo;
  }

  /**
   * Create a trigger in cluster.
   *
   * <p>If TriggerType is STATELESS, we should create TriggerInstance on all DataNodes, the
   * DataNodeLocation in TriggerInformation will be null.
   *
   * <p>If TriggerType is STATEFUL, we should create TriggerInstance on the DataNode with the lowest
   * load, and DataNodeLocation of this DataNode will be saved.
   *
   * <p>All DataNodes will add TriggerInformation of this trigger in local TriggerTable.
   *
   * @param req the createTrigger request
   * @return status of create this trigger
   */
  public TSStatus createTrigger(TCreateTriggerReq req) {
    final boolean isStateful = TriggerType.construct(req.getTriggerType()) == TriggerType.STATEFUL;
    TDataNodeLocation dataNodeLocation = null;
    if (isStateful) {
      Optional<TDataNodeLocation> targetDataNode =
          configManager.getNodeManager().getLowestLoadDataNode();
      if (targetDataNode.isPresent()) {
        dataNodeLocation = targetDataNode.get();
      } else {
        return new TSStatus(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
      }
    }
    final String triggerName = req.getTriggerName();
    final boolean isUsingURI = req.isIsUsingURI();
    final boolean needToSaveJar = isUsingURI && triggerInfo.needToSaveJar(triggerName);
    TriggerInformation triggerInformation =
        new TriggerInformation(
            (PartialPath) PathDeserializeUtil.deserialize(req.pathPattern),
            triggerName,
            req.getClassName(),
            isUsingURI,
            req.getJarName(),
            req.getAttributes(),
            TriggerEvent.construct(req.triggerEvent),
            TTriggerState.INACTIVE,
            isStateful,
            dataNodeLocation,
            FailureStrategy.construct(req.getFailureStrategy()),
            req.getJarMD5());
    return configManager
        .getProcedureManager()
        .createTrigger(
            triggerInformation,
            needToSaveJar ? new Binary(req.getJarFile()) : null,
            req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
  }

  public TSStatus dropTrigger(TDropTriggerReq req) {
    return configManager
        .getProcedureManager()
        .dropTrigger(
            req.getTriggerName(), req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
  }

  public TGetTriggerTableResp getTriggerTable(boolean onlyStateful) {
    try {
      return ((TriggerTableResp)
              configManager.getConsensusManager().read(new GetTriggerTablePlan(onlyStateful)))
          .convertToThriftResponse();
    } catch (IOException | ConsensusException e) {
      LOGGER.error("Fail to get TriggerTable", e);
      return new TGetTriggerTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName) {
    try {
      return ((TriggerLocationResp)
              configManager.getConsensusManager().read(new GetTriggerLocationPlan(triggerName)))
          .convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      return new TGetLocationForTriggerResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()));
    }
  }

  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager.getConsensusManager().read(new GetTriggerJarPlan(req.getJarNameList())))
          .convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new JarResp(res, Collections.emptyList()).convertToThriftResponse();
    }
  }

  /**
   * Step1: Mark Stateful Triggers on UnknownDataNodes as {@link TTriggerState#TRANSFERRING}.
   *
   * <p>Step2: Get all Transferring Triggers marked in Step1.
   *
   * <p>Step3: For each trigger gotten in Step2, find the DataNode with the lowest load, then
   * transfer the Stateful Trigger to it and update this information on all DataNodes.
   *
   * <p>Step4: Update the newest location on ConfigNodes.
   *
   * @param dataNodeLocationMap The DataNodes with {@link
   *     org.apache.iotdb.commons.cluster.NodeStatus#Running} State
   * @return result of transferTrigger
   */
  public TSStatus transferTrigger(
      List<TDataNodeLocation> newUnknownDataNodeList,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    TSStatus transferResult;
    triggerInfo.acquireTriggerTableLock();
    try {
      ConsensusManager consensusManager = configManager.getConsensusManager();
      NodeManager nodeManager = configManager.getNodeManager();

      transferResult =
          consensusManager.write(new UpdateTriggersOnTransferNodesPlan(newUnknownDataNodeList));
      if (transferResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return transferResult;
      }

      List<String> transferringTriggers =
          ((TransferringTriggersResp) consensusManager.read(new GetTransferringTriggersPlan()))
              .getTransferringTriggers();

      for (String trigger : transferringTriggers) {
        TDataNodeLocation newDataNodeLocation =
            nodeManager.getLowestLoadDataNode(dataNodeLocationMap.keySet());

        transferResult =
            RpcUtils.squashResponseStatusList(
                updateTriggerLocation(trigger, newDataNodeLocation, dataNodeLocationMap));
        if (transferResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return transferResult;
        }

        transferResult =
            consensusManager.write(new UpdateTriggerLocationPlan(trigger, newDataNodeLocation));
        if (transferResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return transferResult;
        }
      }
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read/write API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    } finally {
      triggerInfo.releaseTriggerTableLock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public List<TSStatus> updateTriggerLocation(
      String triggerName,
      TDataNodeLocation dataNodeLocation,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    final TUpdateTriggerLocationReq request =
        new TUpdateTriggerLocationReq(triggerName, dataNodeLocation);

    DataNodeAsyncRequestContext<TUpdateTriggerLocationReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TRIGGER_LOCATION, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }
}
