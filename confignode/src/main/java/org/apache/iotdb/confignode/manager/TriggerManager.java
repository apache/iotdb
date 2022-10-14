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
import org.apache.iotdb.confignode.consensus.request.read.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.response.TriggerJarResp;
import org.apache.iotdb.confignode.consensus.response.TriggerTableResp;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerJarReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerJarResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.trigger.api.enums.TriggerType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
    boolean isStateful = TriggerType.construct(req.getTriggerType()) == TriggerType.STATEFUL;
    TDataNodeLocation dataNodeLocation =
        isStateful ? configManager.getNodeManager().getLowestLoadDataNode() : null;
    TriggerInformation triggerInformation =
        new TriggerInformation(
            (PartialPath) PathDeserializeUtil.deserialize(req.pathPattern),
            req.getTriggerName(),
            req.getClassName(),
            req.getJarPath(),
            req.getAttributes(),
            TriggerEvent.construct(req.triggerEvent),
            TTriggerState.INACTIVE,
            isStateful,
            dataNodeLocation,
            req.getJarMD5());
    return configManager
        .getProcedureManager()
        .createTrigger(triggerInformation, new Binary(req.getJarFile()));
  }

  public TSStatus dropTrigger(TDropTriggerReq req) {
    return configManager.getProcedureManager().dropTrigger(req.getTriggerName());
  }

  public TGetTriggerTableResp getTriggerTable() {
    try {
      return ((TriggerTableResp)
              configManager.getConsensusManager().read(new GetTriggerTablePlan()).getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get TriggerTable", e);
      return new TGetTriggerTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetTriggerJarResp getTriggerJar(TGetTriggerJarReq req) {
    try {
      return ((TriggerJarResp)
              configManager
                  .getConsensusManager()
                  .read(new GetTriggerJarPlan(req.getJarNameList()))
                  .getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get TriggerJar", e);
      return new TGetTriggerJarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TSStatus transferTrigger(List<TDataNodeLocation> newUnknownDataList) {
    configManager.getConsensusManager().write(new UpdateTriggerStateInTablePlan());

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
