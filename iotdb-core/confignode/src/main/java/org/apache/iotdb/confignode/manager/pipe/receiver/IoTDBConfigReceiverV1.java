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

package org.apache.iotdb.confignode.manager.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeTransferFileSealReq;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiverV1;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigNodeHandshakeReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBConfigReceiverV1 extends IoTDBFileReceiverV1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigReceiverV1.class);
  private static final AtomicInteger queryIndex = new AtomicInteger(0);

  private final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();

  private static final PipePlanTSStatusVisitor statusVisitor = new PipePlanTSStatusVisitor();
  private static final PipePlanExceptionVisitor exceptionVisitor = new PipePlanExceptionVisitor();

  IoTDBConfigReceiverV1() {}

  @Override
  public IoTDBConnectorRequestVersion getVersion() {
    return IoTDBConnectorRequestVersion.VERSION_1;
  }

  @Override
  public TPipeTransferResp receive(TPipeTransferReq req) {
    try {
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeRequestType.valueOf(rawRequestType)) {
          case CONFIGNODE_HANDSHAKE:
            return handleTransferHandshake(
                PipeTransferConfigNodeHandshakeReq.fromTPipeTransferReq(req));
          case TRANSFER_CONFIG_PLAN:
            return handleTransferConfigPlan(PipeTransferConfigPlanReq.fromTPipeTransferReq(req));
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
            return handleTransferFilePiece(
                PipeTransferConfigSnapshotPieceReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest);
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            return handleTransferFileSeal(
                PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req));
          default:
            break;
        }
      }

      // Unknown request type, which means the request can not be handled by this receiver,
      // maybe the version of the receiver is not compatible with the sender
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TYPE_ERROR,
              String.format("Unsupported PipeRequestType on ConfigNode %s.", rawRequestType));
      LOGGER.warn("Unsupported PipeRequestType on ConfigNode, response status = {}.", status);
      return new TPipeTransferResp(status);
    } catch (IOException | ConsensusException e) {
      String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn(error);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeTransferResp handleTransferConfigPlan(PipeTransferConfigPlanReq req)
      throws IOException, ConsensusException {
    ConfigPhysicalPlan plan = ConfigPhysicalPlan.Factory.create(req.body);
    TSStatus result;
    try {
      result = executePlan(plan);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Failure status encountered while executing plan {}: {}", plan, result);
        result = statusVisitor.process(plan, result);
      }
    } catch (Exception e) {
      LOGGER.warn("Exception encountered while executing plan {}: ", plan, e);
      result = exceptionVisitor.process(plan, e);
    }
    return new TPipeTransferResp(result);
  }

  private TSStatus executePlan(ConfigPhysicalPlan plan) throws ConsensusException {
    switch (plan.getType()) {
      case CreateDatabase:
        ((DatabaseSchemaPlan) plan)
            .getSchema()
            .setSchemaReplicationFactor(
                ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor());
        ((DatabaseSchemaPlan) plan)
            .getSchema()
            .setDataReplicationFactor(
                ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());
        return configManager.getClusterSchemaManager().setDatabase((DatabaseSchemaPlan) plan, true);
      case AlterDatabase:
        return configManager
            .getClusterSchemaManager()
            .alterDatabase((DatabaseSchemaPlan) plan, true);
      case DeleteDatabase:
        return configManager.deleteDatabases(
            new TDeleteDatabasesReq(
                    Collections.singletonList(((DeleteDatabasePlan) plan).getName()))
                .setIsGeneratedByPipe(true));
      case ExtendSchemaTemplate:
        return configManager
            .getClusterSchemaManager()
            .extendSchemaTemplate(((ExtendSchemaTemplatePlan) plan).getTemplateExtendInfo(), true);
      case CommitSetSchemaTemplate:
        return configManager.setSchemaTemplate(
            new TSetSchemaTemplateReq(
                    getPseudoQueryId(),
                    ((CommitSetSchemaTemplatePlan) plan).getName(),
                    ((CommitSetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(true));
      case PipeUnsetTemplate:
        return configManager.unsetSchemaTemplate(
            new TUnsetSchemaTemplateReq(
                    getPseudoQueryId(),
                    ((PipeUnsetSchemaTemplatePlan) plan).getName(),
                    ((PipeUnsetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(true));
      case UpdateTriggerStateInTable:
        // TODO: Record complete message in trigger
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      case DeleteTriggerInTable:
        return configManager.dropTrigger(
            new TDropTriggerReq(((DeleteTriggerInTablePlan) plan).getTriggerName())
                .setIsGeneratedByPipe(true));
      case SetTTL:
        return configManager.getClusterSchemaManager().setTTL((SetTTLPlan) plan, true);
      case DropUser:
      case DropRole:
      case GrantRole:
      case GrantUser:
      case GrantRoleToUser:
      case RevokeUser:
      case RevokeRole:
      case RevokeRoleFromUser:
      case UpdateUser:
        return configManager.getPermissionManager().operatePermission((AuthorPlan) plan, true);
      case CreateSchemaTemplate:
      case CreateUser:
      case CreateRole:
      default:
        return configManager.getConsensusManager().write(new PipeEnrichedPlan(plan));
    }
  }

  // Used to construct pipe related procedures
  private String getPseudoQueryId() {
    return "pipe" + System.currentTimeMillis() + queryIndex.getAndIncrement();
  }

  @Override
  protected String getReceiverFileBaseDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir();
  }

  @Override
  protected TSStatus loadFile(PipeTransferFileSealReq req, String fileAbsolutePath) {
    // TODO
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
