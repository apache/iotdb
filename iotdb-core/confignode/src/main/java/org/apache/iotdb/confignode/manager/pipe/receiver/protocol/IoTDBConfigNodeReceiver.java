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

package org.apache.iotdb.confignode.manager.pipe.receiver.protocol;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiver;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.extractor.IoTDBConfigRegionExtractor;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigNodeReceiverMetrics;
import org.apache.iotdb.confignode.manager.pipe.receiver.visitor.PipeConfigPhysicalPlanExceptionVisitor;
import org.apache.iotdb.confignode.manager.pipe.receiver.visitor.PipeConfigPhysicalPlanTSStatusVisitor;
import org.apache.iotdb.confignode.persistence.schema.CNPhysicalPlanGenerator;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.confignode.persistence.schema.ConfignodeSnapshotParser;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBConfigNodeReceiver extends IoTDBFileReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigNodeReceiver.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final AtomicInteger QUERY_ID_GENERATOR = new AtomicInteger(0);

  private static final PipeConfigPhysicalPlanTSStatusVisitor STATUS_VISITOR =
      new PipeConfigPhysicalPlanTSStatusVisitor();
  private static final PipeConfigPhysicalPlanExceptionVisitor EXCEPTION_VISITOR =
      new PipeConfigPhysicalPlanExceptionVisitor();

  private final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();

  @Override
  public TPipeTransferResp receive(final TPipeTransferReq req) {
    try {
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        final PipeRequestType type = PipeRequestType.valueOf(rawRequestType);
        if (needHandshake(type)) {
          return new TPipeTransferResp(
              new TSStatus(TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode())
                  .setMessage(
                      "The receiver ConfigNode has set up a new receiver and the sender must re-send its handshake request."));
        }
        TPipeTransferResp resp;
        long startTime = System.nanoTime();
        switch (type) {
          case HANDSHAKE_CONFIGNODE_V1:
            resp =
                handleTransferHandshakeV1(
                    PipeTransferConfigNodeHandshakeV1Req.fromTPipeTransferReq(req));
            PipeConfigNodeReceiverMetrics.getInstance()
                .recordHandshakeConfigNodeV1Timer(System.nanoTime() - startTime);
            return resp;
          case HANDSHAKE_CONFIGNODE_V2:
            resp =
                handleTransferHandshakeV2(
                    PipeTransferConfigNodeHandshakeV2Req.fromTPipeTransferReq(req));
            PipeConfigNodeReceiverMetrics.getInstance()
                .recordHandshakeConfigNodeV2Timer(System.nanoTime() - startTime);
            return resp;
          case TRANSFER_CONFIG_PLAN:
            resp = handleTransferConfigPlan(PipeTransferConfigPlanReq.fromTPipeTransferReq(req));
            PipeConfigNodeReceiverMetrics.getInstance()
                .recordTransferConfigPlanTimer(System.nanoTime() - startTime);
            return resp;
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
            resp =
                handleTransferFilePiece(
                    PipeTransferConfigSnapshotPieceReq.fromTPipeTransferReq(req),
                    req instanceof AirGapPseudoTPipeTransferRequest,
                    false);
            PipeConfigNodeReceiverMetrics.getInstance()
                .recordTransferConfigSnapshotPieceTimer(System.nanoTime() - startTime);
            return resp;
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            resp =
                handleTransferFileSealV2(
                    PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req));
            PipeConfigNodeReceiverMetrics.getInstance()
                .recordTransferConfigSnapshotSealTimer(System.nanoTime() - startTime);
            return resp;
          case TRANSFER_COMPRESSED:
            return receive(PipeTransferCompressedReq.fromTPipeTransferReq(req));
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
      LOGGER.warn(
          "Receiver id = {}: Unsupported PipeRequestType on ConfigNode, response status = {}.",
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      final String error =
          "Exception encountered while handling pipe transfer request. Root cause: "
              + e.getMessage();
      LOGGER.warn("Receiver id = {}: {}", receiverId.get(), error, e);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  // This indicates that the client from DataNode to ConfigNode is newly created,
  // mainly because the receiver has changed its leader, and thus the sender needs to re-handshake
  // to notify its configurations.
  // Note that the sender needs not to reconstruct its client because the client
  // is directly linked to the preceding DataNode and has not broken.
  private boolean needHandshake(final PipeRequestType type) {
    return Objects.isNull(receiverFileDirWithIdSuffix.get())
        && type != PipeRequestType.HANDSHAKE_CONFIGNODE_V1
        && type != PipeRequestType.HANDSHAKE_CONFIGNODE_V2;
  }

  private TPipeTransferResp handleTransferConfigPlan(final PipeTransferConfigPlanReq req)
      throws IOException {
    return new TPipeTransferResp(
        executePlanAndClassifyExceptions(ConfigPhysicalPlan.Factory.create(req.body)));
  }

  private TSStatus executePlanAndClassifyExceptions(final ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executePlan(plan);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Receiver id = {}: Failure status encountered while executing plan {}: {}",
            receiverId.get(),
            plan,
            result);
        result = STATUS_VISITOR.process(plan, result);
      }
    } catch (final Exception e) {
      LOGGER.warn(
          "Receiver id = {}: Exception encountered while executing plan {}: ",
          receiverId.get(),
          plan,
          e);
      result = EXCEPTION_VISITOR.process(plan, e);
    }
    return result;
  }

  private TSStatus executePlan(final ConfigPhysicalPlan plan) throws ConsensusException {
    switch (plan.getType()) {
      case CreateDatabase:
        // Here we only reserve database name and substitute the sender's local information
        // with the receiver's default configurations
        final TDatabaseSchema schema = ((DatabaseSchemaPlan) plan).getSchema();
        schema.setSchemaReplicationFactor(
            ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor());
        schema.setDataReplicationFactor(
            ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());
        schema.setTimePartitionInterval(
            CommonDescriptor.getInstance().getConfig().getTimePartitionInterval());
        schema.setMinSchemaRegionGroupNum(
            ConfigNodeDescriptor.getInstance()
                .getConf()
                .getDefaultSchemaRegionGroupNumPerDatabase());
        schema.setMinDataRegionGroupNum(
            ConfigNodeDescriptor.getInstance().getConf().getDefaultDataRegionGroupNumPerDatabase());
        schema.setMaxSchemaRegionGroupNum(schema.getMinSchemaRegionGroupNum());
        schema.setMaxDataRegionGroupNum(schema.getMinDataRegionGroupNum());
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
                    generatePseudoQueryId(),
                    ((CommitSetSchemaTemplatePlan) plan).getName(),
                    ((CommitSetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(true));
      case PipeUnsetTemplate:
        return configManager.unsetSchemaTemplate(
            new TUnsetSchemaTemplateReq(
                    generatePseudoQueryId(),
                    ((PipeUnsetSchemaTemplatePlan) plan).getName(),
                    ((PipeUnsetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(true));
      case PipeDeleteTimeSeries:
        return configManager.deleteTimeSeries(
            new TDeleteTimeSeriesReq(
                    generatePseudoQueryId(),
                    ((PipeDeleteTimeSeriesPlan) plan).getPatternTreeBytes())
                .setIsGeneratedByPipe(true));
      case PipeDeleteLogicalView:
        return configManager.deleteLogicalView(
            new TDeleteLogicalViewReq(
                    generatePseudoQueryId(),
                    ((PipeDeleteLogicalViewPlan) plan).getPatternTreeBytes())
                .setIsGeneratedByPipe(true));
      case PipeDeactivateTemplate:
        return configManager
            .getProcedureManager()
            .deactivateTemplate(
                generatePseudoQueryId(),
                ((PipeDeactivateTemplatePlan) plan).getTemplateSetInfo(),
                true);
      case UpdateTriggerStateInTable:
        // TODO: Record complete message in trigger
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      case DeleteTriggerInTable:
        return configManager.dropTrigger(
            new TDropTriggerReq(((DeleteTriggerInTablePlan) plan).getTriggerName())
                .setIsGeneratedByPipe(true));
      case SetTTL:
        return ((SetTTLPlan) plan).getTTL() == TTLCache.NULL_TTL
            ? configManager.getTTLManager().unsetTTL((SetTTLPlan) plan, true)
            : configManager.getTTLManager().setTTL((SetTTLPlan) plan, true);
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
      case CreateUserWithRawPassword:
      default:
        return configManager.getConsensusManager().write(new PipeEnrichedPlan(plan));
    }
  }

  /** Used to construct pipe related procedures */
  private String generatePseudoQueryId() {
    return "pipe_" + System.currentTimeMillis() + "_" + QUERY_ID_GENERATOR.getAndIncrement();
  }

  @Override
  protected String getClusterId() {
    return ConfigNode.getInstance().getConfigManager().getClusterManager().getClusterId();
  }

  @Override
  protected String getReceiverFileBaseDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir();
  }

  @Override
  protected String getSenderHost() {
    final IClientSession session = SESSION_MANAGER.getCurrSession();
    return session != null ? session.getClientAddress() : "unknown";
  }

  @Override
  protected String getSenderPort() {
    final IClientSession session = SESSION_MANAGER.getCurrSession();
    return session != null ? String.valueOf(session.getClientPort()) : "unknown";
  }

  @Override
  protected TSStatus loadFileV1(
      final PipeTransferFileSealReqV1 req, final String fileAbsolutePath) {
    throw new UnsupportedOperationException(
        "IoTDBConfigNodeReceiver does not support load file V1.");
  }

  @Override
  protected TSStatus loadFileV2(
      final PipeTransferFileSealReqV2 req, final List<String> fileAbsolutePaths)
      throws IOException {
    final Map<String, String> parameters = req.getParameters();
    final CNPhysicalPlanGenerator generator =
        ConfignodeSnapshotParser.translate2PhysicalPlan(
            Paths.get(fileAbsolutePaths.get(0)),
            fileAbsolutePaths.size() > 1 ? Paths.get(fileAbsolutePaths.get(1)) : null,
            CNSnapshotFileType.deserialize(
                Byte.parseByte(parameters.get(PipeTransferConfigSnapshotSealReq.FILE_TYPE))));
    if (Objects.isNull(generator)) {
      throw new IOException(
          String.format("The config region snapshots %s cannot be parsed.", fileAbsolutePaths));
    }
    final Set<ConfigPhysicalPlanType> executionTypes =
        PipeConfigRegionSnapshotEvent.getConfigPhysicalPlanTypeSet(
            parameters.get(ColumnHeaderConstant.TYPE));
    final IoTDBTreePattern pattern =
        new IoTDBTreePattern(parameters.get(ColumnHeaderConstant.PATH_PATTERN));
    final List<TSStatus> results = new ArrayList<>();
    while (generator.hasNext()) {
      IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
          .process(generator.next(), pattern)
          .filter(configPhysicalPlan -> executionTypes.contains(configPhysicalPlan.getType()))
          .ifPresent(
              configPhysicalPlan ->
                  results.add(executePlanAndClassifyExceptions(configPhysicalPlan)));
    }
    return PipeReceiverStatusHandler.getPriorStatus(results);
  }
}
