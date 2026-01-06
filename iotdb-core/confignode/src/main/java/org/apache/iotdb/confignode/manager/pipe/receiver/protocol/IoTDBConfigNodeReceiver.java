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
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiver;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.audit.CNAuditLogger;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterEncodingCompressorPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTableOrViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.AddTableViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewPropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.metric.receiver.PipeConfigNodeReceiverMetrics;
import org.apache.iotdb.confignode.manager.pipe.receiver.visitor.PipeConfigPhysicalPlanExceptionVisitor;
import org.apache.iotdb.confignode.manager.pipe.receiver.visitor.PipeConfigPhysicalPlanTSStatusVisitor;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.manager.pipe.source.IoTDBConfigRegionSource;
import org.apache.iotdb.confignode.persistence.schema.CNPhysicalPlanGenerator;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.confignode.persistence.schema.ConfigNodeSnapshotParser;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AlterTableColumnDataTypeProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.AddViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.SetViewPropertiesProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
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
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTreePrivilegeParseVisitor.checkGlobalOrAnyStatus;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTreePrivilegeParseVisitor.checkGlobalStatus;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTreePrivilegeParseVisitor.checkPathsStatus;

public class IoTDBConfigNodeReceiver extends IoTDBFileReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigNodeReceiver.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final AtomicInteger QUERY_ID_GENERATOR = new AtomicInteger(0);

  private static final PipeConfigPhysicalPlanTSStatusVisitor STATUS_VISITOR =
      new PipeConfigPhysicalPlanTSStatusVisitor();
  private static final PipeConfigPhysicalPlanExceptionVisitor EXCEPTION_VISITOR =
      new PipeConfigPhysicalPlanExceptionVisitor();

  private final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
  private final CNAuditLogger auditLogger = configManager.getAuditLogger();

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
        final TPipeTransferResp resp;
        final long startTime = System.nanoTime();
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
            userEntity.setAuditLogOperation(AuditLogOperation.DDL);
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
      result = checkPermission(plan);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Receiver id = {}: Permission check failed while executing plan {}: {}",
            receiverId.get(),
            plan,
            result);
        return result;
      }
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

  private TSStatus checkPermission(final ConfigPhysicalPlan plan) throws IOException {
    TSStatus status = loginIfNecessary();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    final String database;
    final String templateName;
    final String triggerName;
    final String entityName;
    switch (plan.getType()) {
      case CreateDatabase:
        database = ((DatabaseSchemaPlan) plan).getSchema().getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.CREATE, database);
          return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              ? status
              : checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
        }
        return checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
      case AlterDatabase:
        database = ((DatabaseSchemaPlan) plan).getSchema().getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.ALTER, database);
          return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              ? status
              : checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
        }
        return checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
      case DeleteDatabase:
        database = ((DeleteDatabasePlan) plan).getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.DELETE, database);
          return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              ? status
              : checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
        }
        return checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
      case ExtendSchemaTemplate:
        return checkGlobalStatus(
            userEntity,
            PrivilegeType.EXTEND_TEMPLATE,
            ((ExtendSchemaTemplatePlan) plan).getTemplateExtendInfo().getTemplateName(),
            true);
      case CreateSchemaTemplate:
        templateName = ((CreateSchemaTemplatePlan) plan).getTemplate().getName();
        return checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
      case CommitSetSchemaTemplate:
        templateName = ((CommitSetSchemaTemplatePlan) plan).getName();
        return checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
      case PipeUnsetTemplate:
        templateName = ((PipeUnsetSchemaTemplatePlan) plan).getName();
        return checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
      case PipeDeleteTimeSeries:
        return checkPathsStatus(
            userEntity,
            PrivilegeType.WRITE_SCHEMA,
            new ArrayList<>(
                PathPatternTree.deserialize(((PipeDeleteTimeSeriesPlan) plan).getPatternTreeBytes())
                    .getAllPathPatterns()),
            true);
      case PipeAlterEncodingCompressor:
        // The audit check will only filter but not block the plan
        // Hence we do not write any audit log here
        if (configManager
                .checkUserPrivileges(username, new PrivilegeUnion(PrivilegeType.AUDIT))
                .getStatus()
                .getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          ((PipeAlterEncodingCompressorPlan) plan).setMayAlterAudit(false);
        }
        if (skipIfNoPrivileges.get()) {
          final PathPatternTree pathPatternTree =
              PathPatternTree.deserialize(
                  ByteBuffer.wrap(
                      configManager
                          .fetchAuthizedPatternTree(username, PrivilegeType.WRITE_SCHEMA.ordinal())
                          .getPathPatternTree()));
          if (((PipeAlterEncodingCompressorPlan) plan).isMayAlterAudit()) {
            pathPatternTree.appendPathPattern(Audit.TREE_MODEL_AUDIT_DATABASE_PATH_PATTERN, true);
          }
          final String auditObject = pathPatternTree.getAllPathPatterns().toString();
          final PathPatternTree tree =
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  PathPatternTree.deserialize(
                      ((PipeAlterEncodingCompressorPlan) plan).getPatternTreeBytes()),
                  pathPatternTree);
          ((PipeAlterEncodingCompressorPlan) plan).setPatternTreeBytes(tree.serialize());
          configManager
              .getAuditLogger()
              .recordObjectAuthenticationAuditLog(
                  userEntity
                      .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
                      .setResult(!tree.isEmpty()),
                  () -> auditObject);
          return StatusUtils.OK;
        } else {
          return checkPathsStatus(
              userEntity,
              PrivilegeType.WRITE_SCHEMA,
              new ArrayList<>(
                  PathPatternTree.deserialize(
                          ((PipeAlterEncodingCompressorPlan) plan).getPatternTreeBytes())
                      .getAllPathPatterns()),
              true);
        }
      case PipeDeleteLogicalView:
        return checkPathsStatus(
            userEntity,
            PrivilegeType.WRITE_SCHEMA,
            new ArrayList<>(
                PathPatternTree.deserialize(
                        ((PipeDeleteLogicalViewPlan) plan).getPatternTreeBytes())
                    .getAllPathPatterns()),
            true);
      case PipeDeactivateTemplate:
        return checkPathsStatus(
            userEntity,
            PrivilegeType.WRITE_SCHEMA,
            new ArrayList<>(((PipeDeactivateTemplatePlan) plan).getTemplateSetInfo().keySet()),
            true);
      case SetTTL:
        if (Objects.equals(
            configManager
                .getTTLManager()
                .getAllTTL()
                .get(
                    String.join(
                        String.valueOf(IoTDBConstant.PATH_SEPARATOR),
                        ((SetTTLPlan) plan).getPathPattern())),
            ((SetTTLPlan) plan).getTTL())) {
          return StatusUtils.OK;
        }
        final String[] paths = ((SetTTLPlan) plan).getPathPattern();
        return ((SetTTLPlan) plan).isDataBase()
            ? checkGlobalStatus(
                userEntity, PrivilegeType.MANAGE_DATABASE, Arrays.toString(paths), true)
            : checkPathsStatus(
                userEntity,
                PrivilegeType.WRITE_SCHEMA,
                Collections.singletonList(new PartialPath(paths)),
                true);
      case PipeAlterTimeSeries:
        return checkPathsStatus(
            userEntity,
            PrivilegeType.WRITE_SCHEMA,
            Collections.singletonList(((PipeAlterTimeSeriesPlan) plan).getMeasurementPath()),
            true);
      case UpdateTriggerStateInTable:
        triggerName = ((UpdateTriggerStateInTablePlan) plan).getTriggerName();
        return checkGlobalStatus(userEntity, PrivilegeType.USE_TRIGGER, triggerName, true);
      case DeleteTriggerInTable:
        triggerName = ((DeleteTriggerInTablePlan) plan).getTriggerName();
        return checkGlobalStatus(userEntity, PrivilegeType.USE_TRIGGER, triggerName, true);
      case PipeCreateTableOrView:
        return checkTableStatus(
            userEntity,
            PrivilegeType.CREATE,
            ((PipeCreateTableOrViewPlan) plan).getDatabase(),
            ((PipeCreateTableOrViewPlan) plan).getTable().getTableName());
      case AddTableColumn:
      case AddViewColumn:
      case SetTableProperties:
      case SetViewProperties:
      case CommitDeleteColumn:
      case CommitDeleteViewColumn:
      case SetTableComment:
      case SetViewComment:
      case SetTableColumnComment:
      case RenameTableColumn:
      case RenameViewColumn:
      case RenameTable:
      case RenameView:
      case AlterColumnDataType:
        return checkTableStatus(
            userEntity,
            PrivilegeType.ALTER,
            ((AbstractTablePlan) plan).getDatabase(),
            ((AbstractTablePlan) plan).getTableName());
      case CommitDeleteTable:
      case CommitDeleteView:
        return checkTableStatus(
            userEntity,
            PrivilegeType.DELETE,
            ((CommitDeleteTablePlan) plan).getDatabase(),
            ((CommitDeleteTablePlan) plan).getTableName());
      case GrantRole:
      case GrantUser:
      case RevokeUser:
      case RevokeRole:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.GrantUser
                    || plan.getType() == ConfigPhysicalPlanType.RevokeUser
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        status = checkGlobalStatus(userEntity, PrivilegeType.SECURITY, entityName, false);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        for (final int permission : ((AuthorTreePlan) plan).getPermissions()) {
          status =
              PrivilegeType.values()[permission].isPathPrivilege()
                  ? checkPathsStatus(
                      userEntity,
                      PrivilegeType.values()[permission],
                      ((AuthorTreePlan) plan).getNodeNameList(),
                      false,
                      entityName)
                  : checkGlobalStatus(
                      userEntity, PrivilegeType.values()[permission], entityName, false, true);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case RGrantUserAny:
      case RGrantRoleAny:
      case RRevokeUserAny:
      case RRevokeRoleAny:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.RGrantUserAny
                    || plan.getType() == ConfigPhysicalPlanType.RRevokeUserAny
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        for (final int permission : ((AuthorRelationalPlan) plan).getPermissions()) {
          status =
              checkGlobalOrAnyStatus(
                  userEntity, PrivilegeType.values()[permission], entityName, false, true, true);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case RGrantUserAll:
      case RGrantRoleAll:
      case RRevokeUserAll:
      case RRevokeRoleAll:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.RGrantUserAll
                    || plan.getType() == ConfigPhysicalPlanType.RRevokeUserAll
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        for (PrivilegeType privilegeType : PrivilegeType.values()) {
          if (privilegeType.isRelationalPrivilege()) {
            status =
                checkGlobalOrAnyStatus(userEntity, privilegeType, entityName, false, true, true);
          } else if (privilegeType.forRelationalSys()) {
            status = checkGlobalStatus(userEntity, privilegeType, entityName, false, true);
          } else {
            continue;
          }
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case RGrantUserDBPriv:
      case RGrantRoleDBPriv:
      case RRevokeUserDBPriv:
      case RRevokeRoleDBPriv:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.RGrantUserDBPriv
                    || plan.getType() == ConfigPhysicalPlanType.RRevokeUserDBPriv
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        for (final int permission : ((AuthorRelationalPlan) plan).getPermissions()) {
          status =
              checkDatabaseStatus(
                  userEntity,
                  PrivilegeType.values()[permission],
                  ((AuthorRelationalPlan) plan).getDatabaseName(),
                  true);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case RGrantUserTBPriv:
      case RGrantRoleTBPriv:
      case RRevokeUserTBPriv:
      case RRevokeRoleTBPriv:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.RGrantUserTBPriv
                    || plan.getType() == ConfigPhysicalPlanType.RRevokeUserTBPriv
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        for (final int permission : ((AuthorRelationalPlan) plan).getPermissions()) {
          status =
              checkTableStatus(
                  userEntity,
                  PrivilegeType.values()[permission],
                  ((AuthorRelationalPlan) plan).getDatabaseName(),
                  ((AuthorRelationalPlan) plan).getTableName(),
                  false,
                  true);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case RGrantUserSysPri:
      case RGrantRoleSysPri:
      case RRevokeUserSysPri:
      case RRevokeRoleSysPri:
        entityName =
            plan.getType() == ConfigPhysicalPlanType.RGrantUserSysPri
                    || plan.getType() == ConfigPhysicalPlanType.RRevokeUserSysPri
                ? ((AuthorPlan) plan).getUserName()
                : ((AuthorPlan) plan).getRoleName();
        for (final int permission : ((AuthorRelationalPlan) plan).getPermissions()) {
          status =
              checkGlobalStatus(
                  userEntity, PrivilegeType.values()[permission], entityName, false, true);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        return StatusUtils.OK;
      case UpdateUser:
      case UpdateUserV2:
      case RUpdateUser:
      case RUpdateUserV2:
        if (((AuthorPlan) plan).getUserName().equals(username)) {
          configManager
              .getAuditLogger()
              .recordObjectAuthenticationAuditLog(
                  userEntity.setPrivilegeType(null).setResult(true), () -> username);
          return StatusUtils.OK;
        }
        return checkGlobalStatus(
            userEntity, PrivilegeType.MANAGE_USER, ((AuthorPlan) plan).getUserName(), true);
      case CreateUser:
      case RCreateUser:
      case CreateUserWithRawPassword:
      case DropUser:
      case DropUserV2:
      case RDropUser:
      case RDropUserV2:
        return checkGlobalStatus(
            userEntity, PrivilegeType.MANAGE_USER, ((AuthorPlan) plan).getUserName(), true);
      case CreateRole:
      case RCreateRole:
      case DropRole:
      case RDropRole:
      case GrantRoleToUser:
      case RGrantUserRole:
      case RevokeRoleFromUser:
      case RRevokeUserRole:
        return checkGlobalStatus(
            userEntity, PrivilegeType.MANAGE_ROLE, ((AuthorPlan) plan).getRoleName(), true);
      default:
        return StatusUtils.OK;
    }
  }

  private TSStatus checkDatabaseStatus(
      final IAuditEntity userEntity, final PrivilegeType privilegeType, final String database) {
    return checkDatabaseStatus(userEntity, privilegeType, database, false);
  }

  private TSStatus checkDatabaseStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String database,
      final boolean grantOption) {
    final TSStatus result =
        configManager
            .getPermissionManager()
            .checkUserPrivileges(
                userEntity.getUsername(), new PrivilegeUnion(database, privilegeType, grantOption))
            .getStatus();
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      auditLogger.recordObjectAuthenticationAuditLog(
          userEntity
              .setPrivilegeType(privilegeType)
              .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          () -> database);
    }
    return result;
  }

  private TSStatus checkTableStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String database,
      final String tableName) {
    return checkTableStatus(userEntity, privilegeType, database, tableName, true, false);
  }

  private TSStatus checkTableStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String database,
      final String tableName,
      final boolean isLastCheck,
      final boolean grantOption) {
    final TSStatus result =
        configManager
            .getPermissionManager()
            .checkUserPrivileges(
                userEntity.getUsername(),
                new PrivilegeUnion(database, tableName, privilegeType, grantOption))
            .getStatus();
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode() || isLastCheck) {
      auditLogger.recordObjectAuthenticationAuditLog(
          userEntity
              .setPrivilegeType(privilegeType)
              .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          () -> tableName);
    }
    return result;
  }

  private TSStatus executePlan(final ConfigPhysicalPlan plan) throws ConsensusException {
    final String queryId = generatePseudoQueryId();
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
        return configManager
            .getClusterSchemaManager()
            .setDatabase((DatabaseSchemaPlan) plan, shouldMarkAsPipeRequest.get());
      case AlterDatabase:
        return configManager
            .getClusterSchemaManager()
            .alterDatabase((DatabaseSchemaPlan) plan, shouldMarkAsPipeRequest.get());
      case DeleteDatabase:
        return configManager.deleteDatabases(
            new TDeleteDatabasesReq(
                    Collections.singletonList(((DeleteDatabasePlan) plan).getName()))
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get())
                .setIsTableModel(
                    PathUtils.isTableModelDatabase(((DeleteDatabasePlan) plan).getName())));
      case ExtendSchemaTemplate:
        return configManager
            .getClusterSchemaManager()
            .extendSchemaTemplate(
                ((ExtendSchemaTemplatePlan) plan).getTemplateExtendInfo(),
                shouldMarkAsPipeRequest.get());
      case CommitSetSchemaTemplate:
        return configManager.setSchemaTemplate(
            new TSetSchemaTemplateReq(
                    queryId,
                    ((CommitSetSchemaTemplatePlan) plan).getName(),
                    ((CommitSetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get()));
      case PipeUnsetTemplate:
        return configManager.unsetSchemaTemplate(
            new TUnsetSchemaTemplateReq(
                    queryId,
                    ((PipeUnsetSchemaTemplatePlan) plan).getName(),
                    ((PipeUnsetSchemaTemplatePlan) plan).getPath())
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get()));
      case PipeDeleteTimeSeries:
        return configManager.deleteTimeSeries(
            new TDeleteTimeSeriesReq(
                    queryId, ((PipeDeleteTimeSeriesPlan) plan).getPatternTreeBytes())
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get()));
      case PipeDeleteLogicalView:
        return configManager.deleteLogicalView(
            new TDeleteLogicalViewReq(
                    queryId, ((PipeDeleteLogicalViewPlan) plan).getPatternTreeBytes())
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get()));
      case PipeDeactivateTemplate:
        return configManager
            .getProcedureManager()
            .deactivateTemplate(
                queryId,
                ((PipeDeactivateTemplatePlan) plan).getTemplateSetInfo(),
                shouldMarkAsPipeRequest.get());
      case PipeAlterEncodingCompressor:
        return configManager
            .getProcedureManager()
            .alterEncodingCompressor(
                queryId,
                PathPatternTree.deserialize(
                    ((PipeAlterEncodingCompressorPlan) plan).getPatternTreeBytes()),
                ((PipeAlterEncodingCompressorPlan) plan).getEncoding(),
                ((PipeAlterEncodingCompressorPlan) plan).getCompressor(),
                true,
                shouldMarkAsPipeRequest.get(),
                ((PipeAlterEncodingCompressorPlan) plan).isMayAlterAudit());
      case UpdateTriggerStateInTable:
        // TODO: Record complete message in trigger
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      case DeleteTriggerInTable:
        return configManager.dropTrigger(
            new TDropTriggerReq(((DeleteTriggerInTablePlan) plan).getTriggerName())
                .setIsGeneratedByPipe(shouldMarkAsPipeRequest.get()));
      case SetTTL:
        return ((SetTTLPlan) plan).getTTL() == TTLCache.NULL_TTL
            ? configManager
                .getTTLManager()
                .unsetTTL((SetTTLPlan) plan, shouldMarkAsPipeRequest.get())
            : configManager
                .getTTLManager()
                .setTTL((SetTTLPlan) plan, shouldMarkAsPipeRequest.get());
      case PipeAlterTimeSeries:
        return configManager
            .getProcedureManager()
            .alterTimeSeriesDataType(
                queryId,
                ((PipeAlterTimeSeriesPlan) plan).getMeasurementPath(),
                ((PipeAlterTimeSeriesPlan) plan).getOperationType(),
                ((PipeAlterTimeSeriesPlan) plan).getDataType(),
                true);
      case PipeCreateTableOrView:
        return executeIdempotentCreateTableOrView(
            (PipeCreateTableOrViewPlan) plan, queryId, shouldMarkAsPipeRequest.get());
      case AddTableColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((AddTableColumnPlan) plan).getDatabase(),
                null,
                ((AddTableColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.ADD_TABLE_COLUMN_PROCEDURE,
                new AddTableColumnProcedure(
                    ((AddTableColumnPlan) plan).getDatabase(),
                    ((AddTableColumnPlan) plan).getTableName(),
                    queryId,
                    ((AddTableColumnPlan) plan).getColumnSchemaList(),
                    shouldMarkAsPipeRequest.get()));
      case AddViewColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((AddTableViewColumnPlan) plan).getDatabase(),
                null,
                ((AddTableViewColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.ADD_VIEW_COLUMN_PROCEDURE,
                new AddViewColumnProcedure(
                    ((AddTableViewColumnPlan) plan).getDatabase(),
                    ((AddTableViewColumnPlan) plan).getTableName(),
                    queryId,
                    ((AddTableViewColumnPlan) plan).getColumnSchemaList(),
                    shouldMarkAsPipeRequest.get()));
      case SetTableProperties:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((SetTablePropertiesPlan) plan).getDatabase(),
                null,
                ((SetTablePropertiesPlan) plan).getTableName(),
                queryId,
                ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE,
                new SetTablePropertiesProcedure(
                    ((SetTablePropertiesPlan) plan).getDatabase(),
                    ((SetTablePropertiesPlan) plan).getTableName(),
                    queryId,
                    ((SetTablePropertiesPlan) plan).getProperties(),
                    shouldMarkAsPipeRequest.get()));
      case SetViewProperties:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((SetViewPropertiesPlan) plan).getDatabase(),
                null,
                ((SetViewPropertiesPlan) plan).getTableName(),
                queryId,
                ProcedureType.SET_VIEW_PROPERTIES_PROCEDURE,
                new SetViewPropertiesProcedure(
                    ((SetViewPropertiesPlan) plan).getDatabase(),
                    ((SetViewPropertiesPlan) plan).getTableName(),
                    queryId,
                    ((SetViewPropertiesPlan) plan).getProperties(),
                    shouldMarkAsPipeRequest.get()));
      case CommitDeleteColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((CommitDeleteColumnPlan) plan).getDatabase(),
                null,
                ((CommitDeleteColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.DROP_TABLE_COLUMN_PROCEDURE,
                new DropTableColumnProcedure(
                    ((CommitDeleteColumnPlan) plan).getDatabase(),
                    ((CommitDeleteColumnPlan) plan).getTableName(),
                    queryId,
                    ((CommitDeleteColumnPlan) plan).getColumnName(),
                    shouldMarkAsPipeRequest.get()));
      case CommitDeleteViewColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((CommitDeleteViewColumnPlan) plan).getDatabase(),
                null,
                ((CommitDeleteViewColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.DROP_VIEW_COLUMN_PROCEDURE,
                new DropViewColumnProcedure(
                    ((CommitDeleteViewColumnPlan) plan).getDatabase(),
                    ((CommitDeleteViewColumnPlan) plan).getTableName(),
                    queryId,
                    ((CommitDeleteViewColumnPlan) plan).getColumnName(),
                    shouldMarkAsPipeRequest.get()));
      case AlterColumnDataType:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((AlterColumnDataTypePlan) plan).getDatabase(),
                null,
                ((AlterColumnDataTypePlan) plan).getTableName(),
                queryId,
                ProcedureType.ALTER_TABLE_COLUMN_DATATYPE_PROCEDURE,
                new AlterTableColumnDataTypeProcedure(
                    ((AlterColumnDataTypePlan) plan).getDatabase(),
                    ((AlterColumnDataTypePlan) plan).getTableName(),
                    queryId,
                    ((AlterColumnDataTypePlan) plan).getColumnName(),
                    ((AlterColumnDataTypePlan) plan).getNewType(),
                    shouldMarkAsPipeRequest.get()));
      case RenameTableColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((RenameTableColumnPlan) plan).getDatabase(),
                null,
                ((RenameTableColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE,
                new RenameTableColumnProcedure(
                    ((RenameTableColumnPlan) plan).getDatabase(),
                    ((RenameTableColumnPlan) plan).getTableName(),
                    queryId,
                    ((RenameTableColumnPlan) plan).getOldName(),
                    ((RenameTableColumnPlan) plan).getNewName(),
                    shouldMarkAsPipeRequest.get()));
      case RenameViewColumn:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((RenameViewColumnPlan) plan).getDatabase(),
                null,
                ((RenameViewColumnPlan) plan).getTableName(),
                queryId,
                ProcedureType.RENAME_VIEW_COLUMN_PROCEDURE,
                new RenameViewColumnProcedure(
                    ((RenameViewColumnPlan) plan).getDatabase(),
                    ((RenameViewColumnPlan) plan).getTableName(),
                    queryId,
                    ((RenameViewColumnPlan) plan).getOldName(),
                    ((RenameViewColumnPlan) plan).getNewName(),
                    shouldMarkAsPipeRequest.get()));
      case CommitDeleteTable:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((CommitDeleteTablePlan) plan).getDatabase(),
                null,
                ((CommitDeleteTablePlan) plan).getTableName(),
                queryId,
                ProcedureType.DROP_TABLE_PROCEDURE,
                new DropTableProcedure(
                    ((CommitDeleteTablePlan) plan).getDatabase(),
                    ((CommitDeleteTablePlan) plan).getTableName(),
                    queryId,
                    shouldMarkAsPipeRequest.get()));
      case CommitDeleteView:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((CommitDeleteViewPlan) plan).getDatabase(),
                null,
                ((CommitDeleteViewPlan) plan).getTableName(),
                queryId,
                ProcedureType.DROP_VIEW_PROCEDURE,
                new DropViewProcedure(
                    ((CommitDeleteViewPlan) plan).getDatabase(),
                    ((CommitDeleteViewPlan) plan).getTableName(),
                    queryId,
                    shouldMarkAsPipeRequest.get()));
      case SetTableComment:
        return configManager
            .getClusterSchemaManager()
            .setTableComment(
                ((SetTableCommentPlan) plan).getDatabase(),
                ((SetTableCommentPlan) plan).getTableName(),
                ((SetTableCommentPlan) plan).getComment(),
                false,
                shouldMarkAsPipeRequest.get());
      case SetViewComment:
        return configManager
            .getClusterSchemaManager()
            .setTableComment(
                ((SetViewCommentPlan) plan).getDatabase(),
                ((SetViewCommentPlan) plan).getTableName(),
                ((SetViewCommentPlan) plan).getComment(),
                true,
                shouldMarkAsPipeRequest.get());
      case SetTableColumnComment:
        return configManager
            .getClusterSchemaManager()
            .setTableColumnComment(
                ((SetTableColumnCommentPlan) plan).getDatabase(),
                ((SetTableColumnCommentPlan) plan).getTableName(),
                ((SetTableColumnCommentPlan) plan).getColumnName(),
                ((SetTableColumnCommentPlan) plan).getComment(),
                shouldMarkAsPipeRequest.get());
      case PipeDeleteDevices:
        return configManager
            .getProcedureManager()
            .deleteDevices(
                new TDeleteTableDeviceReq(
                    ((PipeDeleteDevicesPlan) plan).getDatabase(),
                    ((PipeDeleteDevicesPlan) plan).getTableName(),
                    queryId,
                    ByteBuffer.wrap(((PipeDeleteDevicesPlan) plan).getPatternBytes()),
                    ByteBuffer.wrap(((PipeDeleteDevicesPlan) plan).getFilterBytes()),
                    ByteBuffer.wrap(((PipeDeleteDevicesPlan) plan).getModBytes())),
                shouldMarkAsPipeRequest.get())
            .getStatus();
      case RenameTable:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((RenameTablePlan) plan).getDatabase(),
                null,
                ((RenameTablePlan) plan).getTableName(),
                ((RenameTablePlan) plan).getNewName(),
                queryId,
                ProcedureType.RENAME_TABLE_PROCEDURE,
                new RenameTableProcedure(
                    ((RenameTablePlan) plan).getDatabase(),
                    ((RenameTablePlan) plan).getTableName(),
                    queryId,
                    ((RenameTablePlan) plan).getNewName(),
                    shouldMarkAsPipeRequest.get()));
      case RenameView:
        return configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                ((RenameViewPlan) plan).getDatabase(),
                null,
                ((RenameViewPlan) plan).getTableName(),
                queryId,
                ProcedureType.RENAME_VIEW_PROCEDURE,
                new RenameViewProcedure(
                    ((RenameViewPlan) plan).getDatabase(),
                    ((RenameViewPlan) plan).getTableName(),
                    queryId,
                    ((RenameViewPlan) plan).getNewName(),
                    shouldMarkAsPipeRequest.get()));
      case CreateUser:
      case CreateUserWithRawPassword:
      case CreateRole:
      case DropUser:
      case DropUserV2:
      case DropRole:
      case GrantRole:
      case GrantUser:
      case GrantRoleToUser:
      case RevokeUser:
      case RevokeRole:
      case RevokeRoleFromUser:
      case UpdateUser:
      case UpdateUserV2:
      case RCreateUser:
      case RCreateRole:
      case RDropUser:
      case RDropUserV2:
      case RUpdateUser:
      case RUpdateUserV2:
      case RDropRole:
      case RGrantRoleAll:
      case RGrantUserAll:
      case RRevokeRoleAll:
      case RRevokeUserAll:
      case RGrantRoleAny:
      case RGrantUserAny:
      case RRevokeUserAny:
      case RRevokeRoleAny:
      case RGrantRoleDBPriv:
      case RGrantUserDBPriv:
      case RRevokeRoleDBPriv:
      case RRevokeUserDBPriv:
      case RGrantRoleTBPriv:
      case RGrantUserTBPriv:
      case RRevokeRoleTBPriv:
      case RRevokeUserTBPriv:
      case RGrantRoleSysPri:
      case RGrantUserSysPri:
      case RRevokeRoleSysPri:
      case RRevokeUserSysPri:
      case RGrantUserRole:
      case RRevokeUserRole:
        return configManager
            .getPermissionManager()
            .operatePermission((AuthorPlan) plan, shouldMarkAsPipeRequest.get());
      case CreateSchemaTemplate:
      default:
        return configManager
            .getConsensusManager()
            .write(shouldMarkAsPipeRequest.get() ? new PipeEnrichedPlan(plan) : plan);
    }
  }

  private TSStatus executeIdempotentCreateTableOrView(
      final PipeCreateTableOrViewPlan plan,
      final String queryId,
      final boolean shouldMarkAsPipeRequest)
      throws ConsensusException {
    final String database = plan.getDatabase();
    final TsTable table = plan.getTable();
    final boolean isView = TreeViewSchema.isTreeViewTable(table);
    TSStatus result =
        configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                database,
                table,
                table.getTableName(),
                queryId,
                isView
                    ? ProcedureType.CREATE_TABLE_VIEW_PROCEDURE
                    : ProcedureType.CREATE_TABLE_PROCEDURE,
                isView
                    ? new CreateTableViewProcedure(database, table, true, shouldMarkAsPipeRequest)
                    : new CreateTableProcedure(database, table, shouldMarkAsPipeRequest));
    // Note that the view and its column won't be auto created
    // Skip it to avoid affecting the existing base table
    if (!isView && result.getCode() == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()) {
      // If the table already exists, we shall add the sender table's columns to the
      // receiver's table, inner procedure guaranteeing that the columns existing at the
      // receiver table will be trimmed
      result =
          executePlan(
              new AddTableColumnPlan(database, table.getTableName(), table.getColumnList(), false));
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.getCode() != TSStatusCode.COLUMN_ALREADY_EXISTS.getStatusCode()) {
        return result;
      }
      // Add comments
      final Optional<String> tableComment = table.getPropValue(TsTable.COMMENT_KEY);
      if (tableComment.isPresent()) {
        result =
            executePlan(
                new SetTableCommentPlan(database, table.getTableName(), tableComment.get()));
      }
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }
      for (final TsTableColumnSchema schema : table.getColumnList()) {
        final String columnComment = schema.getProps().get(TsTable.COMMENT_KEY);
        if (Objects.nonNull(columnComment)) {
          result =
              executePlan(
                  new SetTableColumnCommentPlan(
                      database, table.getTableName(), schema.getColumnName(), columnComment));
        }
        if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return result;
        }
      }
    }
    return result;
  }

  /** Used to construct pipe related procedures */
  private String generatePseudoQueryId() {
    return "pipe_" + System.currentTimeMillis() + "_" + QUERY_ID_GENERATOR.getAndIncrement();
  }

  @Override
  protected String getClusterId() {
    return configManager.getClusterManager().getClusterId();
  }

  // The configNode will try to log in per plan because:
  // 1. The number of plans at configNode is rather small.
  // 2. The detection period (300s) is too long for configPlans.
  @Override
  protected boolean shouldLogin() {
    return true;
  }

  @Override
  protected TSStatus login() {
    return configManager.login(username, password).getStatus();
  }

  @Override
  protected String getReceiverFileBaseDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir();
  }

  @Override
  protected void markFileBaseDirStateAbnormal(String dir) {}

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
        ConfigNodeSnapshotParser.translate2PhysicalPlan(
            Paths.get(fileAbsolutePaths.get(0)),
            fileAbsolutePaths.size() > 1 ? Paths.get(fileAbsolutePaths.get(1)) : null,
            CNSnapshotFileType.deserialize(
                Byte.parseByte(parameters.get(PipeTransferConfigSnapshotSealReq.FILE_TYPE))),
            parameters.getOrDefault("authUserName", ""));
    if (Objects.isNull(generator)) {
      throw new IOException(
          String.format("The config region snapshots %s cannot be parsed.", fileAbsolutePaths));
    }
    final Set<ConfigPhysicalPlanType> executionTypes =
        PipeConfigRegionSnapshotEvent.getConfigPhysicalPlanTypeSet(
            parameters.get(ColumnHeaderConstant.TYPE));
    final boolean isTreeModelDataAllowedToBeCaptured =
        parameters.containsKey(PipeTransferFileSealReqV2.TREE);
    final TreePattern treePattern =
        TreePattern.parsePatternFromString(
            parameters.get(ColumnHeaderConstant.PATH_PATTERN),
            isTreeModelDataAllowedToBeCaptured,
            p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p));
    final TablePattern tablePattern =
        new TablePattern(
            parameters.containsKey(PipeTransferFileSealReqV2.TABLE),
            parameters.get(PipeTransferFileSealReqV2.DATABASE_PATTERN),
            parameters.get(ColumnHeaderConstant.TABLE_NAME));
    final List<TSStatus> results = new ArrayList<>();
    while (generator.hasNext()) {
      IoTDBConfigRegionSource.parseConfigPlan(
              generator.next(), (IoTDBTreePatternOperations) treePattern, tablePattern)
          .filter(
              configPhysicalPlan ->
                  IoTDBConfigRegionSource.isTypeListened(
                      configPhysicalPlan,
                      executionTypes,
                      (IoTDBTreePatternOperations) treePattern,
                      tablePattern))
          .ifPresent(
              configPhysicalPlan ->
                  results.add(executePlanAndClassifyExceptions(configPhysicalPlan)));
    }
    return PipeReceiverStatusHandler.getPriorStatus(results);
  }

  @Override
  protected void closeSession() {
    // Do nothing. The session will be closed in the data node receiver.
  }
}
