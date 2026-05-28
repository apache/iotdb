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
import org.apache.iotdb.commons.schema.table.WritableView;
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
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeRenameTimeSeriesPlan;
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
import org.apache.iotdb.confignode.i18n.ManagerMessages;
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
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
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

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.AddWritableViewColumnProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.AlterWritableViewColumnDataTypeProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.CreateWritableViewProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.DropWritableViewColumnProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.DropWritableViewProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.RenameWritableViewColumnProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.RenameWritableViewProcedure;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.SetWritableViewPropertiesProcedure;
import org.apache.tsfile.external.commons.lang3.function.TriFunction;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.copyPropertiesForOriginalTable;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getAddWritableViewColumnPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getAlterWritableViewColumnDataTypePlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewColumnPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewColumnCommentPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewCommentPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewPropertiesPlan;
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
          ManagerMessages.RECEIVER_ID_UNSUPPORTED_PIPEREQUESTTYPE_ON_CONFIGNODE_RESPONSE_STATUS,
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      final String error =
          "Exception encountered while handling pipe transfer request. Root cause: "
              + e.getMessage();
      LOGGER.warn(ManagerMessages.RECEIVER_ID, receiverId.get(), error, e);
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

  private TSStatus executePlanAndClassifyExceptions(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      final Pair<ConfigPhysicalPlan, TSStatus> pair = checkPermission(plan);
      plan = pair.getLeft();
      result = pair.getRight();
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            ManagerMessages.RECEIVER_ID_PERMISSION_CHECK_FAILED_WHILE_EXECUTING_PLAN,
            receiverId.get(),
            plan,
            result);
        return result;
      }
      result = executePlan(plan);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            ManagerMessages.RECEIVER_ID_FAILURE_STATUS_ENCOUNTERED_WHILE_EXECUTING_PLAN,
            receiverId.get(),
            plan,
            result);
        result = STATUS_VISITOR.process(plan, result);
      }
    } catch (final Exception e) {
      LOGGER.warn(
          ManagerMessages.RECEIVER_ID_EXCEPTION_ENCOUNTERED_WHILE_EXECUTING_PLAN,
          receiverId.get(),
          plan,
          e);
      result = EXCEPTION_VISITOR.process(plan, e);
    }
    return result;
  }

  private Pair<ConfigPhysicalPlan, TSStatus> checkPermission(ConfigPhysicalPlan plan) {
    TSStatus status = loginIfNecessary();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return new Pair<>(plan, status);
    }

    Pair<ConfigPhysicalPlan, TSStatus> pair = null;
    final String database;
    final String templateName;
    final String triggerName;
    final String entityName;
    switch (plan.getType()) {
      case CreateDatabase:
        database = ((DatabaseSchemaPlan) plan).getSchema().getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.CREATE, database);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
          }
          break;
        }
        status = checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
        break;
      case AlterDatabase:
        database = ((DatabaseSchemaPlan) plan).getSchema().getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.ALTER, database);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
          }
          break;
        }
        status = checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
        break;
      case DeleteDatabase:
        database = ((DeleteDatabasePlan) plan).getName();
        if (PathUtils.isTableModelDatabase(database)) {
          status = checkDatabaseStatus(userEntity, PrivilegeType.DELETE, database);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, database, true);
          }
          break;
        }
        status = checkGlobalStatus(userEntity, PrivilegeType.MANAGE_DATABASE, database, true);
        break;
      case ExtendSchemaTemplate:
        status =
            checkGlobalStatus(
                userEntity,
                PrivilegeType.EXTEND_TEMPLATE,
                ((ExtendSchemaTemplatePlan) plan).getTemplateExtendInfo().getTemplateName(),
                true);
        break;
      case CreateSchemaTemplate:
        templateName = ((CreateSchemaTemplatePlan) plan).getTemplate().getName();
        status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
        break;
      case CommitSetSchemaTemplate:
        templateName = ((CommitSetSchemaTemplatePlan) plan).getName();
        status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
        break;
      case PipeUnsetTemplate:
        templateName = ((PipeUnsetSchemaTemplatePlan) plan).getName();
        status = checkGlobalStatus(userEntity, PrivilegeType.SYSTEM, templateName, true);
        break;
      case PipeDeleteTimeSeries:
        status =
            checkPathsStatus(
                userEntity,
                PrivilegeType.WRITE_SCHEMA,
                new ArrayList<>(
                    PathPatternTree.deserialize(
                            ((PipeDeleteTimeSeriesPlan) plan).getPatternTreeBytes())
                        .getAllPathPatterns()),
                true);
        break;
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
          break;
        } else {
          status =
              checkPathsStatus(
                  userEntity,
                  PrivilegeType.WRITE_SCHEMA,
                  new ArrayList<>(
                      PathPatternTree.deserialize(
                              ((PipeAlterEncodingCompressorPlan) plan).getPatternTreeBytes())
                          .getAllPathPatterns()),
                  true);
          break;
        }
      case PipeDeleteLogicalView:
        status =
            checkPathsStatus(
                userEntity,
                PrivilegeType.WRITE_SCHEMA,
                new ArrayList<>(
                    PathPatternTree.deserialize(
                            ((PipeDeleteLogicalViewPlan) plan).getPatternTreeBytes())
                        .getAllPathPatterns()),
                true);
        break;
      case PipeDeactivateTemplate:
        status =
            checkPathsStatus(
                userEntity,
                PrivilegeType.WRITE_SCHEMA,
                new ArrayList<>(((PipeDeactivateTemplatePlan) plan).getTemplateSetInfo().keySet()),
                true);
        break;
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
          break;
        }
        final String[] paths = ((SetTTLPlan) plan).getPathPattern();
        status =
            ((SetTTLPlan) plan).isDataBase()
                ? checkGlobalStatus(
                    userEntity, PrivilegeType.MANAGE_DATABASE, Arrays.toString(paths), true)
                : checkPathsStatus(
                    userEntity,
                    PrivilegeType.WRITE_SCHEMA,
                    Collections.singletonList(new PartialPath(paths)),
                    true);
        break;
      case PipeAlterTimeSeries:
        status =
            checkPathsStatus(
                userEntity,
                PrivilegeType.WRITE_SCHEMA,
                Collections.singletonList(((PipeAlterTimeSeriesPlan) plan).getMeasurementPath()),
                true);
        break;
      case PipeRenameTimeSeries:
        final List<PartialPath> list = new ArrayList<>(2);
        list.add(PartialPath.deserialize(((PipeRenameTimeSeriesPlan) plan).getOldPathBytes()));
        list.add(PartialPath.deserialize(((PipeRenameTimeSeriesPlan) plan).getOldPathBytes()));
        status = checkPathsStatus(userEntity, PrivilegeType.WRITE_SCHEMA, list, true);
        break;
      case UpdateTriggerStateInTable:
        triggerName = ((UpdateTriggerStateInTablePlan) plan).getTriggerName();
        status = checkGlobalStatus(userEntity, PrivilegeType.USE_TRIGGER, triggerName, true);
        break;
      case DeleteTriggerInTable:
        triggerName = ((DeleteTriggerInTablePlan) plan).getTriggerName();
        status = checkGlobalStatus(userEntity, PrivilegeType.USE_TRIGGER, triggerName, true);
        break;
      case PipeCreateTableOrView:
        status =
            checkTableStatus(
                userEntity,
                PrivilegeType.CREATE,
                ((PipeCreateTableOrViewPlan) plan).getDatabase(),
                ((PipeCreateTableOrViewPlan) plan).getTable().getTableName());
        break;
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
      case RenameWritableViewColumn:
      case RenameTable:
      case RenameView:
      case RenameWritableView:
      case AlterColumnDataType:
        status =
            checkTableStatus(
                userEntity,
                PrivilegeType.ALTER,
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName());
        break;
      case AddWritableViewColumn:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getAddWritableViewColumnPlan(
                        (AddWritableViewColumnPlan) writableViewPlan, viewVisible, originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case SetWritableViewProperties:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getSetWritableViewPropertiesPlan(
                        (SetWritableViewPropertiesPlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case CommitDeleteWritableViewColumn:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getCommitDeleteWritableViewColumnPlan(
                        (CommitDeleteWritableViewColumnPlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case SetWritableViewComment:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getSetWritableViewCommentPlan(
                        (SetWritableViewCommentPlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case SetWritableViewColumnComment:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getSetWritableViewColumnCommentPlan(
                        (SetWritableViewColumnCommentPlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case AlterWritableViewColumnDataType:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getAlterWritableViewColumnDataTypePlan(
                        (AlterWritableViewColumnDataTypePlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.ALTER);
        break;
      case CommitDeleteTable:
      case CommitDeleteView:
        status =
            checkTableStatus(
                userEntity,
                PrivilegeType.DELETE,
                ((CommitDeleteTablePlan) plan).getDatabase(),
                ((CommitDeleteTablePlan) plan).getTableName());
        break;
      case CommitDeleteWritableView:
        pair =
            checkWritableViewPrivilegesAndRewritePlan(
                (writableViewPlan, viewVisible, originalVisible) ->
                    getCommitDeleteWritableViewPlan(
                        (CommitDeleteWritableViewPlan) writableViewPlan,
                        viewVisible,
                        originalVisible),
                (AbstractTablePlan) plan,
                PrivilegeType.DELETE);
        break;
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
          break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        break;
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
            break;
          }
        }
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                () -> entityName);
        status = StatusUtils.OK;
        break;
      case UpdateUser:
      case UpdateUserV2:
      case RUpdateUser:
      case RUpdateUserV2:
        if (((AuthorPlan) plan).getUserName().equals(username)) {
          configManager
              .getAuditLogger()
              .recordObjectAuthenticationAuditLog(
                  userEntity.setPrivilegeType(null).setResult(true), () -> username);
          status = StatusUtils.OK;
          break;
        }
        status =
            checkGlobalStatus(
                userEntity, PrivilegeType.MANAGE_USER, ((AuthorPlan) plan).getUserName(), true);
        break;
      case CreateUser:
      case RCreateUser:
      case CreateUserWithRawPassword:
      case DropUser:
      case DropUserV2:
      case RDropUser:
      case RDropUserV2:
        status =
            checkGlobalStatus(
                userEntity, PrivilegeType.MANAGE_USER, ((AuthorPlan) plan).getUserName(), true);
        break;
      case CreateRole:
      case RCreateRole:
      case DropRole:
      case RDropRole:
      case GrantRoleToUser:
      case RGrantUserRole:
      case RevokeRoleFromUser:
      case RRevokeUserRole:
        status =
            checkGlobalStatus(
                userEntity, PrivilegeType.MANAGE_ROLE, ((AuthorPlan) plan).getRoleName(), true);
        break;
      default:
        break;
    }
    return Objects.nonNull(pair) ? pair : new Pair<>(plan, status);
  }

  private Pair<ConfigPhysicalPlan, TSStatus> checkWritableViewPrivilegesAndRewritePlan(
      final TriFunction<AbstractTablePlan, Boolean, Boolean, Optional<ConfigPhysicalPlan>>
          planRewriter,
      AbstractTablePlan viewPlan,
      final PrivilegeType type) {
    final TSStatus status;

    status = checkTableStatus(userEntity, type, viewPlan.getDatabase(), viewPlan.getTableName());
    final boolean isViewVisible = status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    if (!isViewVisible && !skipIfNoPrivileges.get()) {
      return new Pair<>(viewPlan, status);
    }
    if (Objects.isNull(viewPlan.getOriginalDatabase())) {
      return new Pair<>(viewPlan, status);
    }

    // Skip if the source table does not have privilege despite the setting
    final Optional<ConfigPhysicalPlan> rewrittenPlan =
        planRewriter.apply(
            viewPlan,
            isViewVisible,
            checkTableStatus(
                        userEntity,
                        type,
                        viewPlan.getOriginalDatabase(),
                        viewPlan.getOriginalTableName())
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (rewrittenPlan.isPresent()) {
      viewPlan = (AbstractTablePlan) rewrittenPlan.get();
    }
    return new Pair<>(viewPlan, status);
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
    final Pair<Pair<TSStatus, Boolean>, Pair<String, String>> result;
    final TSStatus status;
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
      case PipeRenameTimeSeries:
        return configManager
            .getProcedureManager()
            .renameTimeSeries(
                queryId,
                PartialPath.deserialize(((PipeRenameTimeSeriesPlan) plan).getOldPathBytes()),
                PartialPath.deserialize(((PipeRenameTimeSeriesPlan) plan).getNewPathBytes()),
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
      case AddWritableViewColumn:
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.ADD_WRITABLE_VIEW_COLUMN_PROCEDURE,
            new AddWritableViewColumnProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                ((AddWritableViewColumnPlan) plan).getColumnSchemaList(),
                shouldMarkAsPipeRequest.get()),
            ProcedureType.ADD_TABLE_COLUMN_PROCEDURE,
            new AddTableColumnProcedure(
                ((AddWritableViewColumnPlan) plan).getOriginalDatabase(),
                ((AddWritableViewColumnPlan) plan).getOriginalTableName(),
                queryId,
                ((AddWritableViewColumnPlan) plan).getOriginalColumnSchemaList(),
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
      case SetWritableViewProperties:
        final SetWritableViewPropertiesPlan setWritableViewPropertiesPlan =
            (SetWritableViewPropertiesPlan) plan;
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE,
            new SetWritableViewPropertiesProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                setWritableViewPropertiesPlan.getProperties(),
                shouldMarkAsPipeRequest.get(),
                false),
            ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE,
            buildOriginalTablePropertiesProcedure(
                setWritableViewPropertiesPlan, queryId, shouldMarkAsPipeRequest.get()));
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
      case CommitDeleteWritableViewColumn:
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.DROP_WRITABLE_VIEW_COLUMN_PROCEDURE,
            new DropWritableViewColumnProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                ((CommitDeleteWritableViewColumnPlan) plan).getColumnName(),
                shouldMarkAsPipeRequest.get()),
            ProcedureType.DROP_TABLE_COLUMN_PROCEDURE,
            new DropTableColumnProcedure(
                ((CommitDeleteWritableViewColumnPlan) plan).getOriginalDatabase(),
                ((CommitDeleteWritableViewColumnPlan) plan).getOriginalTableName(),
                queryId,
                ((CommitDeleteWritableViewColumnPlan) plan).getOriginalColumnName(),
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
      case AlterWritableViewColumnDataType:
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE,
            new AlterWritableViewColumnDataTypeProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                ((AlterWritableViewColumnDataTypePlan) plan).getColumnName(),
                ((AlterWritableViewColumnDataTypePlan) plan).getNewType(),
                shouldMarkAsPipeRequest.get()),
            ProcedureType.ALTER_TABLE_COLUMN_DATATYPE_PROCEDURE,
            new AlterTableColumnDataTypeProcedure(
                ((AlterWritableViewColumnDataTypePlan) plan).getOriginalDatabase(),
                ((AlterWritableViewColumnDataTypePlan) plan).getOriginalTableName(),
                queryId,
                ((AlterWritableViewColumnDataTypePlan) plan).getOriginalColumnName(),
                ((AlterWritableViewColumnDataTypePlan) plan).getNewType(),
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
      case RenameWritableViewColumn:
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.RENAME_WRITABLE_VIEW_COLUMN_PROCEDURE,
            new RenameWritableViewColumnProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                ((RenameWritableViewColumnPlan) plan).getOldName(),
                ((RenameWritableViewColumnPlan) plan).getNewName(),
                shouldMarkAsPipeRequest.get()),
            null,
            null);
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
      case CommitDeleteWritableView:
        return executeWritableViewPlan(
            (AbstractTablePlan) plan,
            queryId,
            ProcedureType.DROP_WRITABLE_VIEW_PROCEDURE,
            new DropWritableViewProcedure(
                ((AbstractTablePlan) plan).getDatabase(),
                ((AbstractTablePlan) plan).getTableName(),
                queryId,
                shouldMarkAsPipeRequest.get()),
            null,
            null);
      case SetTableComment:
        return configManager
            .getClusterSchemaManager()
            .setTableComment(
                ((SetTableCommentPlan) plan).getDatabase(),
                ((SetTableCommentPlan) plan).getTableName(),
                ((SetTableCommentPlan) plan).getComment(),
                false,
                shouldMarkAsPipeRequest.get(),
                false,
                null,
                null);
      case SetViewComment:
        return configManager
            .getClusterSchemaManager()
            .setTableComment(
                ((SetViewCommentPlan) plan).getDatabase(),
                ((SetViewCommentPlan) plan).getTableName(),
                ((SetViewCommentPlan) plan).getComment(),
                true,
                shouldMarkAsPipeRequest.get(),
                false,
                null,
                null);
      case SetWritableViewComment:
        result =
            configManager.checkWritableView(
                ((SetWritableViewCommentPlan) plan).getDatabase(),
                ((SetWritableViewCommentPlan) plan).getTableName(),
                false,
                true);
        if (result.getLeft().getLeft().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return result.getLeft().getLeft();
        }
        status =
            STATUS_VISITOR.process(
                plan,
                configManager
                    .getClusterSchemaManager()
                    .setTableComment(
                        ((SetWritableViewCommentPlan) plan).getDatabase(),
                        ((SetWritableViewCommentPlan) plan).getTableName(),
                        ((SetWritableViewCommentPlan) plan).getComment(),
                        true,
                        shouldMarkAsPipeRequest.get(),
                        result.left.getRight(),
                        result.right.getLeft(),
                        result.right.getRight()));
        return Objects.isNull(((SetWritableViewCommentPlan) plan).getOriginalDatabase())
                || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    && status.getCode()
                        != TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()
            ? status
            : configManager
                .getClusterSchemaManager()
                .setTableComment(
                    ((SetWritableViewCommentPlan) plan).getOriginalDatabase(),
                    ((SetWritableViewCommentPlan) plan).getOriginalTableName(),
                    ((SetWritableViewCommentPlan) plan).getComment(),
                    true,
                    shouldMarkAsPipeRequest.get(),
                    false,
                    result.right.getLeft(),
                    result.right.getRight());
      case SetTableColumnComment:
        return configManager
            .getClusterSchemaManager()
            .setTableColumnComment(
                ((SetTableColumnCommentPlan) plan).getDatabase(),
                ((SetTableColumnCommentPlan) plan).getTableName(),
                ((SetTableColumnCommentPlan) plan).getColumnName(),
                ((SetTableColumnCommentPlan) plan).getComment(),
                shouldMarkAsPipeRequest.get(),
                null,
                null);
      case SetWritableViewColumnComment:
        final SetWritableViewColumnCommentPlan setWritableViewColumnCommentPlan =
            (SetWritableViewColumnCommentPlan) plan;
        result =
            configManager.checkWritableView(
                setWritableViewColumnCommentPlan.getDatabase(),
                setWritableViewColumnCommentPlan.getTableName(),
                false,
                true);
        if (result.getLeft().getLeft().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return result.getLeft().getLeft();
        }
        status =
            STATUS_VISITOR.process(
                plan,
                configManager
                    .getClusterSchemaManager()
                    .setTableColumnComment(
                        setWritableViewColumnCommentPlan.getDatabase(),
                        setWritableViewColumnCommentPlan.getTableName(),
                        setWritableViewColumnCommentPlan.getColumnName(),
                        setWritableViewColumnCommentPlan.getComment(),
                        shouldMarkAsPipeRequest.get(),
                        result.right.getLeft(),
                        result.right.getRight()));
        return Objects.isNull(setWritableViewColumnCommentPlan.getOriginalDatabase())
                || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    && status.getCode()
                        != TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()
            ? status
            : replayOriginalTableColumnComment(
                setWritableViewColumnCommentPlan, shouldMarkAsPipeRequest.get());
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
      case RenameWritableView:
        final RenameWritableViewPlan renameWritableViewPlan = (RenameWritableViewPlan) plan;
        final Pair<Pair<TSStatus, Boolean>, Pair<String, String>> checkWritableViewResult =
            configManager.checkWritableView(
                renameWritableViewPlan.getDatabase(),
                renameWritableViewPlan.getTableName(),
                false,
                true);
        if (checkWritableViewResult.getLeft().getLeft().getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return checkWritableViewResult.getLeft().getLeft();
        }
        return STATUS_VISITOR.process(
            plan,
            configManager
                .getProcedureManager()
                .executeWithoutDuplicate(
                    renameWritableViewPlan.getDatabase(),
                    renameWritableViewPlan.getDatabase(),
                    null,
                    renameWritableViewPlan.getTableName(),
                    renameWritableViewPlan.getNewName(),
                    queryId,
                    ProcedureType.RENAME_WRITABLE_VIEW_PROCEDURE,
                    new RenameWritableViewProcedure(
                        renameWritableViewPlan.getDatabase(),
                        renameWritableViewPlan.getTableName(),
                        queryId,
                        renameWritableViewPlan.getNewName(),
                        shouldMarkAsPipeRequest.get())));
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

  static SetTablePropertiesProcedure buildOriginalTablePropertiesProcedure(
      final SetWritableViewPropertiesPlan plan,
      final String queryId,
      final boolean isGeneratedByPipe) {
    return new SetTablePropertiesProcedure(
        plan.getOriginalDatabase(),
        plan.getOriginalTableName(),
        queryId,
        copyPropertiesForOriginalTable(plan.getProperties()),
        isGeneratedByPipe);
  }

  static SetTableColumnCommentPlan buildOriginalTableColumnCommentPlan(
      final SetWritableViewColumnCommentPlan plan) {
    return new SetTableColumnCommentPlan(
        plan.getOriginalDatabase(),
        plan.getOriginalTableName(),
        plan.getOriginalColumnName(),
        plan.getComment());
  }

  private TSStatus replayOriginalTableColumnComment(
      final SetWritableViewColumnCommentPlan plan, final boolean shouldMarkAsPipeRequest) {
    final SetTableColumnCommentPlan originalPlan = buildOriginalTableColumnCommentPlan(plan);
    return configManager
        .getClusterSchemaManager()
        .setTableColumnComment(
            originalPlan.getDatabase(),
            originalPlan.getTableName(),
            originalPlan.getColumnName(),
            originalPlan.getComment(),
            shouldMarkAsPipeRequest,
            null,
            null);
  }

  private TSStatus executeWritableViewPlan(
      final AbstractTablePlan plan,
      final String queryId,
      final ProcedureType viewProcedureType,
      final Procedure<ConfigNodeProcedureEnv> viewProcedure,
      final @Nullable ProcedureType originalProcedureType,
      final @Nullable Procedure<ConfigNodeProcedureEnv> originalProcedure) {
    final Pair<Pair<TSStatus, Boolean>, Pair<String, String>> result =
        configManager.checkWritableView(
            plan.getDatabase(),
            plan.getTableName(),
            plan.getType() == ConfigPhysicalPlanType.AlterWritableViewColumnDataType,
            true);
    if (result.getLeft().getLeft().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return result.getLeft().getLeft();
    }
    final TSStatus status =
        STATUS_VISITOR.process(
            plan,
            configManager
                .getProcedureManager()
                .executeWithoutDuplicate(
                    plan.getDatabase(),
                    result.getRight().left,
                    null,
                    plan.getTableName(),
                    result.getRight().right,
                    queryId,
                    viewProcedureType,
                    viewProcedure));
    return Objects.isNull(plan.getOriginalDatabase())
            || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                && status.getCode()
                    != TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()
            || Objects.isNull(originalProcedureType)
        ? status
        : configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                plan.getOriginalDatabase(),
                null,
                plan.getOriginalTableName(),
                queryId,
                originalProcedureType,
                originalProcedure);
  }

  private TSStatus executeIdempotentCreateTableOrView(
      final PipeCreateTableOrViewPlan plan,
      final String queryId,
      final boolean shouldMarkAsPipeRequest)
      throws ConsensusException {
    final String database = plan.getDatabase();
    final TsTable table = plan.getTable();
    final boolean isPlainTable =
        !TreeViewSchema.isTreeViewTable(table) && !(table instanceof WritableView);
    TSStatus result =
        configManager
            .getProcedureManager()
            .executeWithoutDuplicate(
                database,
                table,
                table.getTableName(),
                queryId,
                getCreateTableOrViewProcedureType(table),
                buildCreateTableOrViewProcedure(database, table, shouldMarkAsPipeRequest));
    // Note that the view and its column won't be auto created
    // Skip it to avoid affecting the existing base table
    if (isPlainTable && result.getCode() == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()) {
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

  static ProcedureType getCreateTableOrViewProcedureType(final TsTable table) {
    if (table instanceof WritableView) {
      return ProcedureType.CREATE_WRITABLE_VIEW_PROCEDURE;
    }
    return TreeViewSchema.isTreeViewTable(table)
        ? ProcedureType.CREATE_TABLE_VIEW_PROCEDURE
        : ProcedureType.CREATE_TABLE_PROCEDURE;
  }

  static Procedure<ConfigNodeProcedureEnv> buildCreateTableOrViewProcedure(
      final String database, final TsTable table, final boolean shouldMarkAsPipeRequest) {
    if (table instanceof WritableView) {
      return new CreateWritableViewProcedure(
          database, (WritableView) table, false, shouldMarkAsPipeRequest);
    }
    return TreeViewSchema.isTreeViewTable(table)
        ? new CreateTableViewProcedure(database, table, true, shouldMarkAsPipeRequest)
        : new CreateTableProcedure(database, table, shouldMarkAsPipeRequest);
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
    return configManager.login(username, password, false).getStatus();
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
        ManagerMessages.IOTDBCONFIGNODERECEIVER_DOES_NOT_SUPPORT_LOAD_FILE_V1);
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
          String.format(
              ManagerMessages.THE_CONFIG_REGION_SNAPSHOTS_CANNOT_BE_PARSED, fileAbsolutePaths));
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
