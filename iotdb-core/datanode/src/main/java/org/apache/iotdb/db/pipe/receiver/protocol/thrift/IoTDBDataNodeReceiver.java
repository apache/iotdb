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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiver;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.resource.log.PipePeriodicalLogReducer;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferSliceReqHandler;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.metric.receiver.PipeDataNodeReceiverMetrics;
import org.apache.iotdb.db.pipe.receiver.visitor.PipePlanToStatementVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementExceptionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTSStatusVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTablePatternParseVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTreePatternParseVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeTableStatementDataTypeConvertExecutionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeTreeStatementDataTypeConvertExecutionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeTreeStatementToBatchVisitor;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DatabaseSchemaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils.getRootCause;
import static org.apache.iotdb.db.exception.metadata.DatabaseNotSetException.DATABASE_NOT_SET;

public class IoTDBDataNodeReceiver extends IoTDBFileReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeReceiver.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String[] RECEIVER_FILE_BASE_DIRS = IOTDB_CONFIG.getPipeReceiverFileDirs();
  private static FolderManager folderManager = null;

  public static final PipePlanToStatementVisitor PLAN_TO_STATEMENT_VISITOR =
      new PipePlanToStatementVisitor();
  public static final PipeStatementTSStatusVisitor STATEMENT_STATUS_VISITOR =
      new PipeStatementTSStatusVisitor();
  public static final PipeStatementExceptionVisitor STATEMENT_EXCEPTION_VISITOR =
      new PipeStatementExceptionVisitor();
  private static final PipeStatementTreePatternParseVisitor STATEMENT_TREE_PATTERN_PARSE_VISITOR =
      new PipeStatementTreePatternParseVisitor();
  private static final PipeStatementTablePatternParseVisitor STATEMENT_TABLE_PATTERN_PARSE_VISITOR =
      new PipeStatementTablePatternParseVisitor();
  private final PipeTableStatementDataTypeConvertExecutionVisitor
      tableStatementDataTypeConvertExecutionVisitor =
          new PipeTableStatementDataTypeConvertExecutionVisitor(
              this::executeStatementForTableModel);
  private final PipeTreeStatementDataTypeConvertExecutionVisitor
      treeStatementDataTypeConvertExecutionVisitor =
          new PipeTreeStatementDataTypeConvertExecutionVisitor(
              statement -> executeStatementForTreeModel(statement, getTreeDatabaseName(statement)));
  public final PipeTreeStatementToBatchVisitor batchVisitor = new PipeTreeStatementToBatchVisitor();

  // Used for data transfer: confignode (cluster A) -> datanode (cluster B) -> confignode (cluster
  // B).
  // If connection from confignode (cluster A) to datanode (cluster B) is lost, the receiver in
  // confignode (cluster B) needs to handle the thread exit using configReceiverId generated by
  // datanode (cluster B).
  private static final AtomicLong CONFIG_RECEIVER_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicReference<String> configReceiverId = new AtomicReference<>();

  private final PipeTransferSliceReqHandler sliceReqHandler = new PipeTransferSliceReqHandler();

  private final SqlParser tableSqlParser = new SqlParser();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private PipeMemoryBlock allocatedMemoryBlock;
  private final List<PipeMemoryBlock> allocatedSliceMemoryBlocks = new ArrayList<>();
  private final Set<String> autoCreatedTreeDatabases = ConcurrentHashMap.newKeySet();
  private final Set<String> conflictedTreeDatabases = ConcurrentHashMap.newKeySet();

  private enum TreeDatabaseCreationResult {
    SKIPPED,
    CREATED_OR_EXISTED,
    CONFLICTED
  }

  static {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(RECEIVER_FILE_BASE_DIRS), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (final DiskSpaceInsufficientException e) {
      LOGGER.error(DataNodePipeMessages.FAIL_TO_CREATE_PIPE_RECEIVER_FILE_FOLDERS, e);
    }
  }

  @Override
  public synchronized TPipeTransferResp receive(final TPipeTransferReq req) {
    try {
      final long startTime = System.nanoTime();
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        final PipeRequestType requestType = PipeRequestType.valueOf(rawRequestType);
        if (requestType != PipeRequestType.TRANSFER_SLICE) {
          clearSliceReqHandler();
        }
        final TPipeTransferResp authResp = checkPipeTransferAuthenticated(requestType);
        if (Objects.nonNull(authResp)) {
          return authResp;
        }
        switch (requestType) {
          case HANDSHAKE_DATANODE_V1:
            {
              try {
                return new TPipeTransferResp(getUnsupportedHandshakeV1Status());
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordHandshakeDatanodeV1Timer(System.nanoTime() - startTime);
              }
            }
          case HANDSHAKE_DATANODE_V2:
            {
              try {
                if (PipeConfig.getInstance().isPipeEnableMemoryCheck()
                    && PipeDataNodeResourceManager.memory().getFreeMemorySizeInBytes()
                        < PipeConfig.getInstance().getPipeMinimumReceiverMemory()) {
                  return new TPipeTransferResp(
                      RpcUtils.getStatus(
                          TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(),
                          "The receiver memory is not enough to handle the handshake request from datanode."));
                }
                return handleTransferHandshakeV2(
                    PipeTransferDataNodeHandshakeV2Req.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordHandshakeDatanodeV2Timer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_INSERT_NODE:
            {
              try {
                return handleTransferTabletInsertNode(
                    PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req));

              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletInsertNodeTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_INSERT_NODE_V2:
            {
              try {
                return handleTransferTabletInsertNode(
                    PipeTransferTabletInsertNodeReqV2.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletInsertNodeV2Timer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_RAW:
            {
              try {
                return handleTransferTabletRaw(PipeTransferTabletRawReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletRawTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_RAW_V2:
            {
              try {
                return handleTransferTabletRaw(
                    PipeTransferTabletRawReqV2.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletRawV2Timer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_BINARY:
            {
              try {
                return handleTransferTabletBinary(
                    PipeTransferTabletBinaryReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletBinaryTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_BINARY_V2:
            {
              try {
                return handleTransferTabletBinary(
                    PipeTransferTabletBinaryReqV2.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletBinaryV2Timer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_BATCH:
            {
              try {
                return handleTransferTabletBatch(
                    PipeTransferTabletBatchReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletBatchTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TABLET_BATCH_V2:
            {
              try {
                return handleTransferTabletBatchV2(
                    PipeTransferTabletBatchReqV2.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTabletBatchV2Timer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TS_FILE_PIECE:
            {
              try {
                return handleTransferFilePiece(
                    PipeTransferTsFilePieceReq.fromTPipeTransferReq(req),
                    req instanceof AirGapPseudoTPipeTransferRequest,
                    true);
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTsFilePieceTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TS_FILE_SEAL:
            {
              try {
                return handleTransferFileSealV1(
                    PipeTransferTsFileSealReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTsFileSealTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TS_FILE_PIECE_WITH_MOD:
            {
              try {
                return handleTransferFilePiece(
                    PipeTransferTsFilePieceWithModReq.fromTPipeTransferReq(req),
                    req instanceof AirGapPseudoTPipeTransferRequest,
                    false);

              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTsFilePieceWithModTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_TS_FILE_SEAL_WITH_MOD:
            {
              try {
                return handleTransferFileSealV2(
                    PipeTransferTsFileSealWithModReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferTsFileSealWithModTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_PLAN_NODE:
            {
              try {
                return handleTransferSchemaPlan(PipeTransferPlanNodeReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferSchemaPlanTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_SCHEMA_SNAPSHOT_PIECE:
            {
              try {
                return handleTransferFilePiece(
                    PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req),
                    req instanceof AirGapPseudoTPipeTransferRequest,
                    false);

              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferSchemaSnapshotPieceTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_SCHEMA_SNAPSHOT_SEAL:
            {
              try {
                return handleTransferFileSealV2(
                    PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req));

              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferSchemaSnapshotSealTimer(System.nanoTime() - startTime);
              }
            }
          case HANDSHAKE_CONFIGNODE_V1:
          case HANDSHAKE_CONFIGNODE_V2:
          case TRANSFER_CONFIG_PLAN:
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            {
              try {
                // Config requests will first be received by the DataNode receiver,
                // then transferred to ConfigNode receiver to execute.
                return handleTransferConfigPlan(req);
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferConfigPlanTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_SLICE:
            {
              try {
                return handleTransferSlice(PipeTransferSliceReq.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferSliceTimer(System.nanoTime() - startTime);
              }
            }
          case TRANSFER_COMPRESSED:
            {
              long requestedMemorySizeInBytes = 0;
              try {
                requestedMemorySizeInBytes =
                    PipeTransferCompressedReq.getMaxAdditionalDecompressedLengthInBytes(req);
                try (final PipeMemoryBlock ignored =
                    tryAllocateReceiverMemory(requestedMemorySizeInBytes)) {
                  return receive(PipeTransferCompressedReq.fromTPipeTransferReq(req));
                }
              } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
                return new TPipeTransferResp(
                    getReceiverTemporaryUnavailableStatus(
                        "decompressing pipe transfer request", requestedMemorySizeInBytes, e));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordTransferCompressedTimer(System.nanoTime() - startTime);
              }
            }
          default:
            break;
        }
      }

      // Unknown request type, which means the request can not be handled by this receiver,
      // maybe the version of the receiver is not compatible with the sender
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TYPE_ERROR,
              String.format("Unknown PipeRequestType %s.", rawRequestType));
      LOGGER.warn(
          DataNodePipeMessages.RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS,
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      final String error =
          String.format("Exception %s encountered while handling request %s.", e.getMessage(), req);
      PipeLogger.log(LOGGER::warn, e, "Receiver id = %s: %s", receiverId.get(), error);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeTransferResp handleTransferTabletInsertNode(
      final PipeTransferTabletInsertNodeReq req) {
    final InsertBaseStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletBinary(final PipeTransferTabletBinaryReq req) {
    final InsertBaseStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletRaw(final PipeTransferTabletRawReq req) {
    final InsertTabletStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletBatch(final PipeTransferTabletBatchReq req) {
    final Pair<InsertRowsStatement, InsertMultiTabletsStatement> statementPair =
        req.constructStatements();
    return new TPipeTransferResp(
        PipeReceiverStatusHandler.getPriorStatus(
            Stream.of(
                    statementPair.getLeft().isEmpty()
                        ? RpcUtils.SUCCESS_STATUS
                        : executeBatchStatementAndAddRedirectInfo(statementPair.getLeft()),
                    statementPair.getRight().isEmpty()
                        ? RpcUtils.SUCCESS_STATUS
                        : executeBatchStatementAndAddRedirectInfo(statementPair.getRight()))
                .collect(Collectors.toList())));
  }

  private TPipeTransferResp handleTransferTabletBatchV2(final PipeTransferTabletBatchReqV2 req) {
    final List<InsertBaseStatement> statementSet = req.constructStatements();
    return new TPipeTransferResp(
        PipeReceiverStatusHandler.getPriorStatus(
            (statementSet.isEmpty()
                    ? Stream.of(RpcUtils.SUCCESS_STATUS)
                    : statementSet.stream().map(this::executeBatchStatementAndAddRedirectInfo))
                .collect(Collectors.toList())));
  }

  @Override
  protected String getClusterId() {
    return IoTDBDescriptor.getInstance().getConfig().getClusterId();
  }

  private TPipeTransferResp checkPipeTransferAuthenticated(final PipeRequestType requestType) {
    if (!requiresAuthentication(requestType)) {
      return null;
    }

    final IClientSession clientSession = SESSION_MANAGER.getCurrSession();
    if (hasPipeHandshakeCredential || (clientSession != null && clientSession.isLogin())) {
      if (!hasPipeHandshakeCredential && clientSession != null) {
        username = clientSession.getUsername();
        userEntity = AuthorityChecker.createIAuditEntity(username, clientSession);
      }
      return null;
    }

    return new TPipeTransferResp(getNotLoggedInStatus());
  }

  private static boolean requiresAuthentication(final PipeRequestType requestType) {
    switch (requestType) {
      case TRANSFER_TABLET_INSERT_NODE:
      case TRANSFER_TABLET_INSERT_NODE_V2:
      case TRANSFER_TABLET_RAW:
      case TRANSFER_TABLET_RAW_V2:
      case TRANSFER_TABLET_BINARY:
      case TRANSFER_TABLET_BINARY_V2:
      case TRANSFER_TABLET_BATCH:
      case TRANSFER_TABLET_BATCH_V2:
      case TRANSFER_TS_FILE_PIECE:
      case TRANSFER_TS_FILE_SEAL:
      case TRANSFER_TS_FILE_PIECE_WITH_MOD:
      case TRANSFER_TS_FILE_SEAL_WITH_MOD:
      case TRANSFER_PLAN_NODE:
      case TRANSFER_SCHEMA_SNAPSHOT_PIECE:
      case TRANSFER_SCHEMA_SNAPSHOT_SEAL:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected boolean shouldLogin() {
    // The idle time is updated per request
    final IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    return clientSession == null || !clientSession.isLogin() || super.shouldLogin();
  }

  @Override
  protected String getReceiverFileBaseDir() throws DiskSpaceInsufficientException {
    // Get next receiver file base dir by folder manager
    return Objects.isNull(folderManager) ? null : folderManager.getNextFolder();
  }

  @Override
  protected void markFileBaseDirStateAbnormal(String dir) {
    folderManager.updateFolderState(dir, FolderManager.FolderState.ABNORMAL);
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
  protected TSStatus loadFileV1(final PipeTransferFileSealReqV1 req, final String fileAbsolutePath)
      throws IOException {
    return isUsingAsyncLoadTsFileStrategy.get()
        ? loadTsFileAsync(null, Collections.singletonList(fileAbsolutePath))
        : loadTsFileSync(null, fileAbsolutePath);
  }

  @Override
  protected TSStatus loadFileV2(
      final PipeTransferFileSealReqV2 req, final List<String> fileAbsolutePaths)
      throws IOException, IllegalPathException {
    return req instanceof PipeTransferTsFileSealWithModReq
        // TsFile's absolute path will be the second element
        ? (isUsingAsyncLoadTsFileStrategy.get()
            ? loadTsFileAsync(
                ((PipeTransferTsFileSealWithModReq) req).getDatabaseNameByTsFileName(),
                fileAbsolutePaths)
            : loadTsFileSync(
                ((PipeTransferTsFileSealWithModReq) req).getDatabaseNameByTsFileName(),
                fileAbsolutePaths.get(req.getFileNames().size() - 1)))
        : loadSchemaSnapShot(req.getParameters(), fileAbsolutePaths);
  }

  private TSStatus loadTsFileAsync(final String dataBaseName, final List<String> absolutePaths)
      throws IOException {
    final Map<String, String> loadAttributes =
        buildLoadTsFileAttributesForAsync(
            dataBaseName,
            shouldConvertDataTypeOnTypeMismatch,
            validateTsFile.get(),
            shouldMarkAsPipeRequest.get());

    if (!LoadUtil.loadFilesToActiveDir(loadAttributes, absolutePaths, true)) {
      throw new PipeException(DataNodePipeMessages.LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  static Map<String, String> buildLoadTsFileAttributesForAsync(
      final String dataBaseName,
      final boolean shouldConvertDataTypeOnTypeMismatch,
      final boolean validateTsFile,
      final boolean shouldMarkAsPipeRequest) {
    return ActiveLoadPathHelper.buildAttributes(
        dataBaseName,
        LoadTsFileStatement.getDatabaseLevelByTreeDatabase(dataBaseName),
        shouldConvertDataTypeOnTypeMismatch,
        validateTsFile || shouldConvertDataTypeOnTypeMismatch,
        null,
        shouldMarkAsPipeRequest);
  }

  private TSStatus loadTsFileSync(final String dataBaseName, final String fileAbsolutePath)
      throws FileNotFoundException {
    return executeStatementAndClassifyExceptions(
        buildLoadTsFileStatementForSync(
            dataBaseName,
            fileAbsolutePath,
            validateTsFile.get(),
            shouldConvertDataTypeOnTypeMismatch));
  }

  static LoadTsFileStatement buildLoadTsFileStatementForSync(
      final String dataBaseName,
      final String fileAbsolutePath,
      final boolean validateTsFile,
      final boolean shouldConvertDataTypeOnTypeMismatch)
      throws FileNotFoundException {
    final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(fileAbsolutePath);
    statement.setDeleteAfterLoad(true);
    statement.setConvertOnTypeMismatch(shouldConvertDataTypeOnTypeMismatch);
    statement.setVerifySchema(validateTsFile || shouldConvertDataTypeOnTypeMismatch);
    statement.setAutoCreateDatabase(
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled());
    statement.setDatabase(dataBaseName);
    statement.updateDatabaseLevelByTreeDatabase();
    return statement;
  }

  private TSStatus loadSchemaSnapShot(
      final Map<String, String> parameters, final List<String> fileAbsolutePaths)
      throws IllegalPathException, IOException {
    final String databaseName = parameters.get(ColumnHeaderConstant.DATABASE);
    final PartialPath databasePath = PartialPath.getQualifiedDatabasePartialPath(databaseName);
    final boolean isTreeModelDataAllowedToBeCaptured =
        PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters);
    final TreePattern treePattern =
        parseTreePattern(
            parameters.get(ColumnHeaderConstant.PATH_PATTERN), isTreeModelDataAllowedToBeCaptured);

    if (!PathUtils.isTableModelDatabase(databaseName)) {
      if (!shouldLoadTreeSchemaSnapshotDatabase(treePattern, databaseName)) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      final TSStatus createDatabaseStatus = createSchemaSnapshotDatabaseIfNecessary(databasePath);
      if (createDatabaseStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return createDatabaseStatus;
      }
    }

    final SRStatementGenerator generator =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(fileAbsolutePaths.get(0)),
            fileAbsolutePaths.size() > 1 && Objects.nonNull(fileAbsolutePaths.get(1))
                ? Paths.get(fileAbsolutePaths.get(1))
                : null,
            fileAbsolutePaths.size() > 2 && Objects.nonNull(fileAbsolutePaths.get(2))
                ? Paths.get(fileAbsolutePaths.get(2))
                : null,
            databasePath);
    final Set<StatementType> executionTypes =
        PipeSchemaRegionSnapshotEvent.getStatementTypeSet(
            parameters.get(ColumnHeaderConstant.TYPE));
    final TablePattern tablePattern =
        new TablePattern(
            PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters),
            parameters.get(PipeTransferFileSealReqV2.DATABASE_PATTERN),
            parameters.get(ColumnHeaderConstant.TABLE_NAME));

    // Clear to avoid previous exceptions
    batchVisitor.clear();
    final List<TSStatus> results = new ArrayList<>();
    while (generator.hasNext()) {
      final Object treeOrTableStatement = generator.next();
      if (treeOrTableStatement instanceof Statement) {
        final Statement originalStatement = (Statement) treeOrTableStatement;
        if (!executionTypes.contains(originalStatement.getType())) {
          continue;
        }

        // The statements do not contain AlterLogicalViewStatements
        // Here we apply the statements as many as possible
        // Even if there are failed statements
        STATEMENT_TREE_PATTERN_PARSE_VISITOR
            .process(originalStatement, (IoTDBTreePatternOperations) treePattern)
            .flatMap(parsedStatement -> batchVisitor.process(parsedStatement, null))
            .ifPresent(statement -> results.add(executeStatementAndClassifyExceptions(statement)));
      } else if (treeOrTableStatement
          instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement) {
        final org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement
            originalStatement =
                (org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement)
                    treeOrTableStatement;

        if (!executionTypes.contains(StatementType.AUTO_CREATE_DEVICE_MNODE)) {
          continue;
        }

        STATEMENT_TABLE_PATTERN_PARSE_VISITOR
            .process(originalStatement, tablePattern)
            .ifPresent(
                statement ->
                    results.add(
                        executeStatementForTableModelWithPermissionCheck(
                            statement, databasePath.getNodes()[1])));
      }
    }
    batchVisitor.getRemainBatches().stream()
        .filter(Optional::isPresent)
        .forEach(statement -> results.add(executeStatementAndClassifyExceptions(statement.get())));
    return PipeReceiverStatusHandler.getPriorStatus(results);
  }

  static boolean shouldLoadTreeSchemaSnapshotDatabase(
      final String pathPattern,
      final boolean isTreeModelDataAllowedToBeCaptured,
      final String databaseName) {
    return shouldLoadTreeSchemaSnapshotDatabase(
        parseTreePattern(pathPattern, isTreeModelDataAllowedToBeCaptured), databaseName);
  }

  private static TreePattern parseTreePattern(
      final String pathPattern, final boolean isTreeModelDataAllowedToBeCaptured) {
    return TreePattern.parsePatternFromString(
        pathPattern,
        isTreeModelDataAllowedToBeCaptured,
        p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p));
  }

  private static boolean shouldLoadTreeSchemaSnapshotDatabase(
      final TreePattern treePattern, final String databaseName) {
    return treePattern.isTreeModelDataAllowedToBeCaptured()
        && treePattern.mayOverlapWithDb(databaseName);
  }

  private TSStatus createSchemaSnapshotDatabaseIfNecessary(final PartialPath databasePath) {
    final DatabaseSchemaStatement statement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
    statement.setDatabasePath(databasePath);

    final TSStatus status = executeStatementAndClassifyExceptions(statement);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    if (status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      return Objects.equals(
              status.getMessage(),
              databasePath.getFullPath() + " has already been created as database")
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(status.getMessage());
    }

    if (status.getCode() == TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }

    return status;
  }

  private TPipeTransferResp handleTransferSchemaPlan(final PipeTransferPlanNodeReq req) {
    // We may be able to skip the alter logical view's exception parsing because
    // the "AlterLogicalViewNode" is itself idempotent
    if (req.getPlanNode() instanceof AlterLogicalViewNode) {
      AlterLogicalViewNode node = (AlterLogicalViewNode) req.getPlanNode();
      IAuditEntity entity = AuthorityChecker.createIAuditEntity(username, null);
      TSStatus status =
          AuthorityChecker.getAccessControl()
              .checkCanAlterView(entity, node.getSourcePaths(), node.getTargetPaths());
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to check authority for statement %s, username = %s, response = %s.",
            receiverId.get(),
            StatementType.ALTER_LOGICAL_VIEW.name(),
            username,
            status);
        return new TPipeTransferResp(status);
      }
      return new TPipeTransferResp(
          ClusterConfigTaskExecutor.getInstance()
              .alterLogicalViewByPipe(
                  (AlterLogicalViewNode) req.getPlanNode(), shouldMarkAsPipeRequest.get()));
    }
    final Object statement = PLAN_TO_STATEMENT_VISITOR.process(req.getPlanNode(), null);
    return statement instanceof Statement
        ? new TPipeTransferResp(executeStatementAndClassifyExceptions((Statement) statement))
        : new TPipeTransferResp(
            executeStatementForTableModelWithPermissionCheck(
                (org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement) statement,
                null));
  }

  private TPipeTransferResp handleTransferConfigPlan(final TPipeTransferReq req) {
    return ClusterConfigTaskExecutor.getInstance()
        .handleTransferConfigPlan(getConfigReceiverId(), req);
  }

  /** Used to identify the sender client */
  private String getConfigReceiverId() {
    if (Objects.isNull(configReceiverId.get())) {
      configReceiverId.set(
          IoTDBDescriptor.getInstance().getConfig().getDataNodeId()
              + "_"
              + PipeDataNodeAgent.runtime().getRebootTimes()
              + "_"
              + CONFIG_RECEIVER_ID_GENERATOR.incrementAndGet());
    }
    return configReceiverId.get();
  }

  private TPipeTransferResp handleTransferSlice(final PipeTransferSliceReq pipeTransferSliceReq) {
    final long sliceBodySizeInBytes = getSliceBodySizeInBytes(pipeTransferSliceReq);
    long requestedMemorySizeInBytes = sliceBodySizeInBytes;
    String memoryAction = "buffering sliced pipe transfer request";
    PipeMemoryBlock sliceMemoryBlock = null;
    try {
      sliceMemoryBlock = tryAllocateReceiverMemory(sliceBodySizeInBytes);

      final boolean isInorder = sliceReqHandler.receiveSlice(pipeTransferSliceReq);
      if (!isInorder) {
        closeMemoryBlock(sliceMemoryBlock);
        clearSliceReqHandler();
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_SLICE_OUT_OF_ORDER,
                "Slice request is out of order, please check the request sequence."));
      }

      allocatedSliceMemoryBlocks.add(sliceMemoryBlock);
      sliceMemoryBlock = null;

      if (pipeTransferSliceReq.getSliceIndex() + 1 < pipeTransferSliceReq.getSliceCount()) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.SUCCESS_STATUS,
                "Slice received, waiting for more slices to complete the request."));
      }

      memoryAction = "assembling sliced pipe transfer request";
      requestedMemorySizeInBytes = pipeTransferSliceReq.getOriginBodySize();
      try (final PipeMemoryBlock ignored = tryAllocateReceiverMemory(requestedMemorySizeInBytes)) {
        final Optional<TPipeTransferReq> req = sliceReqHandler.makeReqIfComplete();
        if (!req.isPresent()) {
          return new TPipeTransferResp(
              RpcUtils.getStatus(
                  TSStatusCode.SUCCESS_STATUS,
                  "Slice received, waiting for more slices to complete the request."));
        }
        clearSliceReqHandler();
        return receive(req.get());
      }
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      closeMemoryBlock(sliceMemoryBlock);
      clearSliceReqHandler();
      return new TPipeTransferResp(
          getReceiverTemporaryUnavailableStatus(memoryAction, requestedMemorySizeInBytes, e));
    } catch (final RuntimeException e) {
      closeMemoryBlock(sliceMemoryBlock);
      clearSliceReqHandler();
      throw e;
    }
  }

  private long getSliceBodySizeInBytes(final PipeTransferSliceReq pipeTransferSliceReq) {
    return pipeTransferSliceReq.getSliceBody() == null
        ? 0
        : pipeTransferSliceReq.getSliceBody().length;
  }

  private void clearSliceReqHandler() {
    sliceReqHandler.clear();
    allocatedSliceMemoryBlocks.forEach(this::closeMemoryBlock);
    allocatedSliceMemoryBlocks.clear();
  }

  private void closeMemoryBlock(final PipeMemoryBlock memoryBlock) {
    if (Objects.nonNull(memoryBlock)) {
      memoryBlock.close();
    }
  }

  private PipeMemoryBlock tryAllocateReceiverMemory(final long requestedMemorySizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return PipeDataNodeResourceManager.memory()
        .forceAllocate(Math.max(requestedMemorySizeInBytes, 0));
  }

  @Override
  protected TSStatus getReceiverTemporaryUnavailableStatus(
      final String action,
      final long requestedMemorySizeInBytes,
      final PipeRuntimeOutOfMemoryCriticalException e) {
    return new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
        .setMessage(
            String.format(
                DataNodePipeMessages.RECEIVER_TEMPORARILY_OUT_OF_MEMORY_FORMAT,
                action,
                requestedMemorySizeInBytes,
                PipeDataNodeResourceManager.memory().getUsedMemorySizeInBytes(),
                PipeDataNodeResourceManager.memory().getFreeMemorySizeInBytes(),
                PipeDataNodeResourceManager.memory().getTotalNonFloatingMemorySizeInBytes()));
  }

  /**
   * For {@link InsertRowsStatement} and {@link InsertMultiTabletsStatement}, the returned {@link
   * TSStatus} will use sub-status to record the endpoint for redirection. Each sub-status records
   * the redirection endpoint for one device path, and the order is the same as the order of the
   * device paths in the statement. However, this order is not guaranteed to be the same as in the
   * request. So for each sub-status which needs to redirect, we record the device path using the
   * message field.
   */
  private TSStatus executeBatchStatementAndAddRedirectInfo(final InsertBaseStatement statement) {
    final TSStatus result = executeStatementAndClassifyExceptions(statement, 5);

    if (result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        && result.getSubStatusSize() > 0) {
      final List<PartialPath> devicePaths;
      if (statement instanceof InsertRowsStatement) {
        devicePaths = ((InsertRowsStatement) statement).getDevicePaths();
      } else if (statement instanceof InsertMultiTabletsStatement) {
        devicePaths = ((InsertMultiTabletsStatement) statement).getDevicePaths();
      } else {
        LOGGER.warn(
            DataNodePipeMessages.RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION,
            receiverId.get(),
            statement);
        return result;
      }

      if (devicePaths.size() == result.getSubStatusSize()) {
        for (int i = 0; i < devicePaths.size(); ++i) {
          if (result.getSubStatus().get(i).isSetRedirectNode()) {
            result.getSubStatus().get(i).setMessage(devicePaths.get(i).getFullPath());
          }
        }
      } else {
        LOGGER.warn(
            DataNodePipeMessages.RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS,
            receiverId.get(),
            statement,
            result);
      }
    }

    return result;
  }

  private TSStatus executeStatementAndClassifyExceptions(final Statement statement) {
    return executeStatementAndClassifyExceptions(statement, 1);
  }

  private TSStatus executeStatementAndClassifyExceptions(
      final Statement statement, final int tryCount) {
    long estimatedMemory = 0L;
    final double pipeReceiverActualToEstimatedMemoryRatio =
        PIPE_CONFIG.getPipeReceiverActualToEstimatedMemoryRatio();
    try {
      if (statement instanceof InsertBaseStatement) {
        estimatedMemory = ((InsertBaseStatement) statement).ramBytesUsed();
        for (int i = 0; i < tryCount; ++i) {
          try {
            allocatedMemoryBlock =
                PipeDataNodeResourceManager.memory()
                    .forceAllocate(
                        (long) (estimatedMemory * pipeReceiverActualToEstimatedMemoryRatio));
            break;
          } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
            if (i == tryCount - 1) {
              final String message =
                  String.format(
                      "Temporarily out of memory when executing statement %s, Requested memory: %s, "
                          + "used memory: %s, free memory: %s, total non-floating memory: %s",
                      statement,
                      estimatedMemory * pipeReceiverActualToEstimatedMemoryRatio,
                      PipeDataNodeResourceManager.memory().getUsedMemorySizeInBytes(),
                      PipeDataNodeResourceManager.memory().getFreeMemorySizeInBytes(),
                      PipeDataNodeResourceManager.memory().getTotalNonFloatingMemorySizeInBytes());
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(DataNodePipeMessages.RECEIVER_ID, receiverId.get(), message, e);
              }
              return new TSStatus(
                      TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                  .setMessage(message);
            } else {
              Thread.sleep(100L * (i + 1));
            }
          }
        }
      }

      final TSStatus result =
          executeStatementWithPermissionCheckAndRetryOnDataTypeMismatch(statement);
      final int code = result.getCode();
      if (code == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || code == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return result;
      } else {
        if (code != TSStatusCode.OUT_OF_TTL.getStatusCode()) {
          PipeLogger.log(
              LOGGER::warn,
              "Receiver id = %s: Failure status encountered while executing statement %s: %s",
              receiverId.get(),
              statement.getPipeLoggingString(),
              result);
        }
        return STATEMENT_STATUS_VISITOR.process(statement, result);
      }
    } catch (final Exception e) {
      logStatementExceptionIfNecessary(statement, e);
      return STATEMENT_EXCEPTION_VISITOR.process(statement, e);
    } finally {
      if (Objects.nonNull(allocatedMemoryBlock)) {
        allocatedMemoryBlock.close();
        allocatedMemoryBlock = null;
      }
    }
  }

  private void logStatementExceptionIfNecessary(final Statement statement, final Exception e) {
    if (shouldLogStatementException(receiverId.get(), statement, e)) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          "Receiver id = %s: Exception encountered while executing statement %s: ",
          receiverId.get(),
          Objects.isNull(statement) ? null : statement.getPipeLoggingString());
    }
  }

  static boolean shouldLogStatementException(
      final long receiverId, final Statement statement, final Exception e) {
    // Use the reducer cache as a gate. The actual stack trace is logged only when it passes.
    return PipePeriodicalLogReducer.log(
        message -> {},
        "Receiver id = %s, statement = %s, exception = %s, message = %s",
        receiverId,
        Objects.isNull(statement) ? null : statement.getPipeLoggingString(),
        e.getClass().getName(),
        e.getMessage());
  }

  private TSStatus executeStatementWithPermissionCheckAndRetryOnDataTypeMismatch(
      final Statement statement) {
    if (statement == null) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR, "Execute null statement.");
    }

    // Judge which model the statement belongs to
    final boolean isTableModelStatement;
    final String databaseName;
    if (statement instanceof LoadTsFileStatement) {
      // Pipe receiver always constructs a tree-model LoadTsFileStatement. Its database field is
      // only an explicit database hint for table data or pipe-generated tree-model loads, so it
      // must not be used to route execution into the table-model pipeline.
      isTableModelStatement = false;
      databaseName = ((LoadTsFileStatement) statement).getDatabase();
    } else if (statement instanceof InsertBaseStatement
        && ((InsertBaseStatement) statement).isWriteToTable()) {
      isTableModelStatement = true;
      databaseName =
          ((InsertBaseStatement) statement).getDatabaseName().isPresent()
              ? ((InsertBaseStatement) statement).getDatabaseName().get()
              : null;
    } else if (statement instanceof InsertBaseStatement) {
      isTableModelStatement = false;
      databaseName = getTreeDatabaseName(statement);
    } else {
      isTableModelStatement = false;
      databaseName = null;
    }

    // Permission check
    final TSStatus loginStatus = loginIfNecessary();
    if (loginStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return loginStatus;
    }

    final IClientSession clientSession = SESSION_MANAGER.getCurrSession();

    // For table model, the authority check is done in inner execution. No need to check here
    if (!isTableModelStatement) {
      final TSStatus permissionCheckStatus =
          AuthorityChecker.checkAuthority(
              statement,
              new TreeAccessCheckContext(
                  clientSession.getUserId(),
                  clientSession.getUsername(),
                  clientSession.getClientAddress()));
      if (permissionCheckStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to check authority for statement %s, username = %s, response = %s.",
            receiverId.get(),
            statement.getType().name(),
            username,
            permissionCheckStatus);
        return RpcUtils.getStatus(
            permissionCheckStatus.getCode(), permissionCheckStatus.getMessage());
      }
    }

    // Execute insert statements through the conversion wrapper first to avoid writing a partial
    // row/tablet before the type mismatch is converted.
    if (shouldConvertDataTypeOnTypeMismatch && statement instanceof InsertBaseStatement) {
      final Optional<TSStatus> convertedStatus =
          executeInsertStatementWithDataTypeConversion(
              statement, isTableModelStatement, databaseName);
      if (convertedStatus.isPresent()) {
        return convertedStatus.get();
      }
    }

    // Real execution of the statement
    final TSStatus status =
        isTableModelStatement
            ? executeStatementForTableModel(statement, databaseName)
            : executeStatementForTreeModel(statement, getTreeDatabaseName(statement));

    // Try to convert data type if the status code is not success. Insert statements normally return
    // above after the first converted execution. The retry path is kept for load and fallback
    // cases.
    if (!shouldConvertDataTypeOnTypeMismatch
        || !((statement instanceof InsertBaseStatement
                && ((InsertBaseStatement) statement).hasFailedMeasurements())
            || (status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
                && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()))) {
      return status;
    }

    if (statement instanceof LoadTsFileStatement
        && shouldUseTableModelVisitorForLoadStatement((LoadTsFileStatement) statement)) {
      return statement
          .accept(tableStatementDataTypeConvertExecutionVisitor, new Pair<>(status, databaseName))
          .orElse(status);
    }

    return isTableModelStatement
        ? statement
            .accept(tableStatementDataTypeConvertExecutionVisitor, new Pair<>(status, databaseName))
            .orElse(status)
        : statement.accept(treeStatementDataTypeConvertExecutionVisitor, status).orElse(status);
  }

  private Optional<TSStatus> executeInsertStatementWithDataTypeConversion(
      final Statement statement, final boolean isTableModelStatement, final String databaseName) {
    return isTableModelStatement
        ? statement.accept(
            tableStatementDataTypeConvertExecutionVisitor,
            new Pair<>(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), databaseName))
        : statement.accept(
            treeStatementDataTypeConvertExecutionVisitor,
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private boolean shouldUseTableModelVisitorForLoadStatement(
      final LoadTsFileStatement loadTsFileStatement) {
    final List<Boolean> isTableModel = loadTsFileStatement.getIsTableModel();
    return Objects.nonNull(isTableModel)
        && !isTableModel.isEmpty()
        && isTableModel.stream().allMatch(Boolean.TRUE::equals);
  }

  @Override
  protected TSStatus login() {
    final IClientSession session = SESSION_MANAGER.getCurrSession();

    if (!hasPipeHandshakeCredential) {
      return session != null && session.isLogin()
          ? RpcUtils.SUCCESS_STATUS
          : getNotLoggedInStatus();
    }

    if (session != null && !session.isLogin()) {
      final BasicOpenSessionResp openSessionResp =
          SESSION_MANAGER.login(
              session,
              username,
              password,
              ZoneId.systemDefault().toString(),
              SessionManager.CURRENT_RPC_VERSION,
              IoTDBConstant.ClientVersion.V_1_0);
      return RpcUtils.getStatus(openSessionResp.getCode(), openSessionResp.getMessage());
    }

    return AuthorityChecker.checkUser(username, password);
  }

  private TSStatus executeStatementForTableModel(
      final Statement statement, final String databaseName) {
    try {
      autoCreateDatabaseIfNecessary(databaseName);

      return Coordinator.getInstance()
          .executeForTableModel(
              shouldMarkAsPipeRequest.get() ? new PipeEnrichedStatement(statement) : statement,
              tableSqlParser,
              SESSION_MANAGER.getCurrSession(),
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                  SESSION_MANAGER.getCurrSession(), databaseName),
              "",
              LocalExecutionPlanner.getInstance().metadata,
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
          .status;
    } catch (final Exception e) {
      final Throwable rootCause = getRootCause(e);
      if (rootCause.getMessage() != null
          && rootCause
              .getMessage()
              .toLowerCase(Locale.ENGLISH)
              .contains(DATABASE_NOT_SET.toLowerCase(Locale.ENGLISH))) {
        autoCreateDatabaseIfNecessary(databaseName);

        // Retry after creating the database
        return Coordinator.getInstance()
            .executeForTableModel(
                shouldMarkAsPipeRequest.get() ? new PipeEnrichedStatement(statement) : statement,
                tableSqlParser,
                SESSION_MANAGER.getCurrSession(),
                SESSION_MANAGER.requestQueryId(),
                SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                    SESSION_MANAGER.getCurrSession(), databaseName),
                "",
                LocalExecutionPlanner.getInstance().metadata,
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
            .status;
      }

      // If the exception is not caused by database not set, throw it directly
      throw e;
    }
  }

  private void autoCreateDatabaseIfNecessary(final String database) {
    if (DataNodeTableCache.getInstance().isDatabaseExist(database)
        || !IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
      return;
    }

    AuthorityChecker.getAccessControl().checkCanCreateDatabase(username, database, userEntity);
    final TDatabaseSchema schema = new TDatabaseSchema(new TDatabaseSchema(database));
    schema.setIsTableModel(true);

    final CreateDBTask task = new CreateDBTask(schema, true);
    try {
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      final int statusCode = result.getStatusCode().getStatusCode();
      if (statusCode != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && statusCode != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        throw new PipeException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA,
                database,
                result.getStatusCode()));
      }
    } catch (final ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new PipeException(
          DataNodePipeMessages.AUTO_CREATE_DATABASE_FAILED_BECAUSE + e.getMessage());
    }
  }

  private TreeDatabaseCreationResult autoCreateTreeDatabaseIfNecessary(final String database) {
    if (database == null
        || LoadTsFileStatement.getDatabaseLevelByTreeDatabase(database) == null
        || !IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
      return TreeDatabaseCreationResult.SKIPPED;
    }
    if (autoCreatedTreeDatabases.contains(database)) {
      return TreeDatabaseCreationResult.CREATED_OR_EXISTED;
    }
    if (conflictedTreeDatabases.contains(database)) {
      return TreeDatabaseCreationResult.CONFLICTED;
    }

    try {
      final TSStatus status =
          AuthorityChecker.getAccessControl()
              .checkCanCreateDatabaseForTree(getUserEntity(), new PartialPath(database));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(status.getMessage());
      }

      final DatabaseSchemaStatement statement =
          new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
      statement.setDatabasePath(new PartialPath(database));
      statement.setEnablePrintExceptionLog(false);
      final DatabaseSchemaTask task = new DatabaseSchemaTask(statement);
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      final int statusCode = result.getStatusCode().getStatusCode();
      if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || statusCode == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        autoCreatedTreeDatabases.add(database);
        return TreeDatabaseCreationResult.CREATED_OR_EXISTED;
      }
      if (statusCode == TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        conflictedTreeDatabases.add(database);
        return TreeDatabaseCreationResult.CONFLICTED;
      }
      throw new PipeException(
          String.format(
              DataNodePipeMessages
                  .EXCEPTION_AUTO_CREATE_TREE_DATABASE_FAILED_ARG_STATUS_CODE_ARG_C6175C27,
              database,
              result.getStatusCode()));
    } catch (final IllegalPathException e) {
      throw new PipeException(
          String.format(
              DataNodePipeMessages.EXCEPTION_ILLEGAL_TREE_DATABASE_ARG_C805A990, database),
          e);
    } catch (final ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      final Throwable rootCause = getRootCause(e);
      final int errorCode;
      if (rootCause instanceof IoTDBException) {
        errorCode = ((IoTDBException) rootCause).getErrorCode();
      } else if (rootCause instanceof IoTDBRuntimeException) {
        errorCode = ((IoTDBRuntimeException) rootCause).getErrorCode();
      } else {
        errorCode = TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
      }
      if (errorCode == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        autoCreatedTreeDatabases.add(database);
        return TreeDatabaseCreationResult.CREATED_OR_EXISTED;
      }
      if (errorCode == TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        conflictedTreeDatabases.add(database);
        return TreeDatabaseCreationResult.CONFLICTED;
      }
      throw new PipeException(
          DataNodePipeMessages.AUTO_CREATE_DATABASE_FAILED_BECAUSE + e.getMessage());
    }
  }

  private TSStatus executeStatementForTreeModel(
      final Statement statement, final String databaseName) {
    if (autoCreateTreeDatabaseIfNecessary(databaseName) == TreeDatabaseCreationResult.CONFLICTED) {
      // Continue execution, but let partition analysis infer the receiver-side database.
      clearTreeDatabaseName(statement);
    }

    return Coordinator.getInstance()
        .executeForTreeModel(
            shouldMarkAsPipeRequest.get() ? new PipeEnrichedStatement(statement) : statement,
            SESSION_MANAGER.requestQueryId(),
            SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
            "",
            ClusterPartitionFetcher.getInstance(),
            ClusterSchemaFetcher.getInstance(),
            IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
            false,
            statement.isDebug())
        .status;
  }

  private IAuditEntity getUserEntity() {
    return userEntity != null
        ? userEntity
        : AuthorityChecker.createIAuditEntity(username, SESSION_MANAGER.getCurrSession());
  }

  private String getTreeDatabaseName(final Statement statement) {
    if (statement instanceof LoadTsFileStatement) {
      return ((LoadTsFileStatement) statement).getDatabase();
    }
    if (statement instanceof InsertBaseStatement) {
      return ((InsertBaseStatement) statement).getDatabaseName().orElse(null);
    }
    return null;
  }

  static void clearTreeDatabaseName(final Statement statement) {
    if (statement instanceof LoadTsFileStatement) {
      final LoadTsFileStatement loadTsFileStatement = (LoadTsFileStatement) statement;
      loadTsFileStatement.setDatabase(null);
      loadTsFileStatement.setDatabaseLevel(
          IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel());
    } else if (statement instanceof InsertBaseStatement) {
      clearTreeInsertDatabaseName((InsertBaseStatement) statement);
    }
  }

  private static void clearTreeInsertDatabaseName(final InsertBaseStatement statement) {
    statement.setDatabaseName(null);
    if (statement instanceof InsertRowsStatement) {
      for (final InsertBaseStatement childStatement :
          ((InsertRowsStatement) statement).getInsertRowStatementList()) {
        childStatement.setDatabaseName(null);
      }
    } else if (statement instanceof InsertRowsOfOneDeviceStatement) {
      for (final InsertBaseStatement childStatement :
          ((InsertRowsOfOneDeviceStatement) statement).getInsertRowStatementList()) {
        childStatement.setDatabaseName(null);
      }
    } else if (statement instanceof InsertMultiTabletsStatement) {
      for (final InsertBaseStatement childStatement :
          ((InsertMultiTabletsStatement) statement).getInsertTabletStatementList()) {
        childStatement.setDatabaseName(null);
      }
    }
  }

  private TSStatus executeStatementForTableModelWithPermissionCheck(
      final org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      final String databaseName) {
    try {
      final TSStatus status = loginIfNecessary();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      final TSStatus result =
          Coordinator.getInstance()
              .executeForTableModel(
                  shouldMarkAsPipeRequest.get() ? new PipeEnriched(statement) : statement,
                  tableSqlParser,
                  SESSION_MANAGER.getCurrSession(),
                  SESSION_MANAGER.requestQueryId(),
                  SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                      SESSION_MANAGER.getCurrSession(), databaseName),
                  "",
                  LocalExecutionPlanner.getInstance().metadata,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                  false,
                  statement.isDebug())
              .status;

      // Delete data & Update device attribute is itself idempotent
      // No strong need to handle the failure result
      if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failure status encountered while executing statement %s: %s",
            receiverId.get(),
            statement,
            result);
      }
      return result;
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          "Receiver id = %s: Exception encountered while executing statement %s: ",
          receiverId.get(),
          statement);
      return new TSStatus(TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public synchronized void handleExit() {
    clearSliceReqHandler();
    if (Objects.nonNull(configReceiverId.get())) {
      try {
        ClusterConfigTaskExecutor.getInstance().handlePipeConfigClientExit(configReceiverId.get());
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages.FAILED_TO_HANDLE_CONFIG_CLIENT_ID_EXIT, configReceiverId.get(), e);
      }
    }

    super.handleExit();
  }

  @Override
  protected void closeSession() {
    final IClientSession session = SESSION_MANAGER.getCurrSession();
    if (session != null) {
      SESSION_MANAGER.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
    }
    SESSION_MANAGER.removeCurrSession();
  }
}
