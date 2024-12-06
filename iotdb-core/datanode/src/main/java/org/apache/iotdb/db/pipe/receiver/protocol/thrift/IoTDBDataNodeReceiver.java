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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferSliceReqHandler;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiver;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeReceiverMetrics;
import org.apache.iotdb.db.pipe.receiver.visitor.PipePlanToStatementVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementDataTypeConvertExecutionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementExceptionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementPatternParseVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTSStatusVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementToBatchVisitor;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
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

import java.io.File;
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

import static org.apache.iotdb.db.exception.metadata.DatabaseNotSetException.DATABASE_NOT_SET;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.getRootCause;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR_CHAR;

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
  private static final PipeStatementPatternParseVisitor STATEMENT_PATTERN_PARSE_VISITOR =
      new PipeStatementPatternParseVisitor();
  private final PipeStatementDataTypeConvertExecutionVisitor
      statementDataTypeConvertExecutionVisitor =
          new PipeStatementDataTypeConvertExecutionVisitor(this::executeStatementForTreeModel);
  private final PipeStatementToBatchVisitor batchVisitor = new PipeStatementToBatchVisitor();

  // Used for data transfer: confignode (cluster A) -> datanode (cluster B) -> confignode (cluster
  // B).
  // If connection from confignode (cluster A) to datanode (cluster B) is lost, the receiver in
  // confignode (cluster B) needs to handle the thread exit using configReceiverId generated by
  // datanode (cluster B).
  private static final AtomicLong CONFIG_RECEIVER_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicReference<String> configReceiverId = new AtomicReference<>();

  private final PipeTransferSliceReqHandler sliceReqHandler = new PipeTransferSliceReqHandler();

  private final SqlParser relationalSqlParser = new SqlParser();

  private static final Set<String> ALREADY_CREATED_DATABASES = ConcurrentHashMap.newKeySet();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  static {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(RECEIVER_FILE_BASE_DIRS), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (final DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipe receiver file folders allocation strategy because all disks of folders are full.",
          e);
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
          sliceReqHandler.clear();
        }
        switch (requestType) {
          case HANDSHAKE_DATANODE_V1:
            {
              try {
                return handleTransferHandshakeV1(
                    PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req));
              } finally {
                PipeDataNodeReceiverMetrics.getInstance()
                    .recordHandshakeDatanodeV1Timer(System.nanoTime() - startTime);
              }
            }
          case HANDSHAKE_DATANODE_V2:
            {
              try {
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
          case TRANSFER_SCHEMA_PLAN:
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
              try {
                return receive(PipeTransferCompressedReq.fromTPipeTransferReq(req));
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
          "Receiver id = {}: Unknown PipeRequestType, response status = {}.",
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      final String error =
          String.format("Exception %s encountered while handling request %s.", e.getMessage(), req);
      LOGGER.warn("Receiver id = {}: {}", receiverId.get(), error, e);
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
                        : executeStatementAndAddRedirectInfo(statementPair.getLeft()),
                    statementPair.getRight().isEmpty()
                        ? RpcUtils.SUCCESS_STATUS
                        : executeStatementAndAddRedirectInfo(statementPair.getRight()))
                .collect(Collectors.toList())));
  }

  private TPipeTransferResp handleTransferTabletBatchV2(final PipeTransferTabletBatchReqV2 req) {
    final List<InsertBaseStatement> statementSet = req.constructStatements();
    return new TPipeTransferResp(
        PipeReceiverStatusHandler.getPriorStatus(
            (statementSet.isEmpty()
                    ? Stream.of(RpcUtils.SUCCESS_STATUS)
                    : statementSet.stream().map(this::executeStatementAndAddRedirectInfo))
                .collect(Collectors.toList())));
  }

  @Override
  protected String getClusterId() {
    return IoTDBDescriptor.getInstance().getConfig().getClusterId();
  }

  @Override
  protected String getReceiverFileBaseDir() throws DiskSpaceInsufficientException {
    // Get next receiver file base dir by folder manager
    return Objects.isNull(folderManager) ? null : folderManager.getNextFolder();
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
    if (Objects.nonNull(dataBaseName)) {
      throw new PipeException(
          "Async load tsfile does not support table model tsfile. Given database name: "
              + dataBaseName);
    }

    final String loadActiveListeningPipeDir = IOTDB_CONFIG.getLoadActiveListeningPipeDir();

    for (final String absolutePath : absolutePaths) {
      if (absolutePath == null) {
        continue;
      }
      final File sourceFile = new File(absolutePath);
      if (!Objects.equals(
          loadActiveListeningPipeDir, sourceFile.getParentFile().getAbsolutePath())) {
        FileUtils.moveFileWithMD5Check(sourceFile, new File(loadActiveListeningPipeDir));
      }
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus loadTsFileSync(final String dataBaseName, final String fileAbsolutePath)
      throws FileNotFoundException {
    final LoadTsFileStatement statement = new LoadTsFileStatement(fileAbsolutePath);
    statement.setDeleteAfterLoad(true);
    statement.setVerifySchema(true);
    statement.setAutoCreateDatabase(false);

    statement.setModel(
        dataBaseName != null
            ? LoadTsFileConfigurator.MODEL_TABLE_VALUE
            : LoadTsFileConfigurator.MODEL_TREE_VALUE);
    statement.setDatabase(dataBaseName);

    return executeStatementAndClassifyExceptions(statement);
  }

  private TSStatus loadSchemaSnapShot(
      final Map<String, String> parameters, final List<String> fileAbsolutePaths)
      throws IllegalPathException, IOException {
    final SRStatementGenerator generator =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(fileAbsolutePaths.get(0)),
            fileAbsolutePaths.size() > 1 ? Paths.get(fileAbsolutePaths.get(1)) : null,
            new PartialPath(parameters.get(ColumnHeaderConstant.DATABASE)));
    final Set<StatementType> executionTypes =
        PipeSchemaRegionSnapshotEvent.getStatementTypeSet(
            parameters.get(ColumnHeaderConstant.TYPE));
    final IoTDBTreePattern pattern =
        new IoTDBTreePattern(parameters.get(ColumnHeaderConstant.PATH_PATTERN));

    // Clear to avoid previous exceptions
    batchVisitor.clear();
    final List<TSStatus> results = new ArrayList<>();
    while (generator.hasNext()) {
      final Statement originalStatement = generator.next();
      if (!executionTypes.contains(originalStatement.getType())) {
        continue;
      }

      // The statements do not contain AlterLogicalViewStatements
      // Here we apply the statements as many as possible
      // Even if there are failed statements
      STATEMENT_PATTERN_PARSE_VISITOR
          .process(originalStatement, pattern)
          .flatMap(parsedStatement -> batchVisitor.process(parsedStatement, null))
          .ifPresent(statement -> results.add(executeStatementAndClassifyExceptions(statement)));
    }
    batchVisitor.getRemainBatches().stream()
        .filter(Optional::isPresent)
        .forEach(statement -> results.add(executeStatementAndClassifyExceptions(statement.get())));
    return PipeReceiverStatusHandler.getPriorStatus(results);
  }

  private TPipeTransferResp handleTransferSchemaPlan(final PipeTransferPlanNodeReq req) {
    // We may be able to skip the alter logical view's exception parsing because
    // the "AlterLogicalViewNode" is itself idempotent
    return req.getPlanNode() instanceof AlterLogicalViewNode
        ? new TPipeTransferResp(
            ClusterConfigTaskExecutor.getInstance()
                .alterLogicalViewByPipe((AlterLogicalViewNode) req.getPlanNode()))
        : new TPipeTransferResp(
            executeStatementAndClassifyExceptions(
                PLAN_TO_STATEMENT_VISITOR.process(req.getPlanNode(), null)));
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
    final boolean isInorder = sliceReqHandler.receiveSlice(pipeTransferSliceReq);
    if (!isInorder) {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_SLICE_OUT_OF_ORDER,
              "Slice request is out of order, please check the request sequence."));
    }
    final Optional<TPipeTransferReq> req = sliceReqHandler.makeReqIfComplete();
    if (!req.isPresent()) {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.SUCCESS_STATUS,
              "Slice received, waiting for more slices to complete the request."));
    }
    // sliceReqHandler will be cleared in the receive(req) method
    return receive(req.get());
  }

  /**
   * For {@link InsertRowsStatement} and {@link InsertMultiTabletsStatement}, the returned {@link
   * TSStatus} will use sub-status to record the endpoint for redirection. Each sub-status records
   * the redirection endpoint for one device path, and the order is the same as the order of the
   * device paths in the statement. However, this order is not guaranteed to be the same as in the
   * request. So for each sub-status which needs to redirect, we record the device path using the
   * message field.
   */
  private TSStatus executeStatementAndAddRedirectInfo(final InsertBaseStatement statement) {
    final TSStatus result = executeStatementAndClassifyExceptions(statement);

    if (result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        && result.getSubStatusSize() > 0) {
      final List<PartialPath> devicePaths;
      if (statement instanceof InsertRowsStatement) {
        devicePaths = ((InsertRowsStatement) statement).getDevicePaths();
      } else if (statement instanceof InsertMultiTabletsStatement) {
        devicePaths = ((InsertMultiTabletsStatement) statement).getDevicePaths();
      } else {
        LOGGER.warn(
            "Receiver id = {}: Unsupported statement type {} for redirection.",
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
            "Receiver id = {}: The number of device paths is not equal to sub-status in statement {}: {}.",
            receiverId.get(),
            statement,
            result);
      }
    }

    return result;
  }

  private TSStatus executeStatementAndClassifyExceptions(final Statement statement) {
    try {
      final TSStatus result =
          executeStatementWithPermissionCheckAndRetryOnDataTypeMismatch(statement);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return result;
      } else {
        LOGGER.warn(
            "Receiver id = {}: Failure status encountered while executing statement {}: {}",
            receiverId.get(),
            statement,
            result);
        return statement.accept(STATEMENT_STATUS_VISITOR, result);
      }
    } catch (final Exception e) {
      LOGGER.warn(
          "Receiver id = {}: Exception encountered while executing statement {}: ",
          receiverId.get(),
          statement,
          e);
      return statement.accept(STATEMENT_EXCEPTION_VISITOR, e);
    }
  }

  private TSStatus executeStatementWithPermissionCheckAndRetryOnDataTypeMismatch(
      final Statement statement) {
    if (statement == null) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR, "Execute null statement.");
    }

    // Permission check
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (clientSession == null || !clientSession.isLogin()) {
      final BasicOpenSessionResp openSessionResp =
          SESSION_MANAGER.login(
              SESSION_MANAGER.getCurrSession(),
              username,
              password,
              ZoneId.systemDefault().toString(),
              SessionManager.CURRENT_RPC_VERSION,
              IoTDBConstant.ClientVersion.V_1_0);
      if (openSessionResp.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Receiver id = {}: Failed to open session, username = {}, response = {}.",
            receiverId.get(),
            username,
            openSessionResp);
        return RpcUtils.getStatus(openSessionResp.getCode(), openSessionResp.getMessage());
      }
      clientSession = SESSION_MANAGER.getCurrSession();
    }
    final TSStatus permissionCheckStatus =
        AuthorityChecker.checkAuthority(statement, clientSession);
    if (permissionCheckStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Receiver id = {}: Failed to check authority for statement {}, username = {}, response = {}.",
          receiverId.get(),
          statement.getType().name(),
          username,
          permissionCheckStatus);
      return RpcUtils.getStatus(
          permissionCheckStatus.getCode(), permissionCheckStatus.getMessage());
    }

    // Judge which model the statement belongs to
    final boolean isTableModelStatement;
    final String dataBaseName;
    if (statement instanceof LoadTsFileStatement
        && ((LoadTsFileStatement) statement)
            .getModel()
            .equals(LoadTsFileConfigurator.MODEL_TABLE_VALUE)) {
      isTableModelStatement = true;
      dataBaseName = ((LoadTsFileStatement) statement).getDatabase();
    } else if (statement instanceof InsertBaseStatement
        && ((InsertBaseStatement) statement).isWriteToTable()) {
      isTableModelStatement = true;
      dataBaseName =
          ((InsertBaseStatement) statement).getDatabaseName().isPresent()
              ? ((InsertBaseStatement) statement).getDatabaseName().get()
              : null;
    } else {
      isTableModelStatement = false;
      dataBaseName = null;
    }

    // Real execution of the statement
    final TSStatus status =
        isTableModelStatement
            ? executeStatementForTableModel(statement, dataBaseName)
            : executeStatementForTreeModel(statement);

    // The following code is used to handle the data type mismatch exception
    // Data type conversion is not supported for table model statements
    if (isTableModelStatement) {
      return status;
    }
    // Try to convert data type if the statement is a tree model statement
    // and the status code is not success
    return shouldConvertDataTypeOnTypeMismatch
            && ((statement instanceof InsertBaseStatement
                    && ((InsertBaseStatement) statement).hasFailedMeasurements())
                || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        ? statement.accept(statementDataTypeConvertExecutionVisitor, status).orElse(status)
        : status;
  }

  private TSStatus executeStatementForTableModel(Statement statement, String dataBaseName) {
    try {
      autoCreateDatabaseIfNecessary(dataBaseName);

      return Coordinator.getInstance()
          .executeForTableModel(
              new PipeEnrichedStatement(statement),
              relationalSqlParser,
              SESSION_MANAGER.getCurrSession(),
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                  SESSION_MANAGER.getCurrSession(), dataBaseName),
              "",
              LocalExecutionPlanner.getInstance().metadata,
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
          .status;
    } catch (final Exception e) {
      ALREADY_CREATED_DATABASES.remove(dataBaseName);

      final Throwable rootCause = getRootCause(e);
      if (rootCause.getMessage() != null
          && rootCause
              .getMessage()
              .toLowerCase(Locale.ENGLISH)
              .contains(DATABASE_NOT_SET.toLowerCase(Locale.ENGLISH))) {
        autoCreateDatabaseIfNecessary(dataBaseName);

        // Retry after creating the database
        return Coordinator.getInstance()
            .executeForTableModel(
                new PipeEnrichedStatement(statement),
                relationalSqlParser,
                SESSION_MANAGER.getCurrSession(),
                SESSION_MANAGER.requestQueryId(),
                SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                    SESSION_MANAGER.getCurrSession(), dataBaseName),
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
    if (ALREADY_CREATED_DATABASES.contains(database)) {
      return;
    }

    final TDatabaseSchema schema =
        new TDatabaseSchema(new TDatabaseSchema(ROOT + PATH_SEPARATOR_CHAR + database));
    schema.setIsTableModel(true);

    final CreateDBTask task = new CreateDBTask(schema, true);
    try {
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(
            String.format(
                "Auto create database failed: %s, status code: %s",
                database, result.getStatusCode()));
      }
    } catch (final ExecutionException | InterruptedException e) {
      throw new PipeException("Auto create database failed because: " + e.getMessage());
    }

    ALREADY_CREATED_DATABASES.add(database);
  }

  private TSStatus executeStatementForTreeModel(final Statement statement) {
    return Coordinator.getInstance()
        .executeForTreeModel(
            new PipeEnrichedStatement(statement),
            SESSION_MANAGER.requestQueryId(),
            SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
            "",
            ClusterPartitionFetcher.getInstance(),
            ClusterSchemaFetcher.getInstance(),
            IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
        .status;
  }

  @Override
  public synchronized void handleExit() {
    if (Objects.nonNull(configReceiverId.get())) {
      try {
        ClusterConfigTaskExecutor.getInstance().handlePipeConfigClientExit(configReceiverId.get());
      } catch (final Exception e) {
        LOGGER.warn("Failed to handle config client (id = {}) exit", configReceiverId.get(), e);
      }
    }

    super.handleExit();
  }
}
