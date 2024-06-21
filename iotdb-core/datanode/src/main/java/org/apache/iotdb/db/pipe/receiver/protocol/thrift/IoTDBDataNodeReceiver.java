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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.connector.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiver;
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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.receiver.visitor.PipePlanToStatementVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementExceptionVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementPatternParseVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTSStatusVisitor;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementToBatchVisitor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IoTDBDataNodeReceiver extends IoTDBFileReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeReceiver.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String[] RECEIVER_FILE_BASE_DIRS = IOTDB_CONFIG.getPipeReceiverFileDirs();
  private static FolderManager folderManager = null;

  public static final PipePlanToStatementVisitor PLAN_TO_STATEMENT_VISITOR =
      new PipePlanToStatementVisitor();
  private static final PipeStatementTSStatusVisitor STATEMENT_STATUS_VISITOR =
      new PipeStatementTSStatusVisitor();
  private static final PipeStatementExceptionVisitor STATEMENT_EXCEPTION_VISITOR =
      new PipeStatementExceptionVisitor();
  private static final PipeStatementPatternParseVisitor STATEMENT_PATTERN_PARSE_VISITOR =
      new PipeStatementPatternParseVisitor();
  private final PipeStatementToBatchVisitor batchVisitor = new PipeStatementToBatchVisitor();

  // Used for data transfer: confignode (cluster A) -> datanode (cluster B) -> confignode (cluster
  // B).
  // If connection from confignode (cluster A) to datanode (cluster B) is lost, the receiver in
  // confignode (cluster B) needs to handle the thread exit using configReceiverId generated by
  // datanode (cluster B).
  private static final AtomicLong CONFIG_RECEIVER_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicReference<String> configReceiverId = new AtomicReference<>();

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
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeRequestType.valueOf(rawRequestType)) {
          case HANDSHAKE_DATANODE_V1:
            return handleTransferHandshakeV1(
                PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req));
          case HANDSHAKE_DATANODE_V2:
            return handleTransferHandshakeV2(
                PipeTransferDataNodeHandshakeV2Req.fromTPipeTransferReq(req));
          case TRANSFER_TABLET_INSERT_NODE:
            return handleTransferTabletInsertNode(
                PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req));
          case TRANSFER_TABLET_RAW:
            return handleTransferTabletRaw(PipeTransferTabletRawReq.fromTPipeTransferReq(req));
          case TRANSFER_TABLET_BINARY:
            return handleTransferTabletBinary(
                PipeTransferTabletBinaryReq.fromTPipeTransferReq(req));
          case TRANSFER_TABLET_BATCH:
            return handleTransferTabletBatch(PipeTransferTabletBatchReq.fromTPipeTransferReq(req));
          case TRANSFER_TS_FILE_PIECE:
            return handleTransferFilePiece(
                PipeTransferTsFilePieceReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest,
                true);
          case TRANSFER_TS_FILE_SEAL:
            return handleTransferFileSealV1(PipeTransferTsFileSealReq.fromTPipeTransferReq(req));
          case TRANSFER_TS_FILE_PIECE_WITH_MOD:
            return handleTransferFilePiece(
                PipeTransferTsFilePieceWithModReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest,
                false);
          case TRANSFER_TS_FILE_SEAL_WITH_MOD:
            return handleTransferFileSealV2(
                PipeTransferTsFileSealWithModReq.fromTPipeTransferReq(req));
          case TRANSFER_SCHEMA_PLAN:
            return handleTransferSchemaPlan(PipeTransferPlanNodeReq.fromTPipeTransferReq(req));
          case TRANSFER_SCHEMA_SNAPSHOT_PIECE:
            return handleTransferFilePiece(
                PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest,
                false);
          case TRANSFER_SCHEMA_SNAPSHOT_SEAL:
            return handleTransferFileSealV2(
                PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req));
          case HANDSHAKE_CONFIGNODE_V1:
          case HANDSHAKE_CONFIGNODE_V2:
          case TRANSFER_CONFIG_PLAN:
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            // Config requests will first be received by the DataNode receiver,
            // then transferred to ConfigNode receiver to execute.
            return handleTransferConfigPlan(req);
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
      throws FileNotFoundException {
    return loadTsFile(fileAbsolutePath);
  }

  @Override
  protected TSStatus loadFileV2(
      final PipeTransferFileSealReqV2 req, final List<String> fileAbsolutePaths)
      throws IOException, IllegalPathException {
    return req instanceof PipeTransferTsFileSealWithModReq
        // TsFile's absolute path will be the second element
        ? loadTsFile(fileAbsolutePaths.get(1))
        : loadSchemaSnapShot(req.getParameters(), fileAbsolutePaths);
  }

  private TSStatus loadTsFile(final String fileAbsolutePath) throws FileNotFoundException {
    final LoadTsFileStatement statement = new LoadTsFileStatement(fileAbsolutePath);

    statement.setDeleteAfterLoad(true);
    statement.setVerifySchema(true);
    statement.setAutoCreateDatabase(false);

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
    final IoTDBPipePattern pattern =
        new IoTDBPipePattern(parameters.get(ColumnHeaderConstant.PATH_PATTERN));

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
      final TSStatus result = executeStatement(statement);
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

  private TSStatus executeStatement(Statement statement) {
    if (statement == null) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR, "Execute null statement.");
    }

    statement = new PipeEnrichedStatement(statement);

    final ExecutionResult result =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                SessionManager.getInstance().requestQueryId(),
                new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    return result.status;
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
