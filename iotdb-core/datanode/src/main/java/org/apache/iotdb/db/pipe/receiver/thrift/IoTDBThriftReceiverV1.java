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

package org.apache.iotdb.db.pipe.receiver.thrift;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReq;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiverV1;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaPlanReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.receiver.PipePlanToStatementVisitor;
import org.apache.iotdb.db.pipe.receiver.PipeStatementExceptionVisitor;
import org.apache.iotdb.db.pipe.receiver.PipeStatementTSStatusVisitor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IoTDBThriftReceiverV1 extends IoTDBFileReceiverV1 {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftReceiverV1.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String[] RECEIVER_FILE_BASE_DIRS = IOTDB_CONFIG.getPipeReceiverFileDirs();
  private static FolderManager folderManager = null;
  private static final PipeStatementTSStatusVisitor statusVisitor =
      new PipeStatementTSStatusVisitor();
  private static final PipeStatementExceptionVisitor exceptionVisitor =
      new PipeStatementExceptionVisitor();

  static {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(RECEIVER_FILE_BASE_DIRS), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipe receiver file folders allocation strategy because all disks of folders are full.",
          e);
    }
  }

  @Override
  public synchronized TPipeTransferResp receive(TPipeTransferReq req) {
    try {
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeRequestType.valueOf(rawRequestType)) {
          case DATANODE_HANDSHAKE:
            return handleTransferHandshake(
                PipeTransferDataNodeHandshakeReq.fromTPipeTransferReq(req));
          case HANDSHAKE_V1:
            return handleTransferHandshakeV1(PipeTransferHandshakeV1Req.fromTPipeTransferReq(req));
          case HANDSHAKE_V2:
            return handleTransferHandshakeV2(PipeTransferHandshakeV2Req.fromTPipeTransferReq(req));
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
                req instanceof AirGapPseudoTPipeTransferRequest);
          case TRANSFER_TS_FILE_SEAL:
            return handleTransferFileSeal(PipeTransferTsFileSealReq.fromTPipeTransferReq(req));
          case TRANSFER_SCHEMA_PLAN:
            return handleTransferSchemaPlan(PipeTransferPlanNodeReq.fromTPipeTransferReq(req));
          case TRANSFER_SCHEMA_SNAPSHOT_PIECE:
            return handleTransferFilePiece(
                PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest);
          case TRANSFER_SCHEMA_SNAPSHOT_SEAL:
            return handleTransferFileSeal(
                PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req));
            // Config Requests will first be received by the DataNode receiver,
            // then transferred to configNode receiver to execute.
          case CONFIGNODE_HANDSHAKE:
          case TRANSFER_CONFIG_PLAN:
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            return handleTransferConfigPlan(req);
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
      LOGGER.warn("Unknown PipeRequestType, response status = {}.", status);
      return new TPipeTransferResp(status);
    } catch (IOException e) {
      String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn(error);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeTransferResp handleTransferHandshakeV1(PipeTransferHandshakeV1Req req) {
    if (!CommonDescriptor.getInstance()
        .getConfig()
        .getTimestampPrecision()
        .equals(req.getTimestampPrecision())) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              String.format(
                  "IoTDB receiver's timestamp precision %s, "
                      + "connector's timestamp precision %s. Validation fails.",
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
                  req.getTimestampPrecision()));
      LOGGER.warn("Handshake failed, response status = {}.", status);
      return new TPipeTransferResp(status);
    }

    receiverId.set(RECEIVER_ID_GENERATOR.incrementAndGet());

    // clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          Files.delete(receiverFileDirWithIdSuffix.get().toPath());
          LOGGER.info(
              "Original receiver file dir {} was deleted.",
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "Failed to delete original receiver file dir {}, because {}.",
              receiverFileDirWithIdSuffix.get().getPath(),
              e.getMessage());
        }
      } else {
        LOGGER.info(
            "Original receiver file dir {} is not existed. No need to delete.",
            receiverFileDirWithIdSuffix.get().getPath());
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      LOGGER.info("Current receiver file dir is null. No need to delete.");
    }

    // get next receiver file base dir by folder manager
    if (Objects.isNull(folderManager)) {
      LOGGER.error(
          "Failed to init pipe receiver file folder manager because all disks of folders are full.");
      return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
    }
    String receiverFileBaseDir;
    try {
      receiverFileBaseDir = folderManager.getNextFolder();
    } catch (DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipe receiver file folder because all disks of folders are full.", e);
      return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
    }

    // create a new receiver file dir
    final File newReceiverDir = new File(receiverFileBaseDir, Long.toString(receiverId.get()));
    if (!newReceiverDir.exists()) {
      if (newReceiverDir.mkdirs()) {
        LOGGER.info("Receiver file dir {} was created.", newReceiverDir.getPath());
      } else {
        LOGGER.error("Failed to create receiver file dir {}.", newReceiverDir.getPath());
      }
    }
    receiverFileDirWithIdSuffix.set(newReceiverDir);

    LOGGER.info(
        "Handshake successfully, receiver id = {}, receiver file dir = {}.",
        receiverId.get(),
        newReceiverDir.getPath());
    return new TPipeTransferResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeTransferResp handleTransferHandshakeV2(PipeTransferHandshakeV2Req req)
      throws IOException {
    // Reject to handshake if the receiver can not take clusterId from config node.
    final String clusterIdFromConfigNode = PipeAgent.runtime().getClusterIdIfPossible();
    if (clusterIdFromConfigNode == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              "Receiver can not get clusterId from config node.");
      LOGGER.warn("Handshake failed, response status = {}.", status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the request does not contain sender's clusterId.
    final String clusterIdFromHandshakeRequest =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID);
    if (clusterIdFromHandshakeRequest == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR, "Handshake request does not contain clusterId.");
      LOGGER.warn("Handshake failed, response status = {}.", status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the receiver and sender are from the same cluster.
    if (Objects.equals(clusterIdFromConfigNode, clusterIdFromHandshakeRequest)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              String.format(
                  "Receiver and sender are from the same cluster %s.",
                  clusterIdFromHandshakeRequest));
      LOGGER.warn("Handshake failed, response status = {}.", status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the request does not contain timestampPrecision.
    final String timestampPrecision =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION);
    if (timestampPrecision == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              "Handshake request does not contain timestampPrecision.");
      LOGGER.warn("Handshake failed, response status = {}.", status);
      return new TPipeTransferResp(status);
    }

    // Handle the handshake request as a v1 request.
    return handleTransferHandshakeV1(
        PipeTransferHandshakeV1Req.toTPipeTransferReq(timestampPrecision));
  }

  private TPipeTransferResp handleTransferTabletInsertNode(
      PipeTransferTabletInsertNodeReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    InsertBaseStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletBinary(PipeTransferTabletBinaryReq req) {
    InsertBaseStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletRaw(PipeTransferTabletRawReq req) {
    InsertTabletStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatementAndClassifyExceptions(statement));
  }

  private TPipeTransferResp handleTransferTabletBatch(PipeTransferTabletBatchReq req) {
    final Pair<InsertRowsStatement, InsertMultiTabletsStatement> statementPair =
        req.constructStatements();
    return new TPipeTransferResp(
        getPriorStatus(
            Stream.of(
                    statementPair.getLeft().isEmpty()
                        ? RpcUtils.SUCCESS_STATUS
                        : executeStatementAndClassifyExceptions(statementPair.getLeft()),
                    statementPair.getRight().isEmpty()
                        ? RpcUtils.SUCCESS_STATUS
                        : executeStatementAndClassifyExceptions(statementPair.getRight()))
                .collect(Collectors.toList())));
  }

  private TSStatus getPriorStatus(List<TSStatus> tsStatusList) {
    List<Integer> prioritySequence =
        Collections.unmodifiableList(
            Arrays.asList(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode(),
                TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
                TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode()));
    TSStatus resultStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    for (TSStatus status : tsStatusList) {
      if (!prioritySequence.contains(status.getCode())) {
        return status;
      }
      if (prioritySequence.indexOf(status.getCode())
          > prioritySequence.indexOf(resultStatus.getCode())) {
        resultStatus.setCode(status.getCode());
        resultStatus.setMessage(status.getMessage());
      }
    }
    return resultStatus;
  }

  @Override
  protected String getReceiverFileBaseDir() throws DiskSpaceInsufficientException {
    // Get next receiver file base dir by folder manager
    if (Objects.isNull(folderManager)) {
      return null;
    }
    return folderManager.getNextFolder();
  }

  @Override
  protected TSStatus loadFile(PipeTransferFileSealReq req, String fileAbsolutePath)
      throws FileNotFoundException {
    return req instanceof PipeTransferTsFileSealReq
        ? loadTsFile(fileAbsolutePath)
        : loadSchemaSnapShot(fileAbsolutePath);
  }

  private TSStatus loadTsFile(String fileAbsolutePath) throws FileNotFoundException {
    final LoadTsFileStatement statement = new LoadTsFileStatement(fileAbsolutePath);

    statement.setDeleteAfterLoad(true);
    statement.setVerifySchema(true);
    statement.setAutoCreateDatabase(false);

    return executeStatementAndClassifyExceptions(statement);
  }

  private TSStatus loadSchemaSnapShot(String fileAbsolutePath) {
    // TODO
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TPipeTransferResp handleTransferSchemaPlan(PipeTransferPlanNodeReq req) {
    // TODO: parse exception and status for alter logical view node
    return req.getPlanNode() instanceof AlterLogicalViewNode
        ? new TPipeTransferResp(
            ClusterConfigTaskExecutor.getInstance()
                .alterLogicalViewByPipe((AlterLogicalViewNode) req.getPlanNode()))
        : new TPipeTransferResp(
            executeStatementAndClassifyExceptions(
                new PipePlanToStatementVisitor().process(req.getPlanNode(), null)));
  }

  private TPipeTransferResp handleTransferConfigPlan(TPipeTransferReq req) {
    return ClusterConfigTaskExecutor.getInstance().handleTransferConfigPlan(req);
  }

  private TSStatus executeStatementAndClassifyExceptions(Statement statement) {
    try {
      TSStatus result = executeStatement(statement);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      } else {
        LOGGER.warn(
            "Failure status encountered while executing statement {}: {}", statement, result);
        return statement.accept(statusVisitor, result);
      }
    } catch (Exception e) {
      LOGGER.warn("Exception encountered while executing statement {}: ", statement, e);
      return statement.accept(exceptionVisitor, e);
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
            .execute(
                statement,
                SessionManager.getInstance().requestQueryId(),
                new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault().getId()),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    return result.status;
  }

  @Override
  public IoTDBConnectorRequestVersion getVersion() {
    return IoTDBConnectorRequestVersion.VERSION_1;
  }
}
