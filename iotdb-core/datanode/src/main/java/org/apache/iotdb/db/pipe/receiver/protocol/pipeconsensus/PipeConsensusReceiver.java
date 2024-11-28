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

package org.apache.iotdb.db.pipe.receiver.protocol.pipeconsensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.response.PipeConsensusTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusDeleteNodeReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusReceiverMetrics;
import org.apache.iotdb.db.pipe.event.common.tsfile.aggregator.TsFileInsertionPointCounter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils.generateTsFileResource;

public class PipeConsensusReceiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiver.class);
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final long PIPE_CONSENSUS_RECEIVER_MAX_WAITING_TIME_IN_MS =
      (long) IOTDB_CONFIG.getConnectionTimeoutInMS()
          / 6
          * IOTDB_CONFIG.getIotConsensusV2PipelineSize();
  private static final long CLOSE_TSFILE_WRITER_MAX_WAIT_TIME_IN_MS = 5000;
  private static final long RETRY_WAIT_TIME = 500;
  private final RequestExecutor requestExecutor;
  private final PipeConsensus pipeConsensus;
  private final ConsensusGroupId consensusGroupId;
  private final ConsensusPipeName consensusPipeName;
  private final List<String> receiverBaseDirsName;
  // Used to buffer TsFile when transfer TsFile asynchronously.
  private final PipeConsensusTsFileWriterPool pipeConsensusTsFileWriterPool;
  private final AtomicReference<File> receiverFileDirWithIdSuffix = new AtomicReference<>();
  private final PipeConsensusReceiverMetrics pipeConsensusReceiverMetrics;
  private final FolderManager folderManager;

  public PipeConsensusReceiver(
      PipeConsensus pipeConsensus,
      ConsensusGroupId consensusGroupId,
      ConsensusPipeName consensusPipeName) {
    this.pipeConsensus = pipeConsensus;
    this.consensusGroupId = consensusGroupId;
    this.pipeConsensusReceiverMetrics = new PipeConsensusReceiverMetrics(this);
    this.consensusPipeName = consensusPipeName;

    // Each pipeConsensusReceiver has its own base directories. for example, a default dir path is
    // data/datanode/system/pipe/consensus/receiver/__consensus.{consensusGroupId}_{leaderDataNodeId}_{followerDataNodeId}
    receiverBaseDirsName =
        Arrays.asList(
            IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2ReceiverFileDirs());

    try {
      this.folderManager =
          new FolderManager(receiverBaseDirsName, DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (Exception e) {
      LOGGER.error(
          "Fail to create pipeConsensus receiver file folders allocation strategy because all disks of folders are full.",
          e);
      throw new RuntimeException(e);
    }

    try {
      initiateTsFileBufferFolder();
      this.pipeConsensusTsFileWriterPool =
          new PipeConsensusTsFileWriterPool(
              consensusPipeName, receiverFileDirWithIdSuffix.get().getPath());
    } catch (Exception e) {
      LOGGER.error("Fail to initiate file buffer folder, Error msg: {}", e.getMessage());
      throw new RuntimeException(e);
    }
    this.requestExecutor =
        new RequestExecutor(pipeConsensusReceiverMetrics, pipeConsensusTsFileWriterPool);
    MetricService.getInstance().addMetricSet(pipeConsensusReceiverMetrics);
  }

  /**
   * This method cannot be set to synchronize. Receive events can be concurrent since reqBuffer but
   * load event must be synchronized.
   */
  public TPipeConsensusTransferResp receive(final TPipeConsensusTransferReq req) {
    long startNanos = System.nanoTime();
    // PreCheck: if there are these cases: read-only; null impl; inactive impl, etc. The receiver
    // will reject synchronization.
    TPipeConsensusTransferResp resp = preCheckForReceiver(req);
    if (resp != null) {
      return resp;
    }

    final short rawRequestType = req.getType();
    if (PipeConsensusRequestType.isValidatedRequestType(rawRequestType)) {
      switch (PipeConsensusRequestType.valueOf(rawRequestType)) {
        case TRANSFER_TS_FILE_PIECE:
        case TRANSFER_TS_FILE_PIECE_WITH_MOD:
          // Just take a place in requestExecutor's buffer, the further seal request will remove
          // its place from buffer.
          requestExecutor.onRequest(req, true, false);
          resp = loadEvent(req);
          break;
        case TRANSFER_TS_FILE_SEAL:
        case TRANSFER_TS_FILE_SEAL_WITH_MOD:
          // TODO: check memory when logging WAL(in further version)
          resp = requestExecutor.onRequest(req, false, true);
          break;
        case TRANSFER_DELETION:
        case TRANSFER_TABLET_BINARY:
        case TRANSFER_TABLET_INSERT_NODE:
          // TODO: support batch transfer(in further version)
        case TRANSFER_TABLET_BATCH:
        default:
          resp = requestExecutor.onRequest(req, false, false);
          break;
      }
      // update receive an event's duration
      long durationNanos = System.nanoTime() - startNanos;
      pipeConsensusReceiverMetrics.recordReceiveEventTimer(durationNanos);
      return resp;
    }
    // Unknown request type, which means the request can not be handled by this receiver,
    // maybe the version of the receiver is not compatible with the sender
    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("PipeConsensus Unknown PipeRequestType %s.", rawRequestType));
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("PipeConsensus Unknown PipeRequestType, response status = {}.", status);
    }
    return new TPipeConsensusTransferResp(status);
  }

  private TPipeConsensusTransferResp preCheckForReceiver(final TPipeConsensusTransferReq req) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    PipeConsensusServerImpl impl = pipeConsensus.getImpl(groupId);

    if (impl == null) {
      String message =
          String.format(
              "PipeConsensus-PipeName-%s: unexpected consensusGroupId %s",
              consensusPipeName, groupId);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), message));
    }
    if (impl.isReadOnly()) {
      String message =
          String.format(
              "PipeConsensus-PipeName-%s: fail to receive because system is read-only.",
              consensusPipeName);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), message));
    }

    return null;
  }

  private TPipeConsensusTransferResp loadEvent(final TPipeConsensusTransferReq req) {
    // synchronized load event, ensured by upper caller's lock.
    try {
      final short rawRequestType = req.getType();
      if (PipeConsensusRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeConsensusRequestType.valueOf(rawRequestType)) {
          case TRANSFER_TABLET_INSERT_NODE:
            return handleTransferTabletInsertNode(
                PipeConsensusTabletInsertNodeReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TABLET_BINARY:
            return handleTransferTabletBinary(
                PipeConsensusTabletBinaryReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_DELETION:
            return handleTransferDeletion(
                PipeConsensusDeleteNodeReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TS_FILE_PIECE:
            return handleTransferFilePiece(
                PipeConsensusTsFilePieceReq.fromTPipeConsensusTransferReq(req), true);
          case TRANSFER_TS_FILE_SEAL:
            return handleTransferFileSeal(
                PipeConsensusTsFileSealReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TS_FILE_PIECE_WITH_MOD:
            return handleTransferFilePiece(
                PipeConsensusTsFilePieceReq.fromTPipeConsensusTransferReq(req), false);
          case TRANSFER_TS_FILE_SEAL_WITH_MOD:
            return handleTransferFileSealWithMods(
                PipeConsensusTsFileSealWithModReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TABLET_BATCH:
            LOGGER.info("PipeConsensus transfer batch hasn't been implemented yet.");
          default:
            break;
        }
      }
      // Unknown request type, which means the request can not be handled by this receiver,
      // maybe the version of the receiver is not compatible with the sender
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TYPE_ERROR,
              String.format("Unknown PipeConsensusRequestType %s.", rawRequestType));
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Unknown PipeRequestType, response status = {}.",
          consensusPipeName,
          status);
      return new TPipeConsensusTransferResp(status);
    } catch (Exception e) {
      final String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn("PipeConsensus-PipeName-{}: {}", consensusPipeName, error, e);
      return new TPipeConsensusTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeConsensusTransferResp handleTransferTabletInsertNode(
      final PipeConsensusTabletInsertNodeReq req) throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final InsertNode insertNode = req.getInsertNode();
    insertNode.markAsGeneratedByRemoteConsensusLeader();
    insertNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TPipeConsensusTransferResp(impl.writeOnFollowerReplica(insertNode));
  }

  private TPipeConsensusTransferResp handleTransferTabletBinary(
      final PipeConsensusTabletBinaryReq req) throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final InsertNode insertNode = req.convertToInsertNode();
    insertNode.markAsGeneratedByRemoteConsensusLeader();
    insertNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TPipeConsensusTransferResp(impl.writeOnFollowerReplica(insertNode));
  }

  private TPipeConsensusTransferResp handleTransferDeletion(final PipeConsensusDeleteNodeReq req)
      throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final AbstractDeleteDataNode planNode = req.getDeleteDataNode();
    planNode.markAsGeneratedByRemoteConsensusLeader();
    planNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TPipeConsensusTransferResp(impl.writeOnFollowerReplica(planNode));
  }

  private TPipeConsensusTransferResp handleTransferFilePiece(
      final PipeConsensusTransferFilePieceReq req, final boolean isSingleFile) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "PipeConsensus-PipeName-{}: starting to receive tsFile pieces", consensusPipeName);
    }
    long startBorrowTsFileWriterNanos = System.nanoTime();
    PipeConsensusTsFileWriter tsFileWriter =
        pipeConsensusTsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    pipeConsensusReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);

    try {
      updateWritingFileIfNeeded(tsFileWriter, req.getFileName(), isSingleFile);
      final File writingFile = tsFileWriter.getWritingFile();
      final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

      if (isWritingFileOffsetNonCorrect(tsFileWriter, req.getStartWritingOffset())) {
        if (!writingFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
          // If the file is a tsFile, then the content will not be changed for a specific
          // filename. However, for other files (mod, snapshot, etc.) the content varies for the
          // same name in different times, then we must rewrite the file to apply the newest
          // version.
          writingFileWriter.setLength(0);
        }

        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_OFFSET_RESET,
                String.format(
                    "Request sender to reset file reader's offset from %s to %s.",
                    req.getStartWritingOffset(), writingFileWriter.length()));
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: File offset reset requested by receiver, response status = {}.",
            consensusPipeName,
            status);
        return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(
            status, writingFileWriter.length());
      }

      long endPreCheckNanos = System.nanoTime();
      pipeConsensusReceiverMetrics.recordTsFilePiecePreCheckTime(
          endPreCheckNanos - startPreCheckNanos);
      writingFileWriter.write(req.getFilePiece());
      pipeConsensusReceiverMetrics.recordTsFilePieceWriteTime(System.nanoTime() - endPreCheckNanos);
      return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (Exception e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to write file piece from req {}.",
          consensusPipeName,
          req,
          e);
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to write file piece, because %s", e.getMessage()));
      try {
        return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(
            status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
      } catch (IOException ex) {
        return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(status);
      }
    }
  }

  private TPipeConsensusTransferResp handleTransferFileSeal(final PipeConsensusTsFileSealReq req) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("PipeConsensus-PipeName-{}: starting to receive tsFile seal", consensusPipeName);
    }
    long startBorrowTsFileWriterNanos = System.nanoTime();
    PipeConsensusTsFileWriter tsFileWriter =
        pipeConsensusTsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    pipeConsensusReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    try {
      if (isWritingFileNonAvailable(tsFileWriter)) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file, because writing file %s is not available.", writingFile));
        LOGGER.warn(status.getMessage());
        return new TPipeConsensusTransferResp(status);
      }

      final TPipeConsensusTransferResp resp =
          checkFinalFileSeal(tsFileWriter, req.getFileName(), req.getFileLength());
      if (Objects.nonNull(resp)) {
        return resp;
      }

      final String fileAbsolutePath = writingFile.getAbsolutePath();

      // Sync here is necessary to ensure that the data is written to the disk. Or data region may
      // load the file before the data is written to the disk and cause unexpected behavior after
      // system restart. (e.g., empty file in data region's data directory)
      writingFileWriter.getFD().sync();
      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.close();
      tsFileWriter.setWritingFileWriter(null);

      // writingFile will be deleted after load if no exception occurs
      tsFileWriter.setWritingFile(null);

      long endPreCheckNanos = System.nanoTime();
      pipeConsensusReceiverMetrics.recordTsFileSealPreCheckTimer(
          endPreCheckNanos - startPreCheckNanos);
      updateWritePointCountMetrics(req.getPointCount(), fileAbsolutePath);
      final TSStatus status =
          loadFileToDataRegion(
              fileAbsolutePath,
              ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
      pipeConsensusReceiverMetrics.recordTsFileSealLoadTimer(System.nanoTime() - endPreCheckNanos);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // if transfer success, disk buffer will be released.
        tsFileWriter.returnSelf(consensusPipeName);
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Seal file {} successfully.",
            consensusPipeName,
            fileAbsolutePath);
      } else {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: Failed to seal file {}, because {}.",
            consensusPipeName,
            fileAbsolutePath,
            status.getMessage());
      }
      return new TPipeConsensusTransferResp(status);
    } catch (IOException e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {} from req {}.",
          consensusPipeName,
          writingFile,
          req,
          e);
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } catch (LoadFileException e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to load file {} from req {}.",
          consensusPipeName,
          writingFile,
          req,
          e);
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.LOAD_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod (if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(tsFileWriter, false);
      deleteCurrentWritingFile(tsFileWriter);
    }
  }

  private TPipeConsensusTransferResp handleTransferFileSealWithMods(
      final PipeConsensusTsFileSealWithModReq req) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "PipeConsensus-PipeName-{}: starting to receive tsFile seal with mods",
          consensusPipeName);
    }
    long startBorrowTsFileWriterNanos = System.nanoTime();
    PipeConsensusTsFileWriter tsFileWriter =
        pipeConsensusTsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    pipeConsensusReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final List<File> files =
        req.getFileNames().stream()
            .map(fileName -> new File(receiverFileDirWithIdSuffix.get(), fileName))
            .collect(Collectors.toList());
    try {
      if (isWritingFileNonAvailable(tsFileWriter)) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, because writing file %s is not available.",
                    req.getFileNames(), writingFile));
        LOGGER.warn(status.getMessage());
        return new TPipeConsensusTransferResp(status);
      }

      // Any of the transferred files cannot be empty, or else the receiver
      // will not sense this file because no pieces are sent
      for (int i = 0; i < req.getFileNames().size(); ++i) {
        final TPipeConsensusTransferResp resp =
            i == req.getFileNames().size() - 1
                ? checkFinalFileSeal(
                    tsFileWriter, req.getFileNames().get(i), req.getFileLengths().get(i))
                : checkNonFinalFileSeal(
                    tsFileWriter,
                    files.get(i),
                    req.getFileNames().get(i),
                    req.getFileLengths().get(i));
        if (Objects.nonNull(resp)) {
          return resp;
        }
      }

      // Sync here is necessary to ensure that the data is written to the disk. Or data region may
      // load the file before the data is written to the disk and cause unexpected behavior after
      // system restart. (e.g., empty file in data region's data directory)
      writingFileWriter.getFD().sync();
      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.close();
      tsFileWriter.setWritingFileWriter(null);

      // WritingFile will be deleted after load if no exception occurs
      tsFileWriter.setWritingFile(null);

      final List<String> fileAbsolutePaths =
          files.stream().map(File::getAbsolutePath).collect(Collectors.toList());

      long endPreCheckNanos = System.nanoTime();
      pipeConsensusReceiverMetrics.recordTsFileSealPreCheckTimer(
          endPreCheckNanos - startPreCheckNanos);
      final String tsFileAbsolutePath = fileAbsolutePaths.get(1);
      updateWritePointCountMetrics(req.getPointCounts().get(1), tsFileAbsolutePath);
      final TSStatus status =
          loadFileToDataRegion(
              tsFileAbsolutePath,
              ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
      pipeConsensusReceiverMetrics.recordTsFileSealLoadTimer(System.nanoTime() - endPreCheckNanos);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // if transfer success, disk buffer will be released.
        tsFileWriter.returnSelf(consensusPipeName);
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Seal file with mods {} successfully.",
            consensusPipeName,
            fileAbsolutePaths);
      } else {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: Failed to seal file {}, status is {}.",
            consensusPipeName,
            fileAbsolutePaths,
            status);
      }
      return new TPipeConsensusTransferResp(status);
    } catch (Exception e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {} from req {}.",
          consensusPipeName,
          files,
          req,
          e);
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod(if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(tsFileWriter, false);
      // Clear the directory instead of only deleting the referenced files in seal request
      // to avoid previously undeleted file being redundant when transferring multi files
      IoTDBReceiverAgent.cleanPipeReceiverDir(receiverFileDirWithIdSuffix.get());
    }
  }

  private TPipeConsensusTransferResp checkNonFinalFileSeal(
      final PipeConsensusTsFileWriter tsFileWriter,
      final File file,
      final String fileName,
      final long fileLength)
      throws IOException {
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    if (!file.exists()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s, the file does not exist.", fileName));
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {}, because the file does not exist.",
          consensusPipeName,
          fileName);
      return new TPipeConsensusTransferResp(status);
    }

    if (fileLength != file.length()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {} when check non final seal, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusPipeName,
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeConsensusTransferResp(status);
    }

    return null;
  }

  private TSStatus loadFileToDataRegion(String filePath, ProgressIndex progressIndex)
      throws IOException, LoadFileException {
    DataRegion region =
        StorageEngine.getInstance().getDataRegion(((DataRegionId) consensusGroupId));
    if (region != null) {
      TsFileResource resource = generateTsFileResource(filePath, progressIndex);
      region.loadNewTsFile(resource, true, false);
    } else {
      // Data region is null indicates that dr has been removed or migrated. In those cases, there
      // is no need to replicate data. we just return success to avoid leader keeping retry
      LOGGER.info(
          "PipeConsensus-PipeName-{}: skip load tsfile-{} when sealing, because this region has been removed or migrated.",
          consensusPipeName,
          filePath);
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private void updateWritePointCountMetrics(
      final long writePointCountGivenByReq, final String tsFileAbsolutePath) {
    if (writePointCountGivenByReq >= 0) {
      updateWritePointCountMetrics(writePointCountGivenByReq);
      return;
    }

    // If the point count in the req is not given,
    // we will read the actual point count from the TsFile.
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "PipeConsensus-PipeName-{}: The point count of TsFile {} is not given by sender, "
              + "will read actual point count from TsFile.",
          consensusPipeName,
          tsFileAbsolutePath);
    }

    try (final TsFileInsertionPointCounter counter =
        new TsFileInsertionPointCounter(new File(tsFileAbsolutePath), null)) {
      updateWritePointCountMetrics(counter.count());
    } catch (IOException e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to read TsFile when counting points: {}.",
          consensusPipeName,
          tsFileAbsolutePath,
          e);
    }
  }

  private void updateWritePointCountMetrics(long writePointCount) {
    Optional.ofNullable(
            StorageEngine.getInstance().getDataRegion(((DataRegionId) consensusGroupId)))
        .ifPresent(
            dataRegion ->
                dataRegion
                    .getNonSystemDatabaseName()
                    .ifPresent(
                        databaseName ->
                            LoadTsFileManager.updateWritePointCountMetrics(
                                dataRegion, databaseName, writePointCount, true)));
  }

  private TsFileResource generateTsFileResource(String filePath, ProgressIndex progressIndex)
      throws IOException {
    final File tsFile = new File(filePath);

    final TsFileResource tsFileResource = new TsFileResource(tsFile);
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      TsFileResourceUtils.updateTsFileResource(reader, tsFileResource);
    }

    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    tsFileResource.setProgressIndex(progressIndex);
    tsFileResource.setGeneratedByPipeConsensus(true);
    tsFileResource.serialize();
    return tsFileResource;
  }

  private boolean isWritingFileNonAvailable(PipeConsensusTsFileWriter tsFileWriter) {
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final boolean isWritingFileAvailable =
        writingFile != null && writingFile.exists() && writingFileWriter != null;
    if (!isWritingFileAvailable) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: Writing file {} is not available. "
              + "Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.",
          consensusPipeName,
          writingFile,
          writingFile == null,
          writingFile != null && writingFile.exists(),
          writingFileWriter == null);
    }
    return !isWritingFileAvailable;
  }

  private TPipeConsensusTransferResp checkFinalFileSeal(
      final PipeConsensusTsFileWriter tsFileWriter, final String fileName, final long fileLength)
      throws IOException {
    final File writingFile = tsFileWriter.getWritingFile();
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    if (!isFileExistedAndNameCorrect(tsFileWriter, fileName)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because writing file is %s.", fileName, writingFile));
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {}, because writing file is {}.",
          consensusPipeName,
          fileName,
          writingFile);
      return new TPipeConsensusTransferResp(status);
    }

    if (isWritingFileOffsetNonCorrect(tsFileWriter, fileLength)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to seal file {} when check final seal file, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusPipeName,
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeConsensusTransferResp(status);
    }

    return null;
  }

  private boolean isFileExistedAndNameCorrect(
      PipeConsensusTsFileWriter tsFileWriter, String fileName) {
    final File writingFile = tsFileWriter.getWritingFile();
    return writingFile != null && writingFile.getName().equals(fileName);
  }

  private boolean isWritingFileOffsetNonCorrect(
      PipeConsensusTsFileWriter tsFileWriter, final long offset) throws IOException {
    final File writingFile = tsFileWriter.getWritingFile();
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Writing file {}'s offset is {}, but request sender's offset is {}.",
          consensusPipeName,
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return !offsetCorrect;
  }

  private void closeCurrentWritingFileWriter(
      PipeConsensusTsFileWriter tsFileWriter, boolean fsyncAfterClose) {
    if (tsFileWriter.getWritingFileWriter() != null) {
      try {
        tsFileWriter.getWritingFileWriter().close();
        if (fsyncAfterClose) {
          tsFileWriter.getWritingFileWriter().getFD().sync();
        }
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Current writing file writer {} was closed.",
            consensusPipeName,
            tsFileWriter.getWritingFile() == null
                ? "null"
                : tsFileWriter.getWritingFile().getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: Failed to close current writing file writer {}, because {}.",
            consensusPipeName,
            tsFileWriter.getWritingFile() == null
                ? "null"
                : tsFileWriter.getWritingFile().getPath(),
            e.getMessage(),
            e);
      }
      tsFileWriter.setWritingFileWriter(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: Current writing file writer is null. No need to close.",
            consensusPipeName.toString());
      }
    }
  }

  private void deleteFile(File file) {
    if (file.exists()) {
      try {
        FileUtils.delete(file);
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Original writing file {} was deleted.",
            consensusPipeName,
            file.getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: Failed to delete original writing file {}, because {}.",
            consensusPipeName,
            file.getPath(),
            e.getMessage(),
            e);
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: Original file {} is not existed. No need to delete.",
            consensusPipeName,
            file.getPath());
      }
    }
  }

  private void deleteCurrentWritingFile(PipeConsensusTsFileWriter tsFileWriter) {
    if (tsFileWriter.getWritingFile() != null) {
      deleteFile(tsFileWriter.getWritingFile());
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: Current writing file is null. No need to delete.",
            consensusPipeName.toString());
      }
    }
  }

  private void updateWritingFileIfNeeded(
      final PipeConsensusTsFileWriter tsFileWriter,
      final String fileName,
      final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(tsFileWriter, fileName)) {
      return;
    }

    LOGGER.info(
        "PipeConsensus-PipeName-{}: Writing file {} is not existed or name is not correct, try to create it. "
            + "Current writing file is {}.",
        consensusPipeName,
        fileName,
        tsFileWriter.getWritingFile() == null ? "null" : tsFileWriter.getWritingFile().getPath());

    closeCurrentWritingFileWriter(tsFileWriter, !isSingleFile);
    // If there are multiple files we can not delete the current file
    // instead they will be deleted after seal request
    if (tsFileWriter.getWritingFile() != null && isSingleFile) {
      deleteCurrentWritingFile(tsFileWriter);
    }

    // Make sure receiver file dir exists
    // This may be useless, because receiver file dir is created when receiver is initiated. just in
    // case.
    if (!receiverFileDirWithIdSuffix.get().exists()) {
      if (receiverFileDirWithIdSuffix.get().mkdirs()) {
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Receiver file dir {} was created.",
            consensusPipeName,
            receiverFileDirWithIdSuffix.get().getPath());
      } else {
        LOGGER.error(
            "PipeConsensus-PipeName-{}: Failed to create receiver file dir {}.",
            consensusPipeName,
            receiverFileDirWithIdSuffix.get().getPath());
      }
    }
    // Every tsFileWriter has its own writing path.
    // 1 Thread --> 1 connection --> 1 tsFileWriter --> 1 path
    tsFileWriter.setWritingFile(new File(tsFileWriter.getLocalWritingDirPath(), fileName));
    tsFileWriter.setWritingFileWriter(new RandomAccessFile(tsFileWriter.getWritingFile(), "rw"));
    LOGGER.info(
        "PipeConsensus-PipeName-{}: Writing file {} was created. Ready to write file pieces.",
        consensusPipeName,
        tsFileWriter.getWritingFile().getPath());
  }

  private String getReceiverFileBaseDir() throws DiskSpaceInsufficientException {
    // Get next receiver file base dir by folder manager
    return Objects.isNull(folderManager) ? null : folderManager.getNextFolder();
  }

  private void initiateTsFileBufferFolder() throws DiskSpaceInsufficientException, IOException {
    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
          LOGGER.info(
              "PipeConsensus-PipeName-{}: Original receiver file dir {} was deleted successfully.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "PipeConsensus-PipeName-{}: Failed to delete original receiver file dir {}, because {}.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath(),
              e.getMessage(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-PipeName-{}: Original receiver file dir {} is not existed. No need to delete.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      LOGGER.debug(
          "PipeConsensus-PipeName-{}: Current receiver file dir is null. No need to delete.",
          consensusPipeName.toString());
    }

    // initiate receiverFileDirWithIdSuffix
    String receiverFileBaseDir;
    try {
      receiverFileBaseDir = getReceiverFileBaseDir();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to init pipeConsensus receiver file folder manager because all disks of folders are full.",
          e);
      throw e;
    }

    if (Objects.isNull(receiverFileBaseDir)) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to get pipeConsensus receiver file base directory, because your folderManager is null. May because the disk is full.",
          consensusPipeName.toString());
      throw new DiskSpaceInsufficientException(receiverBaseDirsName);
    }
    // Create a new receiver file dir
    final File newReceiverDir = new File(receiverFileBaseDir, consensusPipeName.toString());
    // Check whether systemDir exists in case of system concurrently exit when receiver try to make
    // new dirs.
    final File systemDir = new File(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
    if (!systemDir.exists()) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to create receiver file dir {}. Because parent system dir have been deleted due to system concurrently exit.",
          consensusPipeName,
          newReceiverDir.getPath());
      throw new IOException(
          String.format(
              "PipeConsensus-PipeName-%s: Failed to create receiver file dir %s. Because parent system dir have been deleted due to system concurrently exit.",
              consensusPipeName, newReceiverDir.getPath()));
    }
    // Remove exists dir
    if (newReceiverDir.exists()) {
      FileUtils.deleteDirectory(newReceiverDir);
      LOGGER.info(
          "PipeConsensus-PipeName-{}: Origin receiver file dir {} was deleted.",
          consensusPipeName,
          newReceiverDir.getPath());
    }
    if (!newReceiverDir.mkdirs()) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to create receiver file dir {}. May because authority or dir already exists etc.",
          consensusPipeName,
          newReceiverDir.getPath());
      throw new IOException(
          String.format(
              "PipeConsensus-PipeName-%s: Failed to create receiver file dir %s. May because authority or dir already exists etc.",
              consensusPipeName, newReceiverDir.getPath()));
    }
    receiverFileDirWithIdSuffix.set(newReceiverDir);
  }

  public PipeConsensusRequestVersion getVersion() {
    return PipeConsensusRequestVersion.VERSION_1;
  }

  public synchronized void handleExit() {
    // Clear the tsFileWriters
    pipeConsensusTsFileWriterPool.handleExit(consensusPipeName);

    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
          LOGGER.info(
              "PipeConsensus-PipeName-{}: Receiver exit: Original receiver file dir {} was deleted.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "PipeConsensus-PipeName-{}: Receiver exit: Delete original receiver file dir {} error.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-PipeName-{}: Receiver exit: Original receiver file dir {} does not exist. No need to delete.",
              consensusPipeName,
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: Receiver exit: Original receiver file dir is null. No need to delete.",
            consensusPipeName.toString());
      }
    }

    // remove metric
    MetricService.getInstance().removeMetricSet(pipeConsensusReceiverMetrics);

    LOGGER.info(
        "PipeConsensus-PipeName-{}: Receiver exit: Receiver exited.", consensusPipeName.toString());
  }

  private static class PipeConsensusTsFileWriterPool {
    private final Lock lock = new ReentrantLock();
    private final List<PipeConsensusTsFileWriter> pipeConsensusTsFileWriterPool = new ArrayList<>();
    private final ConsensusPipeName consensusPipeName;

    public PipeConsensusTsFileWriterPool(
        ConsensusPipeName consensusPipeName, String receiverBasePath) throws IOException {
      this.consensusPipeName = consensusPipeName;
      for (int i = 0; i < IOTDB_CONFIG.getIotConsensusV2PipelineSize(); i++) {
        PipeConsensusTsFileWriter tsFileWriter =
            new PipeConsensusTsFileWriter(i, consensusPipeName);
        tsFileWriter.setFilePath(receiverBasePath);
        pipeConsensusTsFileWriterPool.add(tsFileWriter);
      }
    }

    @SuppressWarnings("java:S3655")
    public PipeConsensusTsFileWriter borrowCorrespondingWriter(TCommitId commitId) {
      Optional<PipeConsensusTsFileWriter> tsFileWriter =
          pipeConsensusTsFileWriterPool.stream()
              .filter(
                  item -> Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
              .findFirst();

      // If the TsFileInsertionEvent is first using tsFileWriter, we will find the first available
      // buffer for it.
      if (!tsFileWriter.isPresent()) {
        // We should synchronously find the idle writer to avoid concurrency issues.
        try {
          lock.lock();
          // We need to check tsFileWriter.isPresent() here. Since there may be both retry-sent
          // tsfile
          // events and real-time-sent tsfile events, causing the receiver's tsFileWriter load to
          // exceed IOTDB_CONFIG.getPipeConsensusPipelineSize().
          while (!tsFileWriter.isPresent()) {
            tsFileWriter =
                pipeConsensusTsFileWriterPool.stream().filter(item -> !item.isUsed()).findFirst();
            Thread.sleep(RETRY_WAIT_TIME);
          }
          tsFileWriter.get().setUsed(true);
          tsFileWriter.get().setCommitIdOfCorrespondingHolderEvent(commitId);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              "PipeConsensus: receiver thread get interrupted when waiting for borrowing tsFileWriter.");
        } finally {
          lock.unlock();
        }
      }

      return tsFileWriter.get();
    }

    public void handleExit(ConsensusPipeName consensusPipeName) {
      pipeConsensusTsFileWriterPool.forEach(
          tsFileWriter -> {
            // Wait until tsFileWriter is not used by TsFileInsertionEvent or timeout.
            long currentTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - currentTime
                    < CLOSE_TSFILE_WRITER_MAX_WAIT_TIME_IN_MS
                && tsFileWriter.isUsed()) {
              try {
                Thread.sleep(RETRY_WAIT_TIME);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn(
                    "PipeConsensus-PipeName-{}: receiver thread get interrupted when exiting.",
                    consensusPipeName.toString());
                // avoid infinite loop
                break;
              }
            }
            tsFileWriter.closeSelf(consensusPipeName);
            tsFileWriter.returnSelf(consensusPipeName);
          });
    }
  }

  private static class PipeConsensusTsFileWriter {
    private final ConsensusPipeName consensusPipeName;
    private final int index;
    private String localWritingDirPath;
    // whether this buffer is used. this will be updated when first transfer tsFile piece or
    // when transfer seal.
    private boolean isUsed = false;
    // If isUsed is true, this variable will be set to the TCommitId of holderEvent
    private TCommitId commitIdOfCorrespondingHolderEvent;
    private File writingFile;
    private RandomAccessFile writingFileWriter;

    public PipeConsensusTsFileWriter(int index, ConsensusPipeName consensusPipeName) {
      this.index = index;
      this.consensusPipeName = consensusPipeName;
    }

    public void setFilePath(String receiverBasePath) throws IOException {
      this.localWritingDirPath = receiverBasePath + File.separator + index;
      File tsFileWriterDirectory = new File(this.localWritingDirPath);
      // Remove exists dir
      if (tsFileWriterDirectory.exists()) {
        FileUtils.deleteDirectory(tsFileWriterDirectory);
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Origin receiver tsFileWriter-{} file dir {} was deleted.",
            consensusPipeName,
            index,
            tsFileWriterDirectory.getPath());
      }
      if (!tsFileWriterDirectory.mkdirs()) {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: Failed to create receiver tsFileWriter-{} file dir {}. May because authority or dir already exists etc.",
            consensusPipeName,
            index,
            tsFileWriterDirectory.getPath());
        throw new IOException(
            String.format(
                "PipeConsensus-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir %s. May because authority or dir already exists etc.",
                consensusPipeName, index, tsFileWriterDirectory.getPath()));
      }
    }

    public String getLocalWritingDirPath() {
      return localWritingDirPath;
    }

    public File getWritingFile() {
      return writingFile;
    }

    public void setWritingFile(File writingFile) {
      this.writingFile = writingFile;
    }

    public RandomAccessFile getWritingFileWriter() {
      return writingFileWriter;
    }

    public void setWritingFileWriter(RandomAccessFile writingFileWriter) {
      this.writingFileWriter = writingFileWriter;
    }

    public TCommitId getCommitIdOfCorrespondingHolderEvent() {
      return commitIdOfCorrespondingHolderEvent;
    }

    public void setCommitIdOfCorrespondingHolderEvent(
        TCommitId commitIdOfCorrespondingHolderEvent) {
      this.commitIdOfCorrespondingHolderEvent = commitIdOfCorrespondingHolderEvent;
    }

    public boolean isUsed() {
      return isUsed;
    }

    public void setUsed(boolean used) {
      isUsed = used;
    }

    public void returnSelf(ConsensusPipeName consensusPipeName) {
      this.isUsed = false;
      this.commitIdOfCorrespondingHolderEvent = null;
      LOGGER.info(
          "PipeConsensus-PipeName-{}: tsFileWriter-{} returned self",
          consensusPipeName.toString(),
          index);
    }

    public void closeSelf(ConsensusPipeName consensusPipeName) {
      // close file writer
      if (writingFileWriter != null) {
        try {
          writingFileWriter.close();
          LOGGER.info(
              "PipeConsensus-PipeName-{}: TsFileWriter-{} exit: Writing file writer was closed.",
              consensusPipeName.toString(),
              index);
        } catch (Exception e) {
          LOGGER.warn(
              "PipeConsensus-PipeName-{}: TsFileWriter-{} exit: Close Writing file writer error.",
              consensusPipeName,
              index,
              e);
        }
        setWritingFileWriter(null);
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-PipeName-{}: TsFileWriter-{} exit: Writing file writer is null. No need to close.",
              consensusPipeName.toString(),
              index);
        }
      }

      // close file
      if (writingFile != null) {
        try {
          FileUtils.delete(writingFile);
          LOGGER.info(
              "PipeConsensus-PipeName-{}: TsFileWriter exit: Writing file {} was deleted.",
              consensusPipeName,
              writingFile.getPath());
        } catch (Exception e) {
          LOGGER.warn(
              "PipeConsensus-PipeName-{}: TsFileWriter exit: Delete writing file {} error.",
              consensusPipeName,
              writingFile.getPath(),
              e);
        }
        setWritingFile(null);
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-PipeName-{}: TsFileWriter exit: Writing file is null. No need to delete.",
              consensusPipeName.toString());
        }
      }
    }
  }

  /**
   * An executor component to ensure all events sent from connector can be loaded in sequence,
   * although events can arrive receiver in a random sequence.
   */
  private class RequestExecutor {
    // An ordered set that buffers transfer requests' TCommitId, whose length is not larger than
    // PIPE_CONSENSUS_PIPELINE_SIZE.
    // Here we use set is to avoid duplicate events being received in some special cases
    private final TreeSet<RequestMeta> reqExecutionOrderBuffer;
    private final Lock lock;
    private final Condition condition;
    private final PipeConsensusReceiverMetrics metric;
    private final PipeConsensusTsFileWriterPool tsFileWriterPool;
    private final AtomicInteger WALEventCount = new AtomicInteger(0);
    private final AtomicInteger tsFileEventCount = new AtomicInteger(0);
    private long onSyncedCommitIndex = 0;
    private int connectorRebootTimes = 0;
    private int pipeTaskRestartTimes = 0;

    public RequestExecutor(
        PipeConsensusReceiverMetrics metric, PipeConsensusTsFileWriterPool tsFileWriterPool) {
      this.reqExecutionOrderBuffer =
          new TreeSet<>(
              Comparator.comparingInt(RequestMeta::getDataNodeRebootTimes)
                  .thenComparingInt(RequestMeta::getPipeTaskRestartTimes)
                  .thenComparingLong(RequestMeta::getCommitIndex));
      this.lock = new ReentrantLock();
      this.condition = lock.newCondition();
      this.metric = metric;
      this.tsFileWriterPool = tsFileWriterPool;
    }

    private void onSuccess(TCommitId commitId, boolean isTransferTsFileSeal) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: process no.{} event successfully!",
          consensusPipeName,
          commitId);
      RequestMeta curMeta = reqExecutionOrderBuffer.pollFirst();
      onSyncedCommitIndex = commitId.getCommitIndex();
      // update metric, notice that curMeta is never null.
      if (isTransferTsFileSeal) {
        tsFileEventCount.decrementAndGet();
        metric.recordReceiveTsFileTimer(System.nanoTime() - curMeta.getStartApplyNanos());
      } else {
        WALEventCount.decrementAndGet();
        metric.recordReceiveWALTimer(System.nanoTime() - curMeta.getStartApplyNanos());
      }
    }

    private TPipeConsensusTransferResp onRequest(
        final TPipeConsensusTransferReq req,
        final boolean isTransferTsFilePiece,
        final boolean isTransferTsFileSeal) {
      long startAcquireLockNanos = System.nanoTime();
      lock.lock();
      try {
        long startDispatchNanos = System.nanoTime();
        metric.recordAcquireExecutorLockTimer(startDispatchNanos - startAcquireLockNanos);

        TCommitId tCommitId = req.getCommitId();
        RequestMeta requestMeta = new RequestMeta(tCommitId);
        LOGGER.info(
            "PipeConsensus-PipeName-{}: start to receive no.{} event",
            consensusPipeName,
            tCommitId);
        // if a req is deprecated, we will discard it
        // This case may happen in this scenario: leader has transferred {1,2} and is intending to
        // transfer {3, 4, 5, 6}. And in one moment, follower has received {4, 5, 6}, {3} is still
        // transferring due to some network latency.
        // At this time, leader restarts, and it will resend {3, 4, 5, 6} with incremental
        // rebootTimes. If the {3} sent before the leader restart arrives after the follower
        // receives
        // the request with incremental rebootTimes, the {3} sent before the leader restart needs to
        // be discarded.
        if (tCommitId.getDataNodeRebootTimes() < connectorRebootTimes) {
          final TSStatus status =
              new TSStatus(
                  RpcUtils.getStatus(
                      TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                      "PipeConsensus receiver received a deprecated request, which may be sent before the connector restart. Consider to discard it"));
          LOGGER.info(
              "PipeConsensus-PipeName-{}: received a deprecated request, which may be sent before the connector restart. Consider to discard it",
              consensusPipeName);
          return new TPipeConsensusTransferResp(status);
        }
        // Similarly, check pipeTask restartTimes
        if (tCommitId.getDataNodeRebootTimes() == connectorRebootTimes
            && tCommitId.getPipeTaskRestartTimes() < pipeTaskRestartTimes) {
          final TSStatus status =
              new TSStatus(
                  RpcUtils.getStatus(
                      TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                      "PipeConsensus receiver received a deprecated request, which may be sent before the pipe task restart. Consider to discard it"));
          LOGGER.info(
              "PipeConsensus-PipeName-{}: received a deprecated request, which may be sent before the pipe task restart. Consider to discard it",
              consensusPipeName);
          return new TPipeConsensusTransferResp(status);
        }
        // Judge whether connector has rebooted or not, if the rebootTimes increases compared to
        // connectorRebootTimes, need to reset receiver because connector has been restarted.
        if (tCommitId.getDataNodeRebootTimes() > connectorRebootTimes) {
          resetWithNewestRebootTime(tCommitId.getDataNodeRebootTimes(), condition);
        }
        // Similarly, check pipeTask restartTimes
        if (tCommitId.getPipeTaskRestartTimes() > pipeTaskRestartTimes) {
          resetWithNewestRestartTime(tCommitId.getPipeTaskRestartTimes(), condition);
        }
        // update metric
        if (isTransferTsFilePiece && !reqExecutionOrderBuffer.contains(requestMeta)) {
          // only update tsFileEventCount when tsFileEvent is first enqueue.
          tsFileEventCount.incrementAndGet();
        }
        if (!isTransferTsFileSeal && !isTransferTsFilePiece) {
          WALEventCount.incrementAndGet();
        }
        reqExecutionOrderBuffer.add(requestMeta);

        // TsFilePieceTransferEvent will not enter further procedure, it just holds a place in
        // buffer. Only after the corresponding sealing event is processed, this event can be
        // dequeued.
        if (isTransferTsFilePiece) {
          long startApplyNanos = System.nanoTime();
          metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
          requestMeta.setStartApplyNanos(startApplyNanos);
          return null;
        }

        if (reqExecutionOrderBuffer.size() >= IOTDB_CONFIG.getIotConsensusV2PipelineSize()
            && !reqExecutionOrderBuffer.first().equals(requestMeta)) {
          // If reqBuffer is full and current thread do not hold the reqBuffer's peek, this req
          // is not supposed to be processed. So current thread should notify the corresponding
          // threads to process the peek.
          condition.signalAll();
        }

        // Polling to process
        while (true) {
          if (reqExecutionOrderBuffer.first().equals(requestMeta)
              && tCommitId.getCommitIndex() == onSyncedCommitIndex + 1) {
            long startApplyNanos = System.nanoTime();
            metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
            requestMeta.setStartApplyNanos(startApplyNanos);
            // If current req is supposed to be process, load this event through
            // DataRegionStateMachine.
            TPipeConsensusTransferResp resp = loadEvent(req);

            // Only when event apply is successful and what is transmitted is not TsFilePiece, req
            // will be removed from the buffer and onSyncedCommitIndex will be updated. Because pipe
            // will transfer multi reqs with same commitId in a single TsFileInsertionEvent, only
            // when the last seal req is applied, we can discard this event.
            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              onSuccess(tCommitId, isTransferTsFileSeal);
            }
            return resp;
          }

          if (reqExecutionOrderBuffer.size() >= IOTDB_CONFIG.getIotConsensusV2PipelineSize()
              && reqExecutionOrderBuffer.first().equals(requestMeta)) {
            long startApplyNanos = System.nanoTime();
            metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
            requestMeta.setStartApplyNanos(startApplyNanos);
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            TPipeConsensusTransferResp resp = loadEvent(req);

            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              onSuccess(tCommitId, isTransferTsFileSeal);
              // signal all other reqs that may wait for this event
              condition.signalAll();
            }
            return resp;
          } else {
            // if the req is not supposed to be processed and reqBuffer is not full, current thread
            // should wait until reqBuffer is full, which indicates the receiver has received all
            // the requests from the connector without duplication or leakage.
            try {
              boolean timeout =
                  !condition.await(
                      PIPE_CONSENSUS_RECEIVER_MAX_WAITING_TIME_IN_MS, TimeUnit.MILLISECONDS);

              // If some reqs find the buffer no longer contains their requestMeta after jumping out
              // from condition.await, it may indicate that during their wait, some reqs with newer
              // pipeTaskStartTimes or rebootTimes came in and refreshed the requestBuffer. In that
              // cases we need to discard these requests.
              if (!reqExecutionOrderBuffer.contains(requestMeta)) {
                final TSStatus status =
                    new TSStatus(
                        RpcUtils.getStatus(
                            TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                            "PipeConsensus receiver received a deprecated request, which may be sent before the connector restart or pipe task restart. Consider to discard it"));
                LOGGER.info(
                    "PipeConsensus-PipeName-{}: received a deprecated request, which may be sent before the connector restart or pipe task restart. Consider to discard it",
                    consensusPipeName);
                return new TPipeConsensusTransferResp(status);
              }
              // If the buffer is not full after waiting timeout, we suppose that the sender will
              // not send any more events at this time, that is, the sender has sent all events. At
              // this point we apply the event at reqBuffer's peek
              if (timeout
                  && reqExecutionOrderBuffer.size() < IOTDB_CONFIG.getIotConsensusV2PipelineSize()
                  && reqExecutionOrderBuffer.first() != null
                  && reqExecutionOrderBuffer.first().equals(requestMeta)) {
                long startApplyNanos = System.nanoTime();
                metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
                requestMeta.setStartApplyNanos(startApplyNanos);
                TPipeConsensusTransferResp resp = loadEvent(req);

                if (resp != null
                    && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  onSuccess(tCommitId, isTransferTsFileSeal);
                  // signal all other reqs that may wait for this event
                  condition.signalAll();
                }
                return resp;
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOGGER.warn(
                  "PipeConsensus-PipeName-{}: current waiting is interrupted. onSyncedCommitIndex: {}. Exception: ",
                  consensusPipeName,
                  tCommitId.getCommitIndex(),
                  e);
              // Avoid infinite loop when RPC thread is killed by OS
              return new TPipeConsensusTransferResp(
                  RpcUtils.getStatus(
                      TSStatusCode.SHUT_DOWN_ERROR,
                      "RPC processor is interrupted by shutdown hook when wait on condition!"));
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Reset all data to initial status and set connectorRebootTimes properly. This method is called
     * when receiver identifies connector has rebooted.
     */
    private void resetWithNewestRebootTime(int connectorRebootTimes, Condition condition) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: receiver detected an newer rebootTimes, which indicates the leader has rebooted. receiver will reset all its data.",
          consensusPipeName);
      // since pipe task will resend all data that hasn't synchronized after dataNode reboots, it's
      // safe to clear all events in buffer.
      clear();
      // signal all deprecated requests that may wait on condition to expire them
      condition.signalAll();
      // sync the follower's connectorRebootTimes with connector's actual rebootTimes.
      this.connectorRebootTimes = connectorRebootTimes;
      // Note: dataNode rebooting will reset pipeTaskRestartTimes.
      this.pipeTaskRestartTimes = 0;
    }

    private void resetWithNewestRestartTime(int pipeTaskRestartTimes, Condition condition) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: receiver detected an newer pipeTaskRestartTimes, which indicates the pipe task has restarted. receiver will reset all its data.",
          consensusPipeName);
      // since pipe task will resend all data that hasn't synchronized after restarts, it's safe to
      // clear all events in buffer.
      clear();
      // signal all deprecated requests that may wait on condition to expire them
      condition.signalAll();
      this.pipeTaskRestartTimes = pipeTaskRestartTimes;
    }

    private void clear() {
      this.reqExecutionOrderBuffer.clear();
      this.tsFileWriterPool.handleExit(consensusPipeName);
      this.onSyncedCommitIndex = 0;
    }
  }

  private static class RequestMeta {
    private final TCommitId commitId;
    private long startApplyNanos = 0;

    public RequestMeta(TCommitId commitId) {
      this.commitId = commitId;
    }

    public int getDataNodeRebootTimes() {
      return commitId.getDataNodeRebootTimes();
    }

    public int getPipeTaskRestartTimes() {
      return commitId.getPipeTaskRestartTimes();
    }

    public long getCommitIndex() {
      return commitId.getCommitIndex();
    }

    public void setStartApplyNanos(long startApplyNanos) {
      // Notice that a tsFileInsertionEvent will enter RequestExecutor multiple times, we only need
      // to record the time of the first apply
      if (this.startApplyNanos == 0) {
        this.startApplyNanos = startApplyNanos;
      }
    }

    public long getStartApplyNanos() {
      if (startApplyNanos == 0) {
        return System.nanoTime();
      }
      return startApplyNanos;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RequestMeta that = (RequestMeta) o;
      return commitId.equals(that.commitId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitId);
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getReceiveBufferSize() {
    return this.requestExecutor.reqExecutionOrderBuffer.size();
  }

  public int getWALEventCount() {
    return this.requestExecutor.WALEventCount.get();
  }

  public int getTsFileEventCount() {
    return this.requestExecutor.tsFileEventCount.get();
  }

  public String getConsensusGroupIdStr() {
    return consensusGroupId.toString();
  }
}
