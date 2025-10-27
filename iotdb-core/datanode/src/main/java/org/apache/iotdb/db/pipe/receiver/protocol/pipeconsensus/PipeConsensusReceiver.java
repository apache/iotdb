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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.response.PipeConsensusTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.RetryUtils;
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
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusReceiverMetrics;
import org.apache.iotdb.db.pipe.event.common.tsfile.aggregator.TsFileInsertionPointCounter;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusDeleteNodeReq;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusTabletBinaryReq;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealReq;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealWithModReq;
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

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.external.commons.io.FileUtils;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PipeConsensusReceiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiver.class);
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final long PIPE_CONSENSUS_RECEIVER_MAX_WAITING_TIME_IN_MS =
      (long) IOTDB_CONFIG.getConnectionTimeoutInMS()
          / 3
          * IOTDB_CONFIG.getIotConsensusV2PipelineSize();
  private static final long CLOSE_TSFILE_WRITER_MAX_WAIT_TIME_IN_MS = 5000;
  private static final long RETRY_WAIT_TIME = 500;
  private final RequestExecutor requestExecutor;
  private final PipeConsensus pipeConsensus;
  private final ConsensusGroupId consensusGroupId;
  private final ConsensusPipeName consensusPipeName;
  // Used to buffer TsFile when transfer TsFile asynchronously.
  private final PipeConsensusTsFileWriterPool pipeConsensusTsFileWriterPool;
  private final ScheduledExecutorService scheduledTsFileWriterCheckerPool =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_CONSENSUS_TSFILE_WRITER_CHECKER.getName());
  private Future<?> tsFileWriterCheckerFuture;
  private final List<String> receiveDirs = new ArrayList<>();
  private final PipeConsensusReceiverMetrics pipeConsensusReceiverMetrics;
  private final FolderManager folderManager;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final ReadWriteLock tsFilePieceReadWriteLock = new ReentrantReadWriteLock(true);

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
    List<String> receiverBaseDirsName =
        Arrays.asList(
            IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2ReceiverFileDirs());

    try {
      initiateTsFileBufferFolder(receiverBaseDirsName);
    } catch (Exception e) {
      LOGGER.error("Fail to initiate file buffer folder, Error msg: {}", e.getMessage());
      throw new RuntimeException(e);
    }

    try {
      this.folderManager = new FolderManager(receiveDirs, DirectoryStrategyType.SEQUENCE_STRATEGY);
      this.pipeConsensusTsFileWriterPool = new PipeConsensusTsFileWriterPool(consensusPipeName);
    } catch (Exception e) {
      LOGGER.error(
          "Fail to create pipeConsensus receiver file folders allocation strategy because all disks of folders are full.",
          e);
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
      // release tsFileWriter if pre-check failed as leader will resend the whole tsFileEvent
      releaseTsFileWriter(
          pipeConsensusTsFileWriterPool.tryToFindCorrespondingWriter(req.getCommitId()), false);
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
          resp = requestExecutor.onRequest(req, false, true);
          break;
        case TRANSFER_DELETION:
        case TRANSFER_TABLET_BINARY:
        case TRANSFER_TABLET_INSERT_NODE:
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
    if (isClosed.get()) {
      return PipeConsensusReceiverAgent.closedResp(consensusPipeName.toString(), req.getCommitId());
    }
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
    tsFilePieceReadWriteLock.readLock().lock();
    try {
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
        pipeConsensusReceiverMetrics.recordTsFilePieceWriteTime(
            System.nanoTime() - endPreCheckNanos);
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
        } finally {
          // Exception may occur when disk system go wrong. At this time, we may reset all resource
          // and receive this file from scratch when leader will try to resend this file from
          // scratch as well.
          releaseTsFileWriter(tsFileWriter, false);
        }
      }
    } finally {
      tsFilePieceReadWriteLock.readLock().unlock();
    }
  }

  private TPipeConsensusTransferResp handleTransferFileSeal(final PipeConsensusTsFileSealReq req) {
    LOGGER.info("PipeConsensus-PipeName-{}: starting to receive tsFile seal", consensusPipeName);
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
      // 2. writingFileWriter and writingFile will be reset in `releaseTsFileWriter`
      writingFileWriter.close();

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
      releaseTsFileWriter(tsFileWriter, false);
    }
  }

  private TPipeConsensusTransferResp handleTransferFileSealWithMods(
      final PipeConsensusTsFileSealWithModReq req) {
    LOGGER.info(
        "PipeConsensus-PipeName-{}: starting to receive tsFile seal with mods", consensusPipeName);
    long startBorrowTsFileWriterNanos = System.nanoTime();
    PipeConsensusTsFileWriter tsFileWriter =
        pipeConsensusTsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    pipeConsensusReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    File currentWritingDirPath = tsFileWriter.getLocalWritingDir();

    final List<File> files =
        req.getFileNames().stream()
            .map(fileName -> new File(currentWritingDirPath, fileName))
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
      // 2. writingFileWriter and writingFile will be reset in `releaseTsFileWriter`
      writingFileWriter.close();

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
      releaseTsFileWriter(tsFileWriter, false);
      // Clear the directory instead of only deleting the referenced files in seal request
      // to avoid previously undeleted file being redundant when transferring multi files
      IoTDBReceiverAgent.cleanPipeReceiverDir(currentWritingDirPath);
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
      region.loadNewTsFile(resource, true, false, true);
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

  private void updateWritingFileIfNeeded(
      final PipeConsensusTsFileWriter tsFileWriter,
      final String fileName,
      final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(tsFileWriter, fileName)
        && tsFileWriter.getWritingFileWriter() != null) {
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
      deleteFileOrDirectoryIfExists(
          tsFileWriter.getWritingFile(),
          false,
          String.format("Update TsFileWriter-%s", tsFileWriter.index));
    }

    // Make sure receiver file dir exists
    // This may be useless, because receiver file dir is created when receiver is initiated. just in
    // case.
    if (!tsFileWriter.getLocalWritingDir().exists()) {
      if (tsFileWriter.getLocalWritingDir().mkdirs()) {
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Receiver file dir {} was created.",
            consensusPipeName,
            tsFileWriter.getLocalWritingDir().getPath());
      } else {
        LOGGER.error(
            "PipeConsensus-PipeName-{}: Failed to create receiver file dir {}.",
            consensusPipeName,
            tsFileWriter.getLocalWritingDir().getPath());
      }
    }
    // Every tsFileWriter has its own writing path.
    // 1 Thread --> 1 connection --> 1 tsFileWriter --> 1 path
    tsFileWriter.setWritingFile(new File(tsFileWriter.getLocalWritingDir(), fileName));
    tsFileWriter.setWritingFileWriter(new RandomAccessFile(tsFileWriter.getWritingFile(), "rw"));
    LOGGER.info(
        "PipeConsensus-PipeName-{}: Writing file {} was created. Ready to write file pieces.",
        consensusPipeName,
        tsFileWriter.getWritingFile().getPath());
  }

  private void initiateTsFileBufferFolder(List<String> receiverBaseDirsName) throws IOException {
    // initiate receiverFileDirs
    for (String receiverFileBaseDir : receiverBaseDirsName) {
      // Create a new receiver file dir
      final File newReceiverDir = new File(receiverFileBaseDir, consensusPipeName.toString());
      // Check whether systemDir exists in case of system concurrently exit when receiver try to
      // make new dirs.
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
      deleteFileOrDirectoryIfExists(
          newReceiverDir, true, "Initial Receiver: delete origin receive dir");

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
      this.receiveDirs.add(newReceiverDir.getPath());
    }
  }

  private void clearAllReceiverBaseDir() {
    // Clear the original receiver file dir if exists
    for (String receiverFileBaseDir : receiveDirs) {
      File receiverDir = new File(receiverFileBaseDir);
      deleteFileOrDirectoryIfExists(receiverDir, true, "Clear receive dir manually");
    }
  }

  public PipeConsensusRequestVersion getVersion() {
    return PipeConsensusRequestVersion.VERSION_1;
  }

  public synchronized void handleExit() {
    // only after closing request executor, can we clean receiver.
    requestExecutor.tryClose();
    // remove metric
    MetricService.getInstance().removeMetricSet(pipeConsensusReceiverMetrics);
    // cancel periodic task
    if (tsFileWriterCheckerFuture != null) {
      tsFileWriterCheckerFuture.cancel(false);
      tsFileWriterCheckerFuture = null;
    }
    // shutdown executor
    scheduledTsFileWriterCheckerPool.shutdownNow();
    try {
      if (!scheduledTsFileWriterCheckerPool.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("TsFileChecker did not terminate within {}s", 30);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("TsFileChecker Thread {} still doesn't exit after 30s", consensusPipeName);
      Thread.currentThread().interrupt();
    }
    // Clear the tsFileWriters, receiverBuffer and receiver base dirs
    requestExecutor.clear(false, true);
    LOGGER.info("Receiver-{} exit successfully.", consensusPipeName.toString());
  }

  public void closeExecutor() {
    requestExecutor.tryClose();
  }

  private class PipeConsensusTsFileWriterPool {
    private final Lock lock = new ReentrantLock();
    private final List<PipeConsensusTsFileWriter> pipeConsensusTsFileWriterPool = new ArrayList<>();
    private final ConsensusPipeName consensusPipeName;

    public PipeConsensusTsFileWriterPool(ConsensusPipeName consensusPipeName)
        throws DiskSpaceInsufficientException, IOException {
      this.consensusPipeName = consensusPipeName;
      for (int i = 0; i < IOTDB_CONFIG.getIotConsensusV2PipelineSize(); i++) {
        PipeConsensusTsFileWriter tsFileWriter =
            new PipeConsensusTsFileWriter(i, consensusPipeName);
        // initialize writing path
        tsFileWriter.rollToNextWritingPath();
        pipeConsensusTsFileWriterPool.add(tsFileWriter);
      }

      tsFileWriterCheckerFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              scheduledTsFileWriterCheckerPool,
              this::checkZombieTsFileWriter,
              0,
              IOTDB_CONFIG.getTsFileWriterCheckInterval(),
              TimeUnit.MILLISECONDS);
      LOGGER.info(
          "Register {} with interval in seconds {} successfully.",
          ThreadName.PIPE_CONSENSUS_TSFILE_WRITER_CHECKER.getName(),
          IOTDB_CONFIG.getTsFileWriterCheckInterval());
    }

    public PipeConsensusTsFileWriter tryToFindCorrespondingWriter(TCommitId commitId) {
      Optional<PipeConsensusTsFileWriter> tsFileWriter =
          pipeConsensusTsFileWriterPool.stream()
              .filter(
                  item ->
                      item.isUsed()
                          && Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
              .findFirst();
      return tsFileWriter.orElse(null);
    }

    @SuppressWarnings("java:S3655")
    public PipeConsensusTsFileWriter borrowCorrespondingWriter(TCommitId commitId) {
      Optional<PipeConsensusTsFileWriter> tsFileWriter =
          pipeConsensusTsFileWriterPool.stream()
              .filter(
                  item ->
                      item.isUsed()
                          && Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
              .findFirst();

      // If the TsFileInsertionEvent is first using tsFileWriter, we will find the first available
      // buffer for it.
      if (!tsFileWriter.isPresent()) {
        // We should synchronously find the idle writer to avoid concurrency issues.
        lock.lock();
        try {
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
              "PipeConsensus{}: receiver thread get interrupted when waiting for borrowing tsFileWriter.",
              consensusPipeName);
        } finally {
          lock.unlock();
        }
      }

      return tsFileWriter.get().refreshLastUsedTs();
    }

    private void checkZombieTsFileWriter() {
      pipeConsensusTsFileWriterPool.stream()
          .filter(PipeConsensusTsFileWriter::isUsed)
          .forEach(
              writer -> {
                if (System.currentTimeMillis() - writer.lastUsedTs
                    >= IOTDB_CONFIG.getTsFileWriterZombieThreshold()) {
                  releaseTsFileWriter(writer, false);
                  LOGGER.info(
                      "PipeConsensus-PipeName-{}: tsfile writer-{} is cleaned up because no new requests were received for too long.",
                      consensusPipeName,
                      writer.index);
                }
              });
    }

    public void releaseAllWriters(ConsensusPipeName consensusPipeName) {
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
            releaseTsFileWriter(tsFileWriter, false);
          });
    }
  }

  private class PipeConsensusTsFileWriter {
    private final ConsensusPipeName consensusPipeName;
    private final int index;
    private File localWritingDir;
    private File writingFile;
    private RandomAccessFile writingFileWriter;
    // whether this buffer is used. this will be updated when first transfer tsFile piece or
    // when transfer seal.
    private volatile boolean isUsed = false;
    // If isUsed is true, this variable will be set to the TCommitId of holderEvent
    private volatile TCommitId commitIdOfCorrespondingHolderEvent;
    private long lastUsedTs;

    public PipeConsensusTsFileWriter(int index, ConsensusPipeName consensusPipeName) {
      this.index = index;
      this.consensusPipeName = consensusPipeName;
    }

    public void rollToNextWritingPath() throws IOException, DiskSpaceInsufficientException {
      if (folderManager == null) {
        throw new IOException(
            String.format(
                "PipeConsensus-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir",
                consensusPipeName, index));
      }
      this.localWritingDir =
          folderManager.getNextWithRetry(
              receiverBasePath -> {
                if (receiverBasePath == null) {
                  LOGGER.warn(
                      "PipeConsensus-PipeName-{}: Failed to get base directory", consensusPipeName);
                  return null;
                }
                File writingDir = new File(receiverBasePath + File.separator + index);
                deleteFileOrDirectoryIfExists(
                    writingDir,
                    true,
                    String.format(
                        "TsFileWriter-%s roll to new dir and delete last writing dir", index));

                if (writingDir.mkdirs()) {
                  LOGGER.info(
                      "PipeConsensus-PipeName-{}: tsfileWriter-{} roll to writing path {}",
                      consensusPipeName,
                      index,
                      writingDir.getPath());
                  return writingDir;
                }
                LOGGER.warn(
                    "PipeConsensus-PipeName-{}: Failed to create receiver tsFileWriter-{} file dir {}",
                    consensusPipeName,
                    index,
                    writingDir.getPath());
                return null;
              });

      if (this.localWritingDir == null) {
        throw new IOException(
            String.format(
                "PipeConsensus-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir",
                consensusPipeName, index));
      }
    }

    public File getLocalWritingDir() {
      return localWritingDir;
    }

    public File getWritingFile() {
      return writingFile;
    }

    public void setWritingFile(File writingFile) {
      this.writingFile = writingFile;
      if (writingFile == null) {
        LOGGER.info(
            "PipeConsensus-{}: TsFileWriter-{} set null writing file",
            consensusPipeName.toString(),
            index);
      }
    }

    public RandomAccessFile getWritingFileWriter() {
      return writingFileWriter;
    }

    public void setWritingFileWriter(RandomAccessFile writingFileWriter) throws IOException {
      this.writingFileWriter = writingFileWriter;
      if (writingFileWriter == null) {
        LOGGER.info(
            "PipeConsensus-{}: TsFileWriter-{} set null writing file writer",
            consensusPipeName.toString(),
            index);
      } else {
        // seek to the end of the file to ensure that the next piece will be appended to the end of
        // the file.
        this.writingFileWriter.seek(this.writingFileWriter.length());
      }
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

    public PipeConsensusTsFileWriter refreshLastUsedTs() {
      if (isUsed) {
        lastUsedTs = System.currentTimeMillis();
      }
      return this;
    }

    public void returnSelf(ConsensusPipeName consensusPipeName)
        throws DiskSpaceInsufficientException, IOException {
      // if config multi-disks, tsFileWriter will roll to new writing path.
      // must roll before set used to false, because the writing file may be deleted if other event
      // uses this tsfileWriter.
      if (receiveDirs.size() > 1) {
        rollToNextWritingPath();
      }
      // must set used to false after set commitIdOfCorrespondingHolderEvent to null to avoid the
      // situation that tsfileWriter is used by other event before set
      // commitIdOfCorrespondingHolderEvent to null
      this.commitIdOfCorrespondingHolderEvent = null;
      this.isUsed = false;
      LOGGER.info(
          "PipeConsensus-PipeName-{}: tsFileWriter-{} returned self",
          consensusPipeName.toString(),
          index);
    }
  }

  private void closeCurrentWritingFileWriter(
      PipeConsensusTsFileWriter tsFileWriter, boolean fsyncBeforeClose) {
    if (tsFileWriter.getWritingFileWriter() != null) {
      try {
        if (fsyncBeforeClose) {
          tsFileWriter.getWritingFileWriter().getFD().sync();
        }
        tsFileWriter.getWritingFileWriter().close();
        LOGGER.info(
            "PipeConsensus-PipeName-{}: Current writing file writer {} was closed.",
            consensusPipeName,
            tsFileWriter.getWritingFile() == null
                ? "null"
                : tsFileWriter.getWritingFile().getPath());
        tsFileWriter.setWritingFileWriter(null);
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
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: Current writing file writer is null. No need to close.",
            consensusPipeName.toString());
      }
    }
  }

  private void deleteFileOrDirectoryIfExists(File file, boolean deleteDir, String reason) {
    if (file.exists()) {
      try {
        if (file.isDirectory()) {
          if (deleteDir) {
            RetryUtils.retryOnException(
                () -> {
                  FileUtils.deleteDirectory(file);
                  return null;
                });
          } else {
            // There may be multiple files such as mods and tsfile pieces in the dir. Here we clean
            // the
            // dir instead of deleting it to avoid repeatedly deleting and creating the base dir for
            // tsfile writer
            RetryUtils.retryOnException(
                () -> {
                  FileUtils.cleanDirectory(file);
                  return null;
                });
          }
        } else {
          RetryUtils.retryOnException(() -> FileUtils.delete(file));
        }
        LOGGER.info(
            "PipeConsensus-PipeName-{}: {} {} was deleted.",
            consensusPipeName,
            reason,
            file.getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "PipeConsensus-PipeName-{}: {} Failed to delete {}, because {}.",
            consensusPipeName,
            reason,
            file.getPath(),
            e.getMessage(),
            e);
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-PipeName-{}: {} {} is not existed. No need to delete.",
            consensusPipeName,
            reason,
            file.getPath());
      }
    }
  }

  private void releaseTsFileWriter(
      PipeConsensusTsFileWriter tsFileWriter, boolean fsyncBeforeClose) {
    if (tsFileWriter == null) {
      return;
    }
    closeCurrentWritingFileWriter(tsFileWriter, fsyncBeforeClose);
    deleteFileOrDirectoryIfExists(
        tsFileWriter.getLocalWritingDir(),
        false,
        String.format("Release TsFileWriter-%s", tsFileWriter.index));
    tsFileWriter.setWritingFile(null);
    try {
      tsFileWriter.returnSelf(consensusPipeName);
    } catch (IOException | DiskSpaceInsufficientException e) {
      LOGGER.warn(
          "PipeConsensus-PipeName-{}: Failed to return tsFileWriter {}.",
          consensusPipeName,
          tsFileWriter,
          e);
    }
  }

  /**
   * An executor component to ensure all events sent from connector can be loaded in sequence,
   * although events can arrive receiver in a random sequence.
   */
  private class RequestExecutor {
    private static final String MSG_NODE_RESTART_INDEX_STALE =
        "sender dn restarts before this event was sent here";
    private static final String MSG_PIPE_RESTART_INDEX_STALE =
        "pipe task restarts before this event was sent here";
    private static final String MSG_STALE_REPLICATE_INDEX = "replicate index is out dated";

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
    private volatile long onSyncedReplicateIndex = 0;
    private volatile int connectorRebootTimes = 0;
    private volatile int pipeTaskRestartTimes = 0;

    public RequestExecutor(
        PipeConsensusReceiverMetrics metric, PipeConsensusTsFileWriterPool tsFileWriterPool) {
      this.reqExecutionOrderBuffer =
          new TreeSet<>(
              Comparator.comparingInt(RequestMeta::getDataNodeRebootTimes)
                  .thenComparingInt(RequestMeta::getPipeTaskRestartTimes)
                  .thenComparingLong(RequestMeta::getReplicateIndex));
      this.lock = new ReentrantLock();
      this.condition = lock.newCondition();
      this.metric = metric;
      this.tsFileWriterPool = tsFileWriterPool;
    }

    private TPipeConsensusTransferResp preCheck(TCommitId tCommitId) {
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
        return deprecatedResp(MSG_NODE_RESTART_INDEX_STALE, tCommitId);
      }
      // Similarly, check pipeTask restartTimes
      if (tCommitId.getDataNodeRebootTimes() == connectorRebootTimes
          && tCommitId.getPipeTaskRestartTimes() < pipeTaskRestartTimes) {
        return deprecatedResp(MSG_PIPE_RESTART_INDEX_STALE, tCommitId);
      }
      // Similarly, check replicationIndex
      if (tCommitId.getDataNodeRebootTimes() == connectorRebootTimes
          && tCommitId.getPipeTaskRestartTimes() == pipeTaskRestartTimes
          && tCommitId.getReplicateIndex() < onSyncedReplicateIndex + 1) {
        return deprecatedResp(MSG_STALE_REPLICATE_INDEX, tCommitId);
      }
      // pass check
      return null;
    }

    private TPipeConsensusTransferResp onRequest(
        final TPipeConsensusTransferReq req,
        final boolean isTransferTsFilePiece,
        final boolean isTransferTsFileSeal) {
      long startAcquireLockNanos = System.nanoTime();
      lock.lock();
      try {
        if (isClosed.get()) {
          return PipeConsensusReceiverAgent.closedResp(
              consensusPipeName.toString(), req.getCommitId());
        }
        long startDispatchNanos = System.nanoTime();
        metric.recordAcquireExecutorLockTimer(startDispatchNanos - startAcquireLockNanos);

        TCommitId tCommitId = req.getCommitId();
        RequestMeta requestMeta = new RequestMeta(tCommitId);
        TPipeConsensusTransferResp preCheckRes = preCheck(tCommitId);
        if (preCheckRes != null) {
          return preCheckRes;
        }

        LOGGER.info(
            "PipeConsensus-PipeName-{}: start to receive no.{} event",
            consensusPipeName,
            tCommitId);
        // Judge whether connector has rebooted or not, if the rebootTimes increases compared to
        // connectorRebootTimes, need to reset receiver because connector has been restarted.
        if (tCommitId.getDataNodeRebootTimes() > connectorRebootTimes) {
          resetWithNewestRebootTime(tCommitId.getDataNodeRebootTimes());
        }
        // Similarly, check pipeTask restartTimes
        if (tCommitId.getPipeTaskRestartTimes() > pipeTaskRestartTimes) {
          resetWithNewestRestartTime(tCommitId.getPipeTaskRestartTimes());
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
              && tCommitId.getReplicateIndex() == onSyncedReplicateIndex + 1) {
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
            LOGGER.info(
                "PipeConsensus-PipeName-{}: no.{} event get executed because receiver buffer's len >= pipeline, current receiver syncIndex {}, current buffer len {}",
                consensusPipeName,
                tCommitId,
                onSyncedReplicateIndex,
                reqExecutionOrderBuffer.size());
            long startApplyNanos = System.nanoTime();
            metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
            requestMeta.setStartApplyNanos(startApplyNanos);
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            TPipeConsensusTransferResp resp = loadEvent(req);

            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              onSuccess(tCommitId, isTransferTsFileSeal);
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

              if (isClosed.get()) {
                return PipeConsensusReceiverAgent.closedResp(
                    consensusPipeName.toString(), req.getCommitId());
              }
              // If some reqs find the buffer no longer contains their requestMeta after jumping out
              // from condition.await, it may indicate that during their wait, some reqs with newer
              // pipeTaskStartTimes or rebootTimes came in and refreshed the requestBuffer. In that
              // cases we need to discard these requests.
              if (!reqExecutionOrderBuffer.contains(requestMeta)) {
                return deprecatedResp(
                    String.format(
                        "%s or %s", MSG_NODE_RESTART_INDEX_STALE, MSG_PIPE_RESTART_INDEX_STALE),
                    tCommitId);
              }
              // After waiting timeout, we suppose that the sender will not send any more events at
              // this time, that is, the sender has sent all events. At this point we apply the
              // event at reqBuffer's peek
              if (timeout && reqExecutionOrderBuffer.first() != null) {
                // if current event is the first event in reqBuffer, we can process it.
                if (reqExecutionOrderBuffer.first().equals(requestMeta)) {
                  LOGGER.info(
                      "PipeConsensus-PipeName-{}: no.{} event get executed after awaiting timeout, current receiver syncIndex: {}",
                      consensusPipeName,
                      tCommitId,
                      onSyncedReplicateIndex);
                  long startApplyNanos = System.nanoTime();
                  metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
                  requestMeta.setStartApplyNanos(startApplyNanos);
                  TPipeConsensusTransferResp resp = loadEvent(req);

                  if (resp != null
                      && resp.getStatus().getCode()
                          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    onSuccess(tCommitId, isTransferTsFileSeal);
                  }
                  return resp;
                }
                // if current event is not the first event in reqBuffer, we should return an error
                // code to let leader retry or proceed instead of getting stuck in this while loop
                // and block sender.
                else {
                  final TSStatus status =
                      new TSStatus(
                          RpcUtils.getStatus(
                              TSStatusCode.PIPE_CONSENSUS_WAIT_ORDER_TIMEOUT,
                              "Waiting for the previous event times out, returns an error to let the sender retry and continue scheduling."));
                  // TODO: Turn it to debug after GA
                  LOGGER.info(
                      "PipeConsensus-{}: Waiting for the previous event times out, current peek {}, current id {}",
                      consensusPipeName,
                      reqExecutionOrderBuffer.first().commitId,
                      tCommitId);
                  return new TPipeConsensusTransferResp(status);
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOGGER.warn(
                  "PipeConsensus-PipeName-{}: current waiting is interrupted. onSyncedCommitIndex: {}. Exception: ",
                  consensusPipeName,
                  tCommitId.getReplicateIndex(),
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
        // let all threads that may still await become active again to acquire lock instead of
        // meaningless sleeping in the condition while lock is already released.
        condition.signalAll();
        lock.unlock();
      }
    }

    /**
     * Reset all data to initial status and set connectorRebootTimes properly. This method is called
     * when receiver identifies connector has rebooted.
     */
    private void resetWithNewestRebootTime(int connectorRebootTimes) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: receiver detected an newer rebootTimes, which indicates the leader has rebooted. receiver will reset all its data.",
          consensusPipeName);
      // since pipe task will resend all data that hasn't synchronized after dataNode reboots, it's
      // safe to clear all events in buffer.
      clear(true, false);
      // sync the follower's connectorRebootTimes with connector's actual rebootTimes.
      this.connectorRebootTimes = connectorRebootTimes;
      this.pipeTaskRestartTimes = 0;
    }

    private void resetWithNewestRestartTime(int pipeTaskRestartTimes) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: receiver detected an newer pipeTaskRestartTimes, which indicates the pipe task has restarted. receiver will reset all its data.",
          consensusPipeName);
      // since pipe task will resend all data that hasn't synchronized after restarts, it's safe to
      // clear all events in buffer.
      clear(false, false);
      this.pipeTaskRestartTimes = pipeTaskRestartTimes;
    }

    private void onSuccess(TCommitId commitId, boolean isTransferTsFileSeal) {
      LOGGER.info(
          "PipeConsensus-PipeName-{}: process no.{} event successfully!",
          consensusPipeName,
          commitId);
      RequestMeta curMeta = reqExecutionOrderBuffer.pollFirst();
      onSyncedReplicateIndex = commitId.getReplicateIndex();
      // update metric, notice that curMeta is never null.
      if (isTransferTsFileSeal) {
        tsFileEventCount.decrementAndGet();
        metric.recordReceiveTsFileTimer(System.nanoTime() - curMeta.getStartApplyNanos());
      } else {
        WALEventCount.decrementAndGet();
        metric.recordReceiveWALTimer(System.nanoTime() - curMeta.getStartApplyNanos());
      }
    }

    private void clear(boolean resetSyncIndex, boolean cleanBaseDir) {
      // TsFilePiece Writing may out of RequestExecutor.lock, meaning that we must use additional
      // lock here to ensure serial execution of cleanup and write piece
      tsFilePieceReadWriteLock.writeLock().lock();
      try {
        this.reqExecutionOrderBuffer.clear();
        this.tsFileWriterPool.releaseAllWriters(consensusPipeName);
        this.tsFileEventCount.set(0);
        this.WALEventCount.set(0);
        if (resetSyncIndex) {
          this.onSyncedReplicateIndex = 0;
        }
        if (cleanBaseDir) {
          clearAllReceiverBaseDir();
        }
      } finally {
        tsFilePieceReadWriteLock.writeLock().unlock();
      }
    }

    private void tryClose() {
      // It will not be closed until all requests sent before closing are done.
      lock.lock();
      try {
        isClosed.set(true);
      } finally {
        // let all threads that may still await become active again to acquire lock instead of
        // meaningless sleeping in the condition while lock is already released.
        condition.signalAll();
        lock.unlock();
      }
    }

    private TPipeConsensusTransferResp deprecatedResp(String msg, TCommitId tCommitId) {
      final TSStatus status =
          new TSStatus(
              RpcUtils.getStatus(
                  TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                  String.format(
                      "PipeConsensus receiver received a deprecated request, which may because %s. Consider to discard it.",
                      msg)));
      LOGGER.info(
          "PipeConsensus-PipeName-{}: received a deprecated request-{}, which may because {}. ",
          consensusPipeName,
          tCommitId,
          msg);
      return new TPipeConsensusTransferResp(status);
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

    public long getReplicateIndex() {
      return commitId.getReplicateIndex();
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
