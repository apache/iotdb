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

package org.apache.iotdb.db.pipe.receiver.protocol.iotconsensusv2;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverFilePathUtils;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2TransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.response.IoTConsensusV2TransferFilePieceResp;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2ServerImpl;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2ReceiverMetrics;
import org.apache.iotdb.db.pipe.event.common.tsfile.aggregator.TsFileInsertionPointCounter;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2DeleteNodeReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletBinaryReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletInsertNodeReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TsFileSealReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TsFileSealWithModReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TableDiskUsageStatisticUtil;
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
import java.nio.file.Path;
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

public class IoTConsensusV2Receiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTConsensusV2Receiver.class);
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final long IOT_CONSENSUS_V2_RECEIVER_MAX_WAITING_TIME_IN_MS =
      (long) IOTDB_CONFIG.getConnectionTimeoutInMS()
          / 3
          * IOTDB_CONFIG.getIotConsensusV2PipelineSize();
  private static final long CLOSE_TSFILE_WRITER_MAX_WAIT_TIME_IN_MS = 5000;
  private static final long RETRY_WAIT_TIME = 500;
  private final RequestExecutor requestExecutor;
  private final IoTConsensusV2 iotConsensusV2;
  private final ConsensusGroupId consensusGroupId;
  private final ConsensusPipeName consensusPipeName;
  // Used to buffer TsFile when transfer TsFile asynchronously.
  private final IoTConsensusV2TsFileWriterPool iotConsensusV2TsFileWriterPool;
  private final ScheduledExecutorService scheduledTsFileWriterCheckerPool =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.IOT_CONSENSUS_V2_TSFILE_WRITER_CHECKER.getName());
  private Future<?> tsFileWriterCheckerFuture;
  private final List<String> receiveDirs = new ArrayList<>();
  private final IoTConsensusV2ReceiverMetrics iotConsensusV2ReceiverMetrics;
  private final FolderManager folderManager;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final ReadWriteLock tsFilePieceReadWriteLock = new ReentrantReadWriteLock(true);

  public IoTConsensusV2Receiver(
      IoTConsensusV2 iotConsensusV2,
      ConsensusGroupId consensusGroupId,
      ConsensusPipeName consensusPipeName) {
    this.iotConsensusV2 = iotConsensusV2;
    this.consensusGroupId = consensusGroupId;
    this.iotConsensusV2ReceiverMetrics = new IoTConsensusV2ReceiverMetrics(this);
    this.consensusPipeName = consensusPipeName;

    // Each iotConsensusV2Receiver has its own base directories. for example, a default dir path is
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
      this.iotConsensusV2TsFileWriterPool = new IoTConsensusV2TsFileWriterPool(consensusPipeName);
    } catch (Exception e) {
      LOGGER.error(
          "Fail to create iotConsensusV2 receiver file folders allocation strategy because all disks of folders are full.",
          e);
      throw new RuntimeException(e);
    }

    this.requestExecutor =
        new RequestExecutor(iotConsensusV2ReceiverMetrics, iotConsensusV2TsFileWriterPool);
    MetricService.getInstance().addMetricSet(iotConsensusV2ReceiverMetrics);
  }

  /**
   * This method cannot be set to synchronize. Receive events can be concurrent since reqBuffer but
   * load event must be synchronized.
   */
  public TIoTConsensusV2TransferResp receive(final TIoTConsensusV2TransferReq req) {
    long startNanos = System.nanoTime();
    // PreCheck: if there are these cases: read-only; null impl; inactive impl, etc. The receiver
    // will reject synchronization.
    TIoTConsensusV2TransferResp resp = preCheckForReceiver(req);
    if (resp != null) {
      // release tsFileWriter if pre-check failed as leader will resend the whole tsFileEvent
      releaseTsFileWriter(
          iotConsensusV2TsFileWriterPool.tryToFindCorrespondingWriter(req.getCommitId()), false);
      return resp;
    }

    final short rawRequestType = req.getType();
    if (IoTConsensusV2RequestType.isValidatedRequestType(rawRequestType)) {
      switch (IoTConsensusV2RequestType.valueOf(rawRequestType)) {
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
      iotConsensusV2ReceiverMetrics.recordReceiveEventTimer(durationNanos);
      return resp;
    }
    // Unknown request type, which means the request can not be handled by this receiver,
    // maybe the version of the receiver is not compatible with the sender
    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("IoTConsensusV2 Unknown PipeRequestType %s.", rawRequestType));
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("IoTConsensusV2 Unknown PipeRequestType, response status = {}.", status);
    }
    return new TIoTConsensusV2TransferResp(status);
  }

  private TIoTConsensusV2TransferResp preCheckForReceiver(final TIoTConsensusV2TransferReq req) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusV2ServerImpl impl = iotConsensusV2.getImpl(groupId);

    if (impl == null) {
      String message =
          String.format(
              "IoTConsensusV2-PipeName-%s: unexpected consensusGroupId %s",
              consensusPipeName, groupId);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TIoTConsensusV2TransferResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), message));
    }
    if (impl.isReadOnly()) {
      String message =
          String.format(
              "IoTConsensusV2-PipeName-%s: fail to receive because system is read-only.",
              consensusPipeName);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TIoTConsensusV2TransferResp(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), message));
    }

    return null;
  }

  private TIoTConsensusV2TransferResp loadEvent(final TIoTConsensusV2TransferReq req) {
    if (isClosed.get()) {
      return IoTConsensusV2ReceiverAgent.closedResp(
          consensusPipeName.toString(), req.getCommitId());
    }
    // synchronized load event, ensured by upper caller's lock.
    try {
      final short rawRequestType = req.getType();
      if (IoTConsensusV2RequestType.isValidatedRequestType(rawRequestType)) {
        switch (IoTConsensusV2RequestType.valueOf(rawRequestType)) {
          case TRANSFER_TABLET_INSERT_NODE:
            return handleTransferTabletInsertNode(
                IoTConsensusV2TabletInsertNodeReq.fromTIoTConsensusV2TransferReq(req));
          case TRANSFER_TABLET_BINARY:
            return handleTransferTabletBinary(
                IoTConsensusV2TabletBinaryReq.fromTIoTConsensusV2TransferReq(req));
          case TRANSFER_DELETION:
            return handleTransferDeletion(
                IoTConsensusV2DeleteNodeReq.fromTIoTConsensusV2TransferReq(req));
          case TRANSFER_TS_FILE_PIECE:
            return handleTransferFilePiece(
                IoTConsensusV2TsFilePieceReq.fromTIoTConsensusV2TransferReq(req), true);
          case TRANSFER_TS_FILE_SEAL:
            return handleTransferFileSeal(
                IoTConsensusV2TsFileSealReq.fromTIoTConsensusV2TransferReq(req));
          case TRANSFER_TS_FILE_PIECE_WITH_MOD:
            return handleTransferFilePiece(
                IoTConsensusV2TsFilePieceReq.fromTIoTConsensusV2TransferReq(req), false);
          case TRANSFER_TS_FILE_SEAL_WITH_MOD:
            return handleTransferFileSealWithMods(
                IoTConsensusV2TsFileSealWithModReq.fromTIoTConsensusV2TransferReq(req));
          case TRANSFER_TABLET_BATCH:
            LOGGER.info("IoTConsensusV2 transfer batch hasn't been implemented yet.");
          default:
            break;
        }
      }
      // Unknown request type, which means the request can not be handled by this receiver,
      // maybe the version of the receiver is not compatible with the sender
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TYPE_ERROR,
              String.format("Unknown IoTConsensusV2RequestType %s.", rawRequestType));
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Unknown PipeRequestType, response status = {}.",
          consensusPipeName,
          status);
      return new TIoTConsensusV2TransferResp(status);
    } catch (Exception e) {
      final String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn("IoTConsensusV2-PipeName-{}: {}", consensusPipeName, error, e);
      return new TIoTConsensusV2TransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TIoTConsensusV2TransferResp handleTransferTabletInsertNode(
      final IoTConsensusV2TabletInsertNodeReq req) throws ConsensusGroupNotExistException {
    IoTConsensusV2ServerImpl impl =
        Optional.ofNullable(iotConsensusV2.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final InsertNode insertNode = req.getInsertNode();
    insertNode.markAsGeneratedByRemoteConsensusLeader();
    insertNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TIoTConsensusV2TransferResp(impl.writeOnFollowerReplica(insertNode));
  }

  private TIoTConsensusV2TransferResp handleTransferTabletBinary(
      final IoTConsensusV2TabletBinaryReq req) throws ConsensusGroupNotExistException {
    IoTConsensusV2ServerImpl impl =
        Optional.ofNullable(iotConsensusV2.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final InsertNode insertNode = req.convertToInsertNode();
    insertNode.markAsGeneratedByRemoteConsensusLeader();
    insertNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TIoTConsensusV2TransferResp(impl.writeOnFollowerReplica(insertNode));
  }

  private TIoTConsensusV2TransferResp handleTransferDeletion(final IoTConsensusV2DeleteNodeReq req)
      throws ConsensusGroupNotExistException {
    IoTConsensusV2ServerImpl impl =
        Optional.ofNullable(iotConsensusV2.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    final AbstractDeleteDataNode planNode = req.getDeleteDataNode();
    planNode.markAsGeneratedByRemoteConsensusLeader();
    planNode.setProgressIndex(
        ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
    return new TIoTConsensusV2TransferResp(impl.writeOnFollowerReplica(planNode));
  }

  private TIoTConsensusV2TransferResp handleTransferFilePiece(
      final IoTConsensusV2TransferFilePieceReq req, final boolean isSingleFile) {
    tsFilePieceReadWriteLock.readLock().lock();
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "IoTConsensusV2-PipeName-{}: starting to receive tsFile pieces", consensusPipeName);
      }
      long startBorrowTsFileWriterNanos = System.nanoTime();
      IoTConsensusV2TsFileWriter tsFileWriter =
          iotConsensusV2TsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
      long startPreCheckNanos = System.nanoTime();
      iotConsensusV2ReceiverMetrics.recordBorrowTsFileWriterTimer(
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
                  TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_OFFSET_RESET,
                  String.format(
                      "Request sender to reset file reader's offset from %s to %s.",
                      req.getStartWritingOffset(), writingFileWriter.length()));
          LOGGER.warn(
              "IoTConsensusV2-PipeName-{}: File offset reset requested by receiver, response status = {}.",
              consensusPipeName,
              status);
          return IoTConsensusV2TransferFilePieceResp.toTIoTConsensusV2TransferResp(
              status, writingFileWriter.length());
        }

        long endPreCheckNanos = System.nanoTime();
        iotConsensusV2ReceiverMetrics.recordTsFilePiecePreCheckTime(
            endPreCheckNanos - startPreCheckNanos);
        writingFileWriter.write(req.getFilePiece());
        iotConsensusV2ReceiverMetrics.recordTsFilePieceWriteTime(
            System.nanoTime() - endPreCheckNanos);
        return IoTConsensusV2TransferFilePieceResp.toTIoTConsensusV2TransferResp(
            RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
      } catch (Exception e) {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: Failed to write file piece from req {}.",
            consensusPipeName,
            req,
            e);
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
                String.format("Failed to write file piece, because %s", e.getMessage()));
        try {
          return IoTConsensusV2TransferFilePieceResp.toTIoTConsensusV2TransferResp(
              status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
        } catch (IOException ex) {
          return IoTConsensusV2TransferFilePieceResp.toTIoTConsensusV2TransferResp(status);
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

  private TIoTConsensusV2TransferResp handleTransferFileSeal(
      final IoTConsensusV2TsFileSealReq req) {
    LOGGER.info("IoTConsensusV2-PipeName-{}: starting to receive tsFile seal", consensusPipeName);
    long startBorrowTsFileWriterNanos = System.nanoTime();
    IoTConsensusV2TsFileWriter tsFileWriter =
        iotConsensusV2TsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    iotConsensusV2ReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    try {
      if (isWritingFileNonAvailable(tsFileWriter)) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file, because writing file %s is not available.", writingFile));
        LOGGER.warn(status.getMessage());
        return new TIoTConsensusV2TransferResp(status);
      }

      final TIoTConsensusV2TransferResp resp =
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
      iotConsensusV2ReceiverMetrics.recordTsFileSealPreCheckTimer(
          endPreCheckNanos - startPreCheckNanos);
      updateWritePointCountMetrics(req.getPointCount(), fileAbsolutePath);
      final TSStatus status =
          loadFileToDataRegion(
              fileAbsolutePath,
              ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
      iotConsensusV2ReceiverMetrics.recordTsFileSealLoadTimer(System.nanoTime() - endPreCheckNanos);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "IoTConsensusV2-PipeName-{}: Seal file {} successfully.",
            consensusPipeName,
            fileAbsolutePath);
      } else {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because {}.",
            consensusPipeName,
            fileAbsolutePath,
            status.getMessage());
      }
      return new TIoTConsensusV2TransferResp(status);
    } catch (IOException e) {
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {} from req {}.",
          consensusPipeName,
          writingFile,
          req,
          e);
      return new TIoTConsensusV2TransferResp(
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } catch (LoadFileException e) {
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to load file {} from req {}.",
          consensusPipeName,
          writingFile,
          req,
          e);
      return new TIoTConsensusV2TransferResp(
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

  private TIoTConsensusV2TransferResp handleTransferFileSealWithMods(
      final IoTConsensusV2TsFileSealWithModReq req) {
    LOGGER.info(
        "IoTConsensusV2-PipeName-{}: starting to receive tsFile seal with mods", consensusPipeName);
    long startBorrowTsFileWriterNanos = System.nanoTime();
    IoTConsensusV2TsFileWriter tsFileWriter =
        iotConsensusV2TsFileWriterPool.borrowCorrespondingWriter(req.getCommitId());
    long startPreCheckNanos = System.nanoTime();
    iotConsensusV2ReceiverMetrics.recordBorrowTsFileWriterTimer(
        startPreCheckNanos - startBorrowTsFileWriterNanos);
    final File writingFile = tsFileWriter.getWritingFile();
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final File currentWritingDirPath = tsFileWriter.getLocalWritingDir();
    try {
      final List<File> files =
          req.getFileNames().stream()
              .map(
                  fileName -> {
                    try {
                      return resolveWritingFilePath(tsFileWriter, fileName).toFile();
                    } catch (final IOException e) {
                      throw new IllegalArgumentException(e);
                    }
                  })
              .collect(Collectors.toList());

      if (isWritingFileNonAvailable(tsFileWriter)) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, because writing file %s is not available.",
                    req.getFileNames(), writingFile));
        LOGGER.warn(status.getMessage());
        return new TIoTConsensusV2TransferResp(status);
      }

      // Any of the transferred files cannot be empty, or else the receiver
      // will not sense this file because no pieces are sent
      for (int i = 0; i < req.getFileNames().size(); ++i) {
        final TIoTConsensusV2TransferResp resp =
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

      if (req.getFileNames().size() < 2) {
        return new TIoTConsensusV2TransferResp(
            RpcUtils.getStatus(
                TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, because the number of files is less than 2.",
                    req.getFileNames())));
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
      iotConsensusV2ReceiverMetrics.recordTsFileSealPreCheckTimer(
          endPreCheckNanos - startPreCheckNanos);
      final String tsFileAbsolutePath = fileAbsolutePaths.get(1);
      updateWritePointCountMetrics(req.getPointCounts().get(1), tsFileAbsolutePath);
      final TSStatus status =
          loadFileToDataRegion(
              tsFileAbsolutePath,
              ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex())));
      iotConsensusV2ReceiverMetrics.recordTsFileSealLoadTimer(System.nanoTime() - endPreCheckNanos);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "IoTConsensusV2-PipeName-{}: Seal file with mods {} successfully.",
            consensusPipeName,
            fileAbsolutePaths);
      } else {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: Failed to seal file {}, status is {}.",
            consensusPipeName,
            fileAbsolutePaths,
            status);
      }
      return new TIoTConsensusV2TransferResp(status);
    } catch (Exception e) {
      final Throwable rootCause = e instanceof IllegalArgumentException ? e.getCause() : e;
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {} from req {}.",
          consensusPipeName,
          req.getFileNames(),
          req,
          rootCause);
      return new TIoTConsensusV2TransferResp(
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s because %s",
                  req.getFileNames(),
                  rootCause == null ? e.getMessage() : rootCause.getMessage())));
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

  private TIoTConsensusV2TransferResp checkNonFinalFileSeal(
      final IoTConsensusV2TsFileWriter tsFileWriter,
      final File file,
      final String fileName,
      final long fileLength)
      throws IOException {
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    if (!file.exists()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s, the file does not exist.", fileName));
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because the file does not exist.",
          consensusPipeName,
          fileName);
      return new TIoTConsensusV2TransferResp(status);
    }

    if (fileLength != file.length()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {} when check non final seal, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusPipeName,
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TIoTConsensusV2TransferResp(status);
    }

    return null;
  }

  private TSStatus loadFileToDataRegion(String filePath, ProgressIndex progressIndex)
      throws IOException, LoadFileException {
    DataRegion region =
        StorageEngine.getInstance().getDataRegion(((DataRegionId) consensusGroupId));
    if (region != null) {
      TsFileResource resource = generateTsFileResource(filePath, progressIndex);
      region.loadNewTsFile(
          resource,
          true,
          false,
          true,
          region.isTableModel()
              ? TableDiskUsageStatisticUtil.calculateTableSizeMap(resource)
              : Optional.empty());
    } else {
      // Data region is null indicates that dr has been removed or migrated. In those cases, there
      // is no need to replicate data. we just return success to avoid leader keeping retry
      LOGGER.info(
          "IoTConsensusV2-PipeName-{}: skip load tsfile-{} when sealing, because this region has been removed or migrated.",
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
          "IoTConsensusV2-PipeName-{}: The point count of TsFile {} is not given by sender, "
              + "will read actual point count from TsFile.",
          consensusPipeName,
          tsFileAbsolutePath);
    }

    try (final TsFileInsertionPointCounter counter =
        new TsFileInsertionPointCounter(new File(tsFileAbsolutePath), null)) {
      updateWritePointCountMetrics(counter.count());
    } catch (IOException e) {
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to read TsFile when counting points: {}.",
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
    tsFileResource.setGeneratedByIoTConsensusV2(true);
    tsFileResource.serialize();
    return tsFileResource;
  }

  private boolean isWritingFileNonAvailable(IoTConsensusV2TsFileWriter tsFileWriter) {
    File writingFile = tsFileWriter.getWritingFile();
    RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final boolean isWritingFileAvailable =
        writingFile != null && writingFile.exists() && writingFileWriter != null;
    if (!isWritingFileAvailable) {
      LOGGER.info(
          "IoTConsensusV2-PipeName-{}: Writing file {} is not available. "
              + "Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.",
          consensusPipeName,
          writingFile,
          writingFile == null,
          writingFile != null && writingFile.exists(),
          writingFileWriter == null);
    }
    return !isWritingFileAvailable;
  }

  private TIoTConsensusV2TransferResp checkFinalFileSeal(
      final IoTConsensusV2TsFileWriter tsFileWriter, final String fileName, final long fileLength)
      throws IOException {
    final File writingFile = tsFileWriter.getWritingFile();
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    if (!isFileExistedAndNameCorrect(tsFileWriter, fileName)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because writing file is %s.", fileName, writingFile));
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because writing file is {}.",
          consensusPipeName,
          fileName,
          writingFile);
      return new TIoTConsensusV2TransferResp(status);
    }

    if (isWritingFileOffsetNonCorrect(tsFileWriter, fileLength)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Failed to seal file {} when check final seal file, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusPipeName,
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TIoTConsensusV2TransferResp(status);
    }

    return null;
  }

  private boolean isFileExistedAndNameCorrect(
      IoTConsensusV2TsFileWriter tsFileWriter, String fileName) {
    final File writingFile = tsFileWriter.getWritingFile();
    try {
      return writingFile != null
          && writingFile.exists()
          && writingFile
              .toPath()
              .toAbsolutePath()
              .normalize()
              .equals(resolveWritingFilePath(tsFileWriter, fileName));
    } catch (final IOException e) {
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Illegal file name {} when checking writing file.",
          consensusPipeName,
          fileName,
          e);
      return false;
    }
  }

  private boolean isWritingFileOffsetNonCorrect(
      IoTConsensusV2TsFileWriter tsFileWriter, final long offset) throws IOException {
    final File writingFile = tsFileWriter.getWritingFile();
    final RandomAccessFile writingFileWriter = tsFileWriter.getWritingFileWriter();

    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      LOGGER.warn(
          "IoTConsensusV2-PipeName-{}: Writing file {}'s offset is {}, but request sender's offset is {}.",
          consensusPipeName,
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return !offsetCorrect;
  }

  private void updateWritingFileIfNeeded(
      final IoTConsensusV2TsFileWriter tsFileWriter,
      final String fileName,
      final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(tsFileWriter, fileName)
        && tsFileWriter.getWritingFileWriter() != null) {
      return;
    }

    LOGGER.info(
        "IoTConsensusV2-PipeName-{}: Writing file {} is not existed or name is not correct, try to create it. "
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
            "IoTConsensusV2-PipeName-{}: Receiver file dir {} was created.",
            consensusPipeName,
            tsFileWriter.getLocalWritingDir().getPath());
      } else {
        LOGGER.error(
            "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}.",
            consensusPipeName,
            tsFileWriter.getLocalWritingDir().getPath());
      }
    }
    // Every tsFileWriter has its own writing path.
    // 1 Thread --> 1 connection --> 1 tsFileWriter --> 1 path
    tsFileWriter.setWritingFile(resolveWritingFilePath(tsFileWriter, fileName).toFile());
    tsFileWriter.setWritingFileWriter(new RandomAccessFile(tsFileWriter.getWritingFile(), "rw"));
    LOGGER.info(
        "IoTConsensusV2-PipeName-{}: Writing file {} was created. Ready to write file pieces.",
        consensusPipeName,
        tsFileWriter.getWritingFile().getPath());
  }

  private Path resolveWritingFilePath(
      final IoTConsensusV2TsFileWriter tsFileWriter, final String fileName) throws IOException {
    try {
      return PipeReceiverFilePathUtils.resolveFilePath(
          tsFileWriter.getLocalWritingDir().toPath(), fileName);
    } catch (final IOException e) {
      LOGGER.error(
          "IoTConsensusV2-PipeName-{}: Path traversal attempt detected! Filename: {}",
          consensusPipeName,
          fileName);
      throw e;
    }
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
            "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}. Because parent system dir have been deleted due to system concurrently exit.",
            consensusPipeName,
            newReceiverDir.getPath());
        throw new IOException(
            String.format(
                "IoTConsensusV2-PipeName-%s: Failed to create receiver file dir %s. Because parent system dir have been deleted due to system concurrently exit.",
                consensusPipeName, newReceiverDir.getPath()));
      }
      // Remove exists dir
      deleteFileOrDirectoryIfExists(
          newReceiverDir, true, "Initial Receiver: delete origin receive dir");

      if (!newReceiverDir.mkdirs()) {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}. May because authority or dir already exists etc.",
            consensusPipeName,
            newReceiverDir.getPath());
        throw new IOException(
            String.format(
                "IoTConsensusV2-PipeName-%s: Failed to create receiver file dir %s. May because authority or dir already exists etc.",
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

  public IoTConsensusV2RequestVersion getVersion() {
    return IoTConsensusV2RequestVersion.VERSION_1;
  }

  public synchronized void handleExit() {
    // only after closing request executor, can we clean receiver.
    requestExecutor.tryClose();
    // remove metric
    MetricService.getInstance().removeMetricSet(iotConsensusV2ReceiverMetrics);
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

  private class IoTConsensusV2TsFileWriterPool {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final List<IoTConsensusV2TsFileWriter> iotConsensusV2TsFileWriterPool =
        new ArrayList<>();
    private final ConsensusPipeName consensusPipeName;

    public IoTConsensusV2TsFileWriterPool(ConsensusPipeName consensusPipeName)
        throws DiskSpaceInsufficientException, IOException {
      this.consensusPipeName = consensusPipeName;
      for (int i = 0; i < IOTDB_CONFIG.getIotConsensusV2PipelineSize(); i++) {
        IoTConsensusV2TsFileWriter tsFileWriter =
            new IoTConsensusV2TsFileWriter(i, consensusPipeName);
        // initialize writing path
        tsFileWriter.rollToNextWritingPath();
        iotConsensusV2TsFileWriterPool.add(tsFileWriter);
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
          ThreadName.IOT_CONSENSUS_V2_TSFILE_WRITER_CHECKER.getName(),
          IOTDB_CONFIG.getTsFileWriterCheckInterval());
    }

    public IoTConsensusV2TsFileWriter tryToFindCorrespondingWriter(TCommitId commitId) {
      Optional<IoTConsensusV2TsFileWriter> tsFileWriter =
          iotConsensusV2TsFileWriterPool.stream()
              .filter(
                  item ->
                      item.isUsed()
                          && Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
              .findFirst();
      return tsFileWriter.orElse(null);
    }

    public IoTConsensusV2TsFileWriter borrowCorrespondingWriter(TCommitId commitId) {
      final Optional<IoTConsensusV2TsFileWriter> correspondingWriter =
          iotConsensusV2TsFileWriterPool.stream()
              .filter(
                  item ->
                      item.isUsed()
                          && Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
              .findFirst();
      if (correspondingWriter.isPresent()) {
        return correspondingWriter.get().refreshLastUsedTs();
      }

      lock.lock();
      try {
        while (true) {
          final Optional<IoTConsensusV2TsFileWriter> idleWriter =
              iotConsensusV2TsFileWriterPool.stream().filter(item -> !item.isUsed()).findFirst();
          if (idleWriter.isPresent()) {
            final IoTConsensusV2TsFileWriter writer = idleWriter.get();
            // Publish commitId before marking the writer as used so lock-free lookup callers
            // observing isUsed=true can always see the bound commitId as well.
            writer.setCommitIdOfCorrespondingHolderEvent(commitId);
            writer.setUsed(true);
            return writer.refreshLastUsedTs();
          }

          condition.await(RETRY_WAIT_TIME, TimeUnit.MILLISECONDS);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        final String errorStr =
            String.format(
                "IoTConsensusV2%s: receiver thread get interrupted when waiting for borrowing tsFileWriter.",
                consensusPipeName);
        LOGGER.warn(errorStr);
        throw new RuntimeException(errorStr);
      } finally {
        lock.unlock();
      }
    }

    private void checkZombieTsFileWriter() {
      iotConsensusV2TsFileWriterPool.stream()
          .filter(IoTConsensusV2TsFileWriter::isUsed)
          .forEach(
              writer -> {
                if (System.currentTimeMillis() - writer.lastUsedTs
                    >= IOTDB_CONFIG.getTsFileWriterZombieThreshold()) {
                  releaseTsFileWriter(writer, false);
                  LOGGER.info(
                      "IoTConsensusV2-PipeName-{}: tsfile writer-{} is cleaned up because no new requests were received for too long.",
                      consensusPipeName,
                      writer.index);
                }
              });
    }

    public void releaseAllWriters(ConsensusPipeName consensusPipeName) {
      iotConsensusV2TsFileWriterPool.forEach(
          tsFileWriter -> {
            // Wait until tsFileWriter is not used by TsFileInsertionEvent or timeout.
            long currentTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - currentTime
                    < CLOSE_TSFILE_WRITER_MAX_WAIT_TIME_IN_MS
                && tsFileWriter.isUsed()) {
              try {
                Thread.sleep(RETRY_WAIT_TIME);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn(
                    "IoTConsensusV2-PipeName-{}: receiver thread get interrupted when exiting.",
                    consensusPipeName.toString());
                // avoid infinite loop
                break;
              }
            }
            releaseTsFileWriter(tsFileWriter, false);
          });
    }
  }

  private class IoTConsensusV2TsFileWriter {
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

    public IoTConsensusV2TsFileWriter(int index, ConsensusPipeName consensusPipeName) {
      this.index = index;
      this.consensusPipeName = consensusPipeName;
    }

    public void rollToNextWritingPath() throws IOException, DiskSpaceInsufficientException {
      if (folderManager == null) {
        throw new IOException(
            String.format(
                "IoTConsensusV2-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir",
                consensusPipeName, index));
      }
      this.localWritingDir =
          folderManager.getNextWithRetry(
              receiverBasePath -> {
                if (receiverBasePath == null) {
                  LOGGER.warn(
                      "IoTConsensusV2-PipeName-{}: Failed to get base directory",
                      consensusPipeName);
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
                      "IoTConsensusV2-PipeName-{}: tsfileWriter-{} roll to writing path {}",
                      consensusPipeName,
                      index,
                      writingDir.getPath());
                  return writingDir;
                }
                LOGGER.warn(
                    "IoTConsensusV2-PipeName-{}: Failed to create receiver tsFileWriter-{} file dir {}",
                    consensusPipeName,
                    index,
                    writingDir.getPath());
                return null;
              });

      if (this.localWritingDir == null) {
        throw new IOException(
            String.format(
                "IoTConsensusV2-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir",
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
            "IoTConsensusV2-{}: TsFileWriter-{} set null writing file",
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
            "IoTConsensusV2-{}: TsFileWriter-{} set null writing file writer",
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

    public IoTConsensusV2TsFileWriter refreshLastUsedTs() {
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
          "IoTConsensusV2-PipeName-{}: tsFileWriter-{} returned self",
          consensusPipeName.toString(),
          index);
    }
  }

  private void closeCurrentWritingFileWriter(
      IoTConsensusV2TsFileWriter tsFileWriter, boolean fsyncBeforeClose) {
    if (tsFileWriter.getWritingFileWriter() != null) {
      try {
        if (fsyncBeforeClose) {
          tsFileWriter.getWritingFileWriter().getFD().sync();
        }
        tsFileWriter.getWritingFileWriter().close();
        LOGGER.info(
            "IoTConsensusV2-PipeName-{}: Current writing file writer {} was closed.",
            consensusPipeName,
            tsFileWriter.getWritingFile() == null
                ? "null"
                : tsFileWriter.getWritingFile().getPath());
        tsFileWriter.setWritingFileWriter(null);
      } catch (IOException e) {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: Failed to close current writing file writer {}, because {}.",
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
            "IoTConsensusV2-PipeName-{}: Current writing file writer is null. No need to close.",
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
            "IoTConsensusV2-PipeName-{}: {} {} was deleted.",
            consensusPipeName,
            reason,
            file.getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "IoTConsensusV2-PipeName-{}: {} Failed to delete {}, because {}.",
            consensusPipeName,
            reason,
            file.getPath(),
            e.getMessage(),
            e);
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "IoTConsensusV2-PipeName-{}: {} {} is not existed. No need to delete.",
            consensusPipeName,
            reason,
            file.getPath());
      }
    }
  }

  private void releaseTsFileWriter(
      IoTConsensusV2TsFileWriter tsFileWriter, boolean fsyncBeforeClose) {
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
          "IoTConsensusV2-PipeName-{}: Failed to return tsFileWriter {}.",
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
    // IOT_CONSENSUS_V2_PIPELINE_SIZE.
    // Here we use set is to avoid duplicate events being received in some special cases
    private final TreeSet<RequestMeta> reqExecutionOrderBuffer;
    private final Lock lock;
    private final Condition condition;
    private final IoTConsensusV2ReceiverMetrics metric;
    private final IoTConsensusV2TsFileWriterPool tsFileWriterPool;
    private final AtomicInteger WALEventCount = new AtomicInteger(0);
    private final AtomicInteger tsFileEventCount = new AtomicInteger(0);
    private volatile long onSyncedReplicateIndex = 0;
    private volatile int connectorRebootTimes = 0;
    private volatile int pipeTaskRestartTimes = 0;

    public RequestExecutor(
        IoTConsensusV2ReceiverMetrics metric, IoTConsensusV2TsFileWriterPool tsFileWriterPool) {
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

    private TIoTConsensusV2TransferResp preCheck(TCommitId tCommitId) {
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

    private TIoTConsensusV2TransferResp onRequest(
        final TIoTConsensusV2TransferReq req,
        final boolean isTransferTsFilePiece,
        final boolean isTransferTsFileSeal) {
      long startAcquireLockNanos = System.nanoTime();
      lock.lock();
      try {
        if (isClosed.get()) {
          return IoTConsensusV2ReceiverAgent.closedResp(
              consensusPipeName.toString(), req.getCommitId());
        }
        long startDispatchNanos = System.nanoTime();
        metric.recordAcquireExecutorLockTimer(startDispatchNanos - startAcquireLockNanos);

        TCommitId tCommitId = req.getCommitId();
        RequestMeta requestMeta = new RequestMeta(tCommitId);
        TIoTConsensusV2TransferResp preCheckRes = preCheck(tCommitId);
        if (preCheckRes != null) {
          return preCheckRes;
        }

        LOGGER.info(
            "IoTConsensusV2-PipeName-{}: start to receive no.{} event",
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
            TIoTConsensusV2TransferResp resp = loadEvent(req);

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
                "IoTConsensusV2-PipeName-{}: no.{} event get executed because receiver buffer's len >= pipeline, current receiver syncIndex {}, current buffer len {}",
                consensusPipeName,
                tCommitId,
                onSyncedReplicateIndex,
                reqExecutionOrderBuffer.size());
            long startApplyNanos = System.nanoTime();
            metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
            requestMeta.setStartApplyNanos(startApplyNanos);
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            TIoTConsensusV2TransferResp resp = loadEvent(req);

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
                      IOT_CONSENSUS_V2_RECEIVER_MAX_WAITING_TIME_IN_MS, TimeUnit.MILLISECONDS);

              if (isClosed.get()) {
                return IoTConsensusV2ReceiverAgent.closedResp(
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
                      "IoTConsensusV2-PipeName-{}: no.{} event get executed after awaiting timeout, current receiver syncIndex: {}",
                      consensusPipeName,
                      tCommitId,
                      onSyncedReplicateIndex);
                  long startApplyNanos = System.nanoTime();
                  metric.recordDispatchWaitingTimer(startApplyNanos - startDispatchNanos);
                  requestMeta.setStartApplyNanos(startApplyNanos);
                  TIoTConsensusV2TransferResp resp = loadEvent(req);

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
                              TSStatusCode.IOT_CONSENSUS_V2_WAIT_ORDER_TIMEOUT,
                              "Waiting for the previous event times out, returns an error to let the sender retry and continue scheduling."));
                  // TODO: Turn it to debug after GA
                  LOGGER.info(
                      "IoTConsensusV2-{}: Waiting for the previous event times out, current peek {}, current id {}",
                      consensusPipeName,
                      reqExecutionOrderBuffer.first().commitId,
                      tCommitId);
                  return new TIoTConsensusV2TransferResp(status);
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOGGER.warn(
                  "IoTConsensusV2-PipeName-{}: current waiting is interrupted. onSyncedCommitIndex: {}. Exception: ",
                  consensusPipeName,
                  tCommitId.getReplicateIndex(),
                  e);
              // Avoid infinite loop when RPC thread is killed by OS
              return new TIoTConsensusV2TransferResp(
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
          "IoTConsensusV2-PipeName-{}: receiver detected an newer rebootTimes, which indicates the leader has rebooted. receiver will reset all its data.",
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
          "IoTConsensusV2-PipeName-{}: receiver detected an newer pipeTaskRestartTimes, which indicates the pipe task has restarted. receiver will reset all its data.",
          consensusPipeName);
      // since pipe task will resend all data that hasn't synchronized after restarts, it's safe to
      // clear all events in buffer.
      clear(false, false);
      this.pipeTaskRestartTimes = pipeTaskRestartTimes;
    }

    private void onSuccess(TCommitId commitId, boolean isTransferTsFileSeal) {
      LOGGER.info(
          "IoTConsensusV2-PipeName-{}: process no.{} event successfully!",
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

    private TIoTConsensusV2TransferResp deprecatedResp(String msg, TCommitId tCommitId) {
      final TSStatus status =
          new TSStatus(
              RpcUtils.getStatus(
                  TSStatusCode.IOT_CONSENSUS_V2_DEPRECATED_REQUEST,
                  String.format(
                      "IoTConsensusV2 receiver received a deprecated request, which may because %s. Consider to discard it.",
                      msg)));
      LOGGER.info(
          "IoTConsensusV2-PipeName-{}: received a deprecated request-{}, which may because {}. ",
          consensusPipeName,
          tCommitId,
          msg);
      return new TIoTConsensusV2TransferResp(status);
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
