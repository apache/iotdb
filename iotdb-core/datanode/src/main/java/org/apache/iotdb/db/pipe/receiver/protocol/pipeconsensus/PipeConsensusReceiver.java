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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.response.PipeConsensusTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealWithModReq;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// TODO: 如：1,1 1,2 1,3 1,4 1,5 / 1,6 1,7 1,8（follower 得想办法知道 leader
// 是否发满了/前置请求是否发完了）：发送端等待事件超时后尝试握手
public class PipeConsensusReceiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiver.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private final RequestExecutor requestExecutor = new RequestExecutor();
  private final PipeConsensus pipeConsensus;
  private final ConsensusGroupId consensusGroupId;
  // Used to buffer TsFile when transfer TsFile asynchronously.
  // TODO: move to pipe config
  private static final String[] RECEIVER_FILE_BASE_DIRS =
      IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDirs();
  private final AtomicReference<File> receiverFileDirWithIdSuffix = new AtomicReference<>();
  private final List<TsFileTransferDiskBuffer> tsFileTransferDiskBuffers = new ArrayList<>();
  private FolderManager folderManager;

  public PipeConsensusReceiver(PipeConsensus pipeConsensus, ConsensusGroupId consensusGroupId) {
    this.pipeConsensus = pipeConsensus;
    this.consensusGroupId = consensusGroupId;
    for (int i = 0; i < COMMON_CONFIG.getPipeConsensusEventBufferSize(); i++) {
      tsFileTransferDiskBuffers.add(new TsFileTransferDiskBuffer());
    }

    try {
      this.folderManager =
          new FolderManager(
              Arrays.asList(RECEIVER_FILE_BASE_DIRS), DirectoryStrategyType.SEQUENCE_STRATEGY);
      initiateTsFileBufferFolder();
    } catch (DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipeConsensus receiver file folders allocation strategy because all disks of folders are full.",
          e);
    }
  }

  /**
   * This method cannot be set to synchronize. Receive events can be concurrent since reqBuffer but
   * load event must be synchronized.
   */
  public TPipeConsensusTransferResp receive(final TPipeConsensusTransferReq req) {
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
          {
            // Just take a place in requestExecutor's buffer, the further seal request will remove
            // its place from buffer.
            Object ignore = requestExecutor.onRequest(req, true);
            return loadEvent(req);
          }
          // TODO: check memory when logging wal(in further version)
        case TRANSFER_TABLET_RAW:
        case TRANSFER_TABLET_BINARY:
        case TRANSFER_TABLET_INSERT_NODE:
          // TODO: support batch transfer(in further version)
        case TRANSFER_TABLET_BATCH:
        case TRANSFER_TS_FILE_SEAL:
        case TRANSFER_TS_FILE_SEAL_WITH_MOD:
        default:
          return requestExecutor.onRequest(req, false);
      }
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
      String message = String.format("PipeConsensus: unexpected consensusGroupId %s", groupId);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), message));
    }
    if (impl.isReadOnly()) {
      String message =
          String.format(
              "PipeConsensus-ConsensusGroupId-%s: fail to receive because system is read-only.",
              groupId);
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(message);
      }
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), message));
    }
    if (!impl.isActive()) {
      String message =
          String.format(
              "PipeConsensus-ConsensusGroupId-%s: fail to receive because peer is inactive and not ready.",
              groupId);
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(message);
      }
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), message));
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
          case TRANSFER_TABLET_RAW:
            // PipeConsensus doesn't expect to handle rawTabletEvent.
            LOGGER.error("PipeConsensus Unknown PipeRequestType: do not support tablet raw!");
            return new TPipeConsensusTransferResp(
                new TSStatus(TSStatusCode.PIPE_CONSENSUS_UNSUPPORTED_EVENT.getStatusCode()));
          case TRANSFER_TABLET_BINARY:
            return handleTransferTabletBinary(
                PipeConsensusTabletBinaryReq.fromTPipeConsensusTransferReq(req));
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
          "PipeConsensus-ConsensusGroupId-{}: Unknown PipeRequestType, response status = {}.",
          consensusGroupId.getId(),
          status);
      return new TPipeConsensusTransferResp(status);
    } catch (Exception e) {
      final String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn("PipeConsensus-ConsensusGroupId-{}: {}", consensusGroupId.getId(), error, e);
      return new TPipeConsensusTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeConsensusTransferResp handleTransferTabletInsertNode(
      final PipeConsensusTabletInsertNodeReq req) throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    return new TPipeConsensusTransferResp(impl.write(req.getInsertNode()));
  }

  private TPipeConsensusTransferResp handleTransferTabletBinary(
      final PipeConsensusTabletBinaryReq req) throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    return new TPipeConsensusTransferResp(impl.write(req.convertToInsertNode()));
  }

  private TPipeConsensusTransferResp handleTransferFilePiece(
      final PipeConsensusTransferFilePieceReq req, final boolean isSingleFile) {
    TsFileTransferDiskBuffer diskBuffer = prepareCorrespondingBuffer(req.getCommitId());

    try {
      updateWritingFileIfNeeded(diskBuffer, req.getFileName(), isSingleFile);
      final File writingFile = diskBuffer.getWritingFile();
      final RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

      if (isWritingFileOffsetNonCorrect(diskBuffer, req.getStartWritingOffset())) {
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
            "PipeConsensus-ConsensusGroupId-{}: File offset reset requested by receiver, response status = {}.",
            consensusGroupId.getId(),
            status);
        return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(
            status, writingFileWriter.length());
      }

      writingFileWriter.write(req.getFilePiece());
      writingFileWriter.getFD().sync();
      return PipeConsensusTransferFilePieceResp.toTPipeConsensusTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (Exception e) {
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to write file piece from req {}.",
          consensusGroupId.getId(),
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
    TsFileTransferDiskBuffer diskBuffer = prepareCorrespondingBuffer(req.getCommitId());
    File writingFile = diskBuffer.getWritingFile();
    RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    try {

      if (isWritingFileNonAvailable(diskBuffer)) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file, because writing file %s is not available.", writingFile));
        LOGGER.warn(status.getMessage());
        return new TPipeConsensusTransferResp(status);
      }

      final TPipeConsensusTransferResp resp =
          checkFinalFileSeal(diskBuffer, req.getFileName(), req.getFileLength());
      if (Objects.nonNull(resp)) {
        return resp;
      }

      final String fileAbsolutePath = writingFile.getAbsolutePath();

      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.getFD().sync();
      writingFileWriter.close();
      diskBuffer.setWritingFileWriter(null);

      // writingFile will be deleted after load if no exception occurs
      diskBuffer.setWritingFile(null);

      final TSStatus status = loadFileToDateRegion(fileAbsolutePath);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // if transfer success, disk buffer will be released.
        diskBuffer.releaseSelf();
        LOGGER.info(
            "PipeConsensus-ConsensusGroupId-{}: Seal file {} successfully.",
            consensusGroupId.getId(),
            fileAbsolutePath);
      } else {
        LOGGER.warn(
            "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, because {}.",
            consensusGroupId.getId(),
            fileAbsolutePath,
            status.getMessage());
      }
      return new TPipeConsensusTransferResp(status);
    } catch (IOException e) {
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {} from req {}.",
          consensusGroupId.getId(),
          writingFile,
          req,
          e);
      return new TPipeConsensusTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod (if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(diskBuffer);
      deleteCurrentWritingFile(diskBuffer);
    }
  }

  private TPipeConsensusTransferResp handleTransferFileSealWithMods(
      final PipeConsensusTsFileSealWithModReq req) {
    TsFileTransferDiskBuffer diskBuffer = prepareCorrespondingBuffer(req.getCommitId());
    File writingFile = diskBuffer.getWritingFile();
    RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    final List<File> files =
        req.getFileNames().stream()
            .map(fileName -> new File(receiverFileDirWithIdSuffix.get(), fileName))
            .collect(Collectors.toList());
    try {
      if (isWritingFileNonAvailable(diskBuffer)) {
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
                    diskBuffer, req.getFileNames().get(i), req.getFileLengths().get(i))
                : checkNonFinalFileSeal(
                    diskBuffer,
                    files.get(i),
                    req.getFileNames().get(i),
                    req.getFileLengths().get(i));
        if (Objects.nonNull(resp)) {
          return resp;
        }
      }

      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.getFD().sync();
      writingFileWriter.close();
      diskBuffer.setWritingFileWriter(null);

      // WritingFile will be deleted after load if no exception occurs
      diskBuffer.setWritingFile(null);

      final List<String> fileAbsolutePaths =
          files.stream().map(File::getAbsolutePath).collect(Collectors.toList());

      // only load mods
      final TSStatus status = loadFileToDateRegion(fileAbsolutePaths.get(1));
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // if transfer success, disk buffer will be released.
        diskBuffer.releaseSelf();
        LOGGER.info(
            "PipeConsensus-ConsensusGroupId-{}: Seal file with mods {} successfully.",
            consensusGroupId.getId(),
            fileAbsolutePaths);
      } else {
        LOGGER.warn(
            "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, status is {}.",
            consensusGroupId.getId(),
            fileAbsolutePaths,
            status);
      }
      return new TPipeConsensusTransferResp(status);
    } catch (Exception e) {
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {} from req {}.",
          consensusGroupId.getId(),
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
      closeCurrentWritingFileWriter(diskBuffer);
      // Clear the directory instead of only deleting the referenced files in seal request
      // to avoid previously undeleted file being redundant when transferring multi files
      IoTDBReceiverAgent.cleanPipeReceiverDir(receiverFileDirWithIdSuffix.get());
    }
  }

  private TPipeConsensusTransferResp checkNonFinalFileSeal(
      final TsFileTransferDiskBuffer diskBuffer,
      final File file,
      final String fileName,
      final long fileLength)
      throws IOException {
    final RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    if (!file.exists()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s, the file does not exist.", fileName));
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, because the file does not exist.",
          consensusGroupId.getId(),
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
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusGroupId.getId(),
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeConsensusTransferResp(status);
    }

    return null;
  }

  private TSStatus loadFileToDateRegion(String filePath) throws IOException {
    // TODO(sc)
    return null;
  }

  private boolean isWritingFileNonAvailable(TsFileTransferDiskBuffer diskBuffer) {
    File writingFile = diskBuffer.getWritingFile();
    RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    final boolean isWritingFileAvailable =
        writingFile != null && writingFile.exists() && writingFileWriter != null;
    if (!isWritingFileAvailable) {
      LOGGER.info(
          "PipeConsensus-ConsensusGroupId-{}: Writing file {} is not available. "
              + "Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.",
          consensusGroupId.getId(),
          writingFile,
          writingFile == null,
          writingFile != null && writingFile.exists(),
          writingFileWriter == null);
    }
    return !isWritingFileAvailable;
  }

  private TPipeConsensusTransferResp checkFinalFileSeal(
      final TsFileTransferDiskBuffer diskBuffer, final String fileName, final long fileLength)
      throws IOException {
    final File writingFile = diskBuffer.getWritingFile();
    final RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    if (!isFileExistedAndNameCorrect(diskBuffer, fileName)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because writing file is %s.", fileName, writingFile));
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, because writing file is {}.",
          consensusGroupId.getId(),
          fileName,
          writingFile);
      return new TPipeConsensusTransferResp(status);
    }

    if (isWritingFileOffsetNonCorrect(diskBuffer, fileLength)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Failed to seal file {}, because the length of file is not correct. "
              + "The original file has length {}, but receiver file has length {}.",
          consensusGroupId.getId(),
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeConsensusTransferResp(status);
    }

    return null;
  }

  private boolean isFileExistedAndNameCorrect(
      TsFileTransferDiskBuffer diskBuffer, String fileName) {
    final File writingFile = diskBuffer.getWritingFile();
    return writingFile != null && writingFile.getName().equals(fileName);
  }

  private boolean isWritingFileOffsetNonCorrect(
      TsFileTransferDiskBuffer diskBuffer, final long offset) throws IOException {
    final File writingFile = diskBuffer.getWritingFile();
    final RandomAccessFile writingFileWriter = diskBuffer.getWritingFileWriter();

    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      LOGGER.warn(
          "PipeConsensus-ConsensusGroupId-{}: Writing file {}'s offset is {}, but request sender's offset is {}.",
          consensusGroupId.getId(),
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return !offsetCorrect;
  }

  private void closeCurrentWritingFileWriter(TsFileTransferDiskBuffer diskBuffer) {
    if (diskBuffer.getWritingFileWriter() != null) {
      try {
        diskBuffer.getWritingFileWriter().close();
        LOGGER.info(
            "PipeConsensus-ConsensusGroupId-{}: Current writing file writer {} was closed.",
            consensusGroupId.getId(),
            diskBuffer.getWritingFile() == null ? "null" : diskBuffer.getWritingFile().getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "PipeConsensus-ConsensusGroupId-{}: Failed to close current writing file writer {}, because {}.",
            consensusGroupId.getId(),
            diskBuffer.getWritingFile() == null ? "null" : diskBuffer.getWritingFile().getPath(),
            e.getMessage(),
            e);
      }
      diskBuffer.setWritingFileWriter(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-ConsensusGroupId-{}: Current writing file writer is null. No need to close.",
            consensusGroupId.getId());
      }
    }
  }

  private void deleteFile(File file) {
    if (file.exists()) {
      try {
        FileUtils.delete(file);
        LOGGER.info(
            "PipeConsensus-ConsensusGroupId-{}: Original writing file {} was deleted.",
            consensusGroupId.getId(),
            file.getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "PipeConsensus-ConsensusGroupId-{}: Failed to delete original writing file {}, because {}.",
            consensusGroupId.getId(),
            file.getPath(),
            e.getMessage(),
            e);
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-ConsensusGroupId-{}: Original file {} is not existed. No need to delete.",
            consensusGroupId.getId(),
            file.getPath());
      }
    }
  }

  private void deleteCurrentWritingFile(TsFileTransferDiskBuffer diskBuffer) {
    if (diskBuffer.getWritingFile() != null) {
      deleteFile(diskBuffer.getWritingFile());
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-ConsensusGroupId-{}: Current writing file is null. No need to delete.",
            consensusGroupId.getId());
      }
    }
  }

  private void updateWritingFileIfNeeded(
      final TsFileTransferDiskBuffer diskBuffer, final String fileName, final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(diskBuffer, fileName)) {
      return;
    }

    LOGGER.info(
        "PipeConsensus-ConsensusGroupId-{}: Writing file {} is not existed or name is not correct, try to create it. "
            + "Current writing file is {}.",
        consensusGroupId.getId(),
        fileName,
        diskBuffer.getWritingFile() == null ? "null" : diskBuffer.getWritingFile().getPath());

    closeCurrentWritingFileWriter(diskBuffer);
    // If there are multiple files we can not delete the current file
    // instead they will be deleted after seal request
    if (diskBuffer.getWritingFile() != null && isSingleFile) {
      deleteCurrentWritingFile(diskBuffer);
    }

    // Make sure receiver file dir exists
    // This may be useless, because receiver file dir is created when receiver is initiated. just in
    // case.
    if (!receiverFileDirWithIdSuffix.get().exists()) {
      if (receiverFileDirWithIdSuffix.get().mkdirs()) {
        LOGGER.info(
            "PipeConsensus-ConsensusGroupId-{}: Receiver file dir {} was created.",
            consensusGroupId.getId(),
            receiverFileDirWithIdSuffix.get().getPath());
      } else {
        LOGGER.error(
            "PipeConsensus-ConsensusGroupId-{}: Failed to create receiver file dir {}.",
            consensusGroupId.getId(),
            receiverFileDirWithIdSuffix.get().getPath());
      }
    }

    diskBuffer.setWritingFile(new File(receiverFileDirWithIdSuffix.get(), fileName));
    diskBuffer.setWritingFileWriter(new RandomAccessFile(diskBuffer.getWritingFile(), "rw"));
    LOGGER.info(
        "PipeConsensus-ConsensusGroupId-{}: Writing file {} was created. Ready to write file pieces.",
        consensusGroupId.getId(),
        diskBuffer.getWritingFile().getPath());
  }

  private String getReceiverFileBaseDir() throws DiskSpaceInsufficientException {
    // Get next receiver file base dir by folder manager
    return Objects.isNull(folderManager) ? null : folderManager.getNextFolder();
  }

  private void initiateTsFileBufferFolder() throws DiskSpaceInsufficientException {
    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
          LOGGER.info(
              "PipeConsensus-ConsensusGroupId-{}: Original receiver file dir {} was deleted successfully.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "PipeConsensus-ConsensusGroupId-{}: Failed to delete original receiver file dir {}, because {}.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e.getMessage(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-ConsensusGroupId-{}: Original receiver file dir {} is not existed. No need to delete.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      LOGGER.debug(
          "PipeConsensus-ConsensusGroupId-{}: Current receiver file dir is null. No need to delete.",
          consensusGroupId.getId());
    }

    // initiate receiverFileDirWithIdSuffix
    try {
      final String receiverFileBaseDir = getReceiverFileBaseDir();
      if (Objects.isNull(receiverFileBaseDir)) {
        LOGGER.warn(
            "Failed to init pipeConsensus receiver file folder manager because all disks of folders are full.");
        throw new DiskSpaceInsufficientException(Arrays.asList(RECEIVER_FILE_BASE_DIRS));
      }
      // Create a new receiver file dir
      final File newReceiverDir = new File(receiverFileBaseDir, consensusGroupId.toString());
      if (!newReceiverDir.exists() && !newReceiverDir.mkdirs()) {
        LOGGER.warn(
            "PipeConsensus-ConsensusGroupId-{}: Failed to create receiver file dir {}.",
            newReceiverDir.getPath(),
            consensusGroupId.getId());
        throw new DiskSpaceInsufficientException(Arrays.asList(RECEIVER_FILE_BASE_DIRS));
      }
      receiverFileDirWithIdSuffix.set(newReceiverDir);

    } catch (Exception e) {
      LOGGER.warn(
          "Failed to init pipeConsensus receiver file folder manager because all disks of folders are full.",
          e);
      throw e;
    }
  }

  private TsFileTransferDiskBuffer prepareCorrespondingBuffer(TCommitId commitId) {
    Optional<TsFileTransferDiskBuffer> diskBuffer;
    diskBuffer =
        tsFileTransferDiskBuffers.stream()
            .filter(item -> Objects.equals(commitId, item.getCommitIdOfCorrespondingHolderEvent()))
            .findFirst();

    // If the TsFileInsertionEvent is first using diskBuffer, we will find the first available
    // buffer for it.
    if (!diskBuffer.isPresent()) {
      diskBuffer = tsFileTransferDiskBuffers.stream().filter(item -> !item.isUsed()).findFirst();
      // We don't need to check diskBuffer.isPresent() here. Since diskBuffers' length is equals to
      // ReqExecutor's buffer, so the diskBuffer is always present.
      diskBuffer.get().setUsed(true);
      diskBuffer.get().setCommitIdOfCorrespondingHolderEvent(commitId);
    }

    return diskBuffer.get();
  }

  public PipeConsensusRequestVersion getVersion() {
    return PipeConsensusRequestVersion.VERSION_1;
  }

  public synchronized void handleExit() {
    // clean the diskBuffers
    tsFileTransferDiskBuffers.forEach(
        diskBuffer -> {
          // close file writer
          if (diskBuffer.getWritingFileWriter() != null) {
            try {
              diskBuffer.getWritingFileWriter().close();
              LOGGER.info(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Writing file writer was closed.",
                  consensusGroupId.getId());
            } catch (Exception e) {
              LOGGER.warn(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Close writing file writer error.",
                  consensusGroupId.getId(),
                  e);
            }
            diskBuffer.setWritingFileWriter(null);
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Writing file writer is null. No need to close.",
                  consensusGroupId.getId());
            }
          }

          // close file
          if (diskBuffer.getWritingFile() != null) {
            try {
              FileUtils.delete(diskBuffer.getWritingFile());
              LOGGER.info(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Writing file {} was deleted.",
                  consensusGroupId.getId(),
                  diskBuffer.getWritingFile().getPath());
            } catch (Exception e) {
              LOGGER.warn(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Delete writing file {} error.",
                  consensusGroupId.getId(),
                  diskBuffer.getWritingFile().getPath(),
                  e);
            }
            diskBuffer.setWritingFile(null);
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "PipeConsensus-ConsensusGroupId-{}: Handling exit: Writing file is null. No need to delete.",
                  consensusGroupId.getId());
            }
          }

          // release disk buffer
          diskBuffer.releaseSelf();
        });

    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
          LOGGER.info(
              "PipeConsensus-ConsensusGroupId-{}: Handling exit: Original receiver file dir {} was deleted.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "PipeConsensus-ConsensusGroupId-{}: Handling exit: Delete original receiver file dir {} error.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "PipeConsensus-ConsensusGroupId-{}: Handling exit: Original receiver file dir {} does not exist. No need to delete.",
              consensusGroupId.getId(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus-ConsensusGroupId-{}: Handling exit: Original receiver file dir is null. No need to delete.",
            consensusGroupId.getId());
      }
    }

    LOGGER.info(
        "PipeConsensus-ConsensusGroupId-{}: Handling exit: Receiver exited.",
        consensusGroupId.getId());
  }

  private static class TsFileTransferDiskBuffer {
    // whether this buffer is used. this will be updated when first transfer tsFile piece or
    // when transfer seal.
    private boolean isUsed = false;
    // If isUsed is true, this variable will be set to the TCommitId of holderEvent
    private TCommitId commitIdOfCorrespondingHolderEvent;
    private File writingFile;
    private RandomAccessFile writingFileWriter;

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

    public void releaseSelf() {
      this.isUsed = false;
      this.commitIdOfCorrespondingHolderEvent = null;
    }
  }

  /**
   * An executor component to ensure all events sent from connector can be loaded in sequence,
   * although events can arrive receiver in a random sequence.
   */
  private class RequestExecutor {
    // An ordered set that buffers transfer request, whose length is not larger than
    // PIPE_CONSENSUS_EVENT_BUFFER_SIZE.
    // Here we use set is to avoid duplicate events being received in some special cases
    private final TreeSet<WrappedRequest> reqBuffer;
    private final Lock lock;
    private final Condition condition;
    private long onSyncedCommitIndex = -1;
    private int connectorRebootTimes = 1;

    public RequestExecutor() {
      reqBuffer =
          new TreeSet<>(
              Comparator.comparingInt(WrappedRequest::getRebootTime)
                  .thenComparingLong(WrappedRequest::getCommitIndex));
      lock = new ReentrantLock();
      condition = lock.newCondition();
    }

    private void onSuccess(long nextSyncedCommitIndex) {
      LOGGER.info("Debug only: process no.{} event successfully!", nextSyncedCommitIndex);
      reqBuffer.pollFirst();
      onSyncedCommitIndex = nextSyncedCommitIndex;
    }

    private TPipeConsensusTransferResp onRequest(
        final TPipeConsensusTransferReq req, final boolean isTransferTsFilePiece) {
      LOGGER.info(
          "Debug only: no.{} event try to acquire lock", req.getCommitId().getCommitIndex());
      lock.lock();
      try {
        WrappedRequest wrappedReq = new WrappedRequest(req);
        LOGGER.info("Debug only: start process no.{} event", wrappedReq.getCommitIndex());
        // if a req is deprecated, we will discard it
        // This case may happen in this scenario: leader has transferred {1,2} and is intending to
        // transfer {3, 4, 5, 6}. And in one moment, follower has received {4, 5, 6}, {3} is still
        // transferring due to some network latency.
        // At this time, leader restarts, and it will resend {3, 4, 5, 6} with incremental
        // rebootTimes. If the {3} sent before the leader restart arrives after the follower
        // receives
        // the request with incremental rebootTimes, the {3} sent before the leader restart needs to
        // be discarded.
        if (wrappedReq.getRebootTime() < connectorRebootTimes) {
          final TSStatus status =
              new TSStatus(
                  RpcUtils.getStatus(
                      TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                      "PipeConsensus receiver received a deprecated request, which may be sent before the connector restart. Consider to discard it"));
          return new TPipeConsensusTransferResp(status);
        }
        // Judge whether connector has rebooted or not, if the rebootTimes increases compared to
        // connectorRebootTimes, need to reset receiver because connector has been restarted.
        if (wrappedReq.getRebootTime() > connectorRebootTimes) {
          resetWithNewestRebootTime(wrappedReq.getRebootTime());
        }
        reqBuffer.add(wrappedReq);
        // TsFilePieceTransferEvent will not enter further procedure, it just holds a place in
        // buffer. Only after the corresponding sealing event is processed, this event can be
        // dequeued.
        if (isTransferTsFilePiece) {
          return null;
        }

        if (reqBuffer.size() >= COMMON_CONFIG.getPipeConsensusEventBufferSize()
            && !reqBuffer.first().equals(wrappedReq)) {
          // If reqBuffer is full and current thread do not hold the reqBuffer's peek, this req
          // is not supposed to be processed. So current thread should notify the corresponding
          // threads to process the peek.
          condition.signalAll();
        }

        // Polling to process
        while (true) {
          if (reqBuffer.first().equals(wrappedReq)
              && wrappedReq.getCommitIndex() == onSyncedCommitIndex + 1) {
            // If current req is supposed to be process, load this event through
            // DataRegionStateMachine.
            TPipeConsensusTransferResp resp = loadEvent(req);

            // Only when event apply is successful and what is transmitted is not TsFilePiece, req
            // will be removed from the buffer and onSyncedCommitIndex will be updated. Because pipe
            // will transfer multi reqs with same commitId in a single TsFileInsertionEvent, only
            // when the last seal req is applied, we can discard this event.
            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              onSuccess(onSyncedCommitIndex + 1);
            }
            return resp;
          }

          if (reqBuffer.size() >= COMMON_CONFIG.getPipeConsensusEventBufferSize()
              && reqBuffer.first().equals(wrappedReq)) {
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            TPipeConsensusTransferResp resp = loadEvent(req);

            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              onSuccess(wrappedReq.getCommitIndex());
              // signal all other reqs that may wait for this event
              condition.signalAll();
            }
            return resp;
          } else {
            // if the req is not supposed to be processed and reqBuffer is not full, current thread
            // should wait until reqBuffer is full, which indicates the receiver has received all
            // the requests from the connector without duplication or leakage.
            try {
              LOGGER.info(
                  "Debug only: no.{} event waiting on the lock...",
                  req.getCommitId().getCommitIndex());
              condition.await(
                  COMMON_CONFIG.getPipeConsensusReceiverMaxWaitingTimeForEventsInMs(),
                  TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              LOGGER.warn(
                  "current waiting is interrupted. onSyncedCommitIndex: {}. Exception: ",
                  wrappedReq.getCommitIndex(),
                  e);
              Thread.currentThread().interrupt();
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
    private void resetWithNewestRebootTime(int connectorRebootTimes) {
      this.reqBuffer.clear();
      this.onSyncedCommitIndex = -1;
      // sync the follower's connectorRebootTimes with connector's actual rebootTimes
      this.connectorRebootTimes = connectorRebootTimes;
    }
  }

  /**
   * Wrapped TPipeConsensusTransferReq for RequestExecutor.reqBuffer in order to save memory
   * allocation. We don’t really need to hold a reference to TPipeConsensusTransferReq here, because
   * we only need the commitId information of TPipeConsensusTransferReq in the
   * RequestExecutor.reqBuffer.
   */
  private static class WrappedRequest {
    final int rebootTime;
    final long commitIndex;

    public WrappedRequest(TPipeConsensusTransferReq req) {
      this.rebootTime = req.getCommitId().getRebootTimes();
      this.commitIndex = req.getCommitId().getCommitIndex();
    }

    public int getRebootTime() {
      return rebootTime;
    }

    public long getCommitIndex() {
      return commitIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WrappedRequest that = (WrappedRequest) o;
      return rebootTime == that.rebootTime && commitIndex == that.commitIndex;
    }
  }
}
