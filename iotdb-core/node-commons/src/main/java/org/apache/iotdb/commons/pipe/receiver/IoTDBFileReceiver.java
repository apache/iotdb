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

package org.apache.iotdb.commons.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;

/**
 * {@link IoTDBFileReceiver} is the parent class of receiver on both configNode and DataNode,
 * handling all the logic of parallel file receiving.
 */
public abstract class IoTDBFileReceiver implements IoTDBReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBFileReceiver.class);
  protected final AtomicReference<File> receiverFileDirWithIdSuffix = new AtomicReference<>();

  // Used to generate transfer id, which is used to identify a receiver thread.
  private static final AtomicLong RECEIVER_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicLong receiverId = new AtomicLong(0);

  // Used to restore the original thread name when the receiver is closed.
  private String originalThreadName;

  protected String username = CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
  protected String password = CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
  protected IAuditEntity userEntity;

  protected long lastSuccessfulLoginTime = Long.MIN_VALUE;

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private File writingFile;
  private RandomAccessFile writingFileWriter;

  protected boolean shouldConvertDataTypeOnTypeMismatch =
      CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE;

  // Used to determine current strategy is sync or async
  protected final AtomicBoolean isUsingAsyncLoadTsFileStrategy = new AtomicBoolean(false);
  protected final AtomicBoolean validateTsFile = new AtomicBoolean(true);

  protected final AtomicBoolean shouldMarkAsPipeRequest = new AtomicBoolean(true);
  protected final AtomicBoolean skipIfNoPrivileges = new AtomicBoolean(false);

  @Override
  public IoTDBSinkRequestVersion getVersion() {
    return IoTDBSinkRequestVersion.VERSION_1;
  }

  protected TPipeTransferResp handleTransferHandshakeV1(final PipeTransferHandshakeV1Req req) {
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
      PipeLogger.log(LOGGER::warn, "Handshake failed, response status = %s.", status);
      return new TPipeTransferResp(status);
    }

    if (originalThreadName == null) {
      originalThreadName = Thread.currentThread().getName();
    }
    receiverId.set(RECEIVER_ID_GENERATOR.incrementAndGet());
    Thread.currentThread()
        .setName(
            String.format(
                "Pipe-Receiver-%s-%s:%s", receiverId.get(), getSenderHost(), getSenderPort()));

    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          RetryUtils.retryOnException(
              () -> {
                FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
                return null;
              });
          LOGGER.info(
              "Receiver id = {}: Original receiver file dir {} was deleted.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (Exception e) {
          PipeLogger.log(
              LOGGER::warn,
              "Receiver id = %s: Failed to delete original receiver file dir %s, because %s.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e.getMessage(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Receiver id = {}: Original receiver file dir {} is not existed. No need to delete.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Current receiver file dir is null. No need to delete.",
            receiverId.get());
      }
    }

    String receiverFileBaseDir;
    File newReceiverDir = null;
    for (int retryTimes = 0; retryTimes <= 1; retryTimes++) {
      try {
        receiverFileBaseDir = getReceiverFileBaseDir();
        if (Objects.isNull(receiverFileBaseDir)) {
          PipeLogger.log(
              LOGGER::warn,
              "Receiver id = %s: Failed to init pipe receiver file folder manager because all disks of folders are full.",
              receiverId.get());
          return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
        }
      } catch (Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to create pipe receiver file folder because all disks of folders are full.",
            receiverId.get(),
            e);
        return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
      }

      try {
        // Create a new receiver file dir
        newReceiverDir = new File(receiverFileBaseDir, Long.toString(receiverId.get()));
        if (newReceiverDir.exists() || newReceiverDir.mkdirs()) {
          receiverFileDirWithIdSuffix.set(newReceiverDir);
          LOGGER.info(
              "Receiver id = {}: Handshake successfully! Sender's host = {}, port = {}. Receiver's file dir = {}.",
              receiverId.get(),
              getSenderHost(),
              getSenderPort(),
              newReceiverDir.getPath());
          return new TPipeTransferResp(RpcUtils.SUCCESS_STATUS);
        }
      } catch (Exception ignored) {
      }
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to create receiver file dir %s.",
          receiverId.get(),
          Objects.nonNull(newReceiverDir) ? newReceiverDir.getPath() : null);
      markFileBaseDirStateAbnormal(receiverFileBaseDir);
    }
    return new TPipeTransferResp(
        RpcUtils.getStatus(
            TSStatusCode.PIPE_HANDSHAKE_ERROR,
            String.format("Failed to create receiver file dir %s.", newReceiverDir.getPath())));
  }

  protected abstract String getReceiverFileBaseDir() throws Exception;

  protected abstract void markFileBaseDirStateAbnormal(String dir);

  protected abstract String getSenderHost();

  protected abstract String getSenderPort();

  protected TPipeTransferResp handleTransferHandshakeV2(final PipeTransferHandshakeV2Req req)
      throws IOException {
    // Reject to handshake if the receiver can not take clusterId from config node.
    final String clusterIdFromConfigNode = getClusterId();
    if (clusterIdFromConfigNode == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              "Receiver can not get clusterId from config node.");
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Handshake failed, response status = %s.",
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the request does not contain sender's clusterId.
    final String clusterIdFromHandshakeRequest =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID);
    if (clusterIdFromHandshakeRequest == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR, "Handshake request does not contain clusterId.");
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Handshake failed, response status = %s.",
          receiverId.get(),
          status);
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
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Handshake failed, response status = %s.",
          receiverId.get(),
          status);
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
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Handshake failed, response status = %s.",
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    }

    long userId = -1;
    String cliHostname = "";

    final String userIdString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID);
    if (userIdString != null) {
      userId = Long.parseLong(userIdString);
    }
    final String usernameString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME);
    if (usernameString != null) {
      username = usernameString;
    }
    final String cliHostnameString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME);
    if (cliHostnameString != null) {
      cliHostname = cliHostnameString;
    }

    userEntity = new UserEntity(userId, username, cliHostname);

    final String passwordString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD);
    if (passwordString != null) {
      password = passwordString;
    }
    final TSStatus status = loginIfNecessary();
    if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Handshake failed because login failed, response status = %s.",
          receiverId.get(),
          status);
      return new TPipeTransferResp(status);
    } else {
      LOGGER.info("Receiver id = {}: User {} login successfully.", receiverId.get(), username);
    }

    final String shouldConvertDataTypeOnTypeMismatchString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH);
    if (shouldConvertDataTypeOnTypeMismatchString != null) {
      shouldConvertDataTypeOnTypeMismatch =
          Boolean.parseBoolean(shouldConvertDataTypeOnTypeMismatchString);
    }

    final String loadTsFileStrategyString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY);
    if (loadTsFileStrategyString != null) {
      isUsingAsyncLoadTsFileStrategy.set(
          Objects.equals(
              PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_ASYNC_VALUE,
              loadTsFileStrategyString));
    }

    validateTsFile.set(
        Boolean.parseBoolean(
            req.getParams()
                .getOrDefault(
                    PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE, "true")));

    shouldMarkAsPipeRequest.set(
        Boolean.parseBoolean(
            req.getParams()
                .getOrDefault(
                    PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST, "true")));

    skipIfNoPrivileges.set(
        Boolean.parseBoolean(
            req.getParams()
                .getOrDefault(PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF, "false")));

    // Handle the handshake request as a v1 request.
    // Here we construct a fake "dataNode" request to valid from v1 validation logic, though
    // it may not require the actual type of the v1 request.
    return handleTransferHandshakeV1(
        new PipeTransferHandshakeV1Req() {
          @Override
          protected PipeRequestType getPlanType() {
            return PipeRequestType.HANDSHAKE_DATANODE_V1;
          }
        }.convertToTPipeTransferReq(timestampPrecision));
  }

  protected abstract String getClusterId();

  protected boolean shouldLogin() {
    final long pipeReceiverLoginPeriodicVerificationIntervalMs =
        PIPE_CONFIG.getPipeReceiverLoginPeriodicVerificationIntervalMs();
    return pipeReceiverLoginPeriodicVerificationIntervalMs >= 0
        && lastSuccessfulLoginTime
            < System.currentTimeMillis() - pipeReceiverLoginPeriodicVerificationIntervalMs;
  }

  protected TSStatus loginIfNecessary() {
    if (shouldLogin()) {
      final TSStatus permissionCheckStatus = login();
      if (permissionCheckStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to login, username = %s, response = %s.",
            receiverId.get(),
            username,
            permissionCheckStatus);
        return permissionCheckStatus;
      } else {
        lastSuccessfulLoginTime = System.currentTimeMillis();
      }
    }
    return StatusUtils.OK;
  }

  protected abstract TSStatus login();

  protected final TPipeTransferResp handleTransferFilePiece(
      final PipeTransferFilePieceReq req,
      final boolean isRequestThroughAirGap,
      final boolean isSingleFile) {
    try {
      updateWritingFileIfNeeded(req.getFileName(), isSingleFile);

      // If the request is through air gap, the sender will resend the file piece from the beginning
      // of the file. So the receiver should reset the offset of the writing file to the beginning
      // of the file.
      if (isRequestThroughAirGap && req.getStartWritingOffset() < writingFileWriter.length()) {
        writingFileWriter.setLength(req.getStartWritingOffset());
      }

      if (!isWritingFileOffsetCorrect(req.getStartWritingOffset())) {
        if (!writingFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
          // If the file is a tsFile, then the content will not be changed for a specific filename.
          // However, for other files (mod, snapshot, etc.) the content varies for the same name in
          // different times, then we must rewrite the file to apply the newest version.
          writingFileWriter.setLength(0);
        }

        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET,
                String.format(
                    "Request sender to reset file reader's offset from %s to %s.",
                    req.getStartWritingOffset(), writingFileWriter.length()));
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: File offset reset requested by receiver, response status = %s.",
            receiverId.get(),
            status);
        return PipeTransferFilePieceResp.toTPipeTransferResp(status, writingFileWriter.length());
      }

      writingFileWriter.write(req.getFilePiece());
      return PipeTransferFilePieceResp.toTPipeTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          "Receiver id = %s: Failed to write file piece from req %s.",
          receiverId.get(),
          req);
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to write file piece, because %s", e.getMessage()));
      try {
        return PipeTransferFilePieceResp.toTPipeTransferResp(
            status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
      } catch (Exception ex) {
        return PipeTransferFilePieceResp.toTPipeTransferResp(status);
      }
    }
  }

  protected final void updateWritingFileIfNeeded(final String fileName, final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(fileName)) {
      return;
    }

    LOGGER.info(
        "Receiver id = {}: Writing file {} is not existed or name is not correct, try to create it. "
            + "Current writing file is {}.",
        receiverId.get(),
        fileName,
        writingFile == null ? "null" : writingFile.getPath());

    closeCurrentWritingFileWriter(!isSingleFile);
    // If there are multiple files we can not delete the current file
    // instead they will be deleted after seal request
    if (writingFile != null && isSingleFile) {
      deleteCurrentWritingFile();
    }

    // Make sure receiver file dir exists
    // This may be useless, because receiver file dir is created when handshake. just in case.
    if (!receiverFileDirWithIdSuffix.get().exists()) {
      if (receiverFileDirWithIdSuffix.get().mkdirs()) {
        LOGGER.info(
            "Receiver id = {}: Receiver file dir {} was created.",
            receiverId.get(),
            receiverFileDirWithIdSuffix.get().getPath());
      } else {
        LOGGER.error(
            "Receiver id = {}: Failed to create receiver file dir {}.",
            receiverId.get(),
            receiverFileDirWithIdSuffix.get().getPath());
      }
    }

    writingFile = new File(receiverFileDirWithIdSuffix.get(), fileName);
    writingFileWriter = new RandomAccessFile(writingFile, "rw");
    LOGGER.info(
        "Receiver id = {}: Writing file {} was created. Ready to write file pieces.",
        receiverId.get(),
        writingFile.getPath());
  }

  private boolean isFileExistedAndNameCorrect(final String fileName) {
    return writingFile != null && writingFile.exists() && writingFile.getName().equals(fileName);
  }

  private void closeCurrentWritingFileWriter(final boolean fsyncBeforeClose) {
    if (writingFileWriter != null) {
      try {
        if (PIPE_CONFIG.getPipeFileReceiverFsyncEnabled() && fsyncBeforeClose) {
          writingFileWriter.getFD().sync();
        }
        writingFileWriter.close();
        LOGGER.info(
            "Receiver id = {}: Current writing file writer {} was closed, length {}.",
            receiverId.get(),
            writingFile == null ? "null" : writingFile.getPath(),
            writingFile == null ? 0 : writingFile.length());
      } catch (final Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to close current writing file writer %s, because %s.",
            receiverId.get(),
            writingFile == null ? "null" : writingFile.getPath(),
            e.getMessage(),
            e);
      }
      writingFileWriter = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Current writing file writer is null. No need to close.",
            receiverId.get());
      }
    }
  }

  private void deleteCurrentWritingFile() {
    if (writingFile != null) {
      deleteFile(writingFile);
      writingFile = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Current writing file is null. No need to delete.", receiverId.get());
      }
    }
  }

  private void deleteFile(final File file) {
    if (file.exists()) {
      try {
        RetryUtils.retryOnException(() -> FileUtils.delete(file));
        LOGGER.info(
            "Receiver id = {}: Original writing file {} was deleted.",
            receiverId.get(),
            file.getPath());
      } catch (final Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to delete original writing file %s, because %s.",
            receiverId.get(),
            file.getPath(),
            e.getMessage(),
            e);
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Original file {} is not existed. No need to delete.",
            receiverId.get(),
            file.getPath());
      }
    }
  }

  private boolean isWritingFileOffsetCorrect(final long offset) throws IOException {
    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Writing file %s's offset is %s, but request sender's offset is %s.",
          receiverId.get(),
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return offsetCorrect;
  }

  protected final TPipeTransferResp handleTransferFileSealV1(final PipeTransferFileSealReqV1 req) {
    try {
      if (!isWritingFileAvailable()) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file, because writing file %s is not available.", writingFile));
        PipeLogger.log(LOGGER::warn, status.getMessage());
        return new TPipeTransferResp(status);
      }

      final TPipeTransferResp resp = checkFinalFileSeal(req.getFileName(), req.getFileLength());
      if (Objects.nonNull(resp)) {
        return resp;
      }

      final String fileAbsolutePath = writingFile.getAbsolutePath();

      // Sync here is necessary to ensure that the data is written to the disk. Or data region may
      // load the file before the data is written to the disk and cause unexpected behavior after
      // system restart. (e.g., empty file in data region's data directory)
      if (PIPE_CONFIG.getPipeFileReceiverFsyncEnabled()) {
        writingFileWriter.getFD().sync();
      }
      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.close();
      writingFileWriter = null;

      // writingFile will be deleted after load if no exception occurs
      writingFile = null;

      final TSStatus status = loadFileV1(req, fileAbsolutePath);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "Receiver id = {}: Seal file {} successfully.", receiverId.get(), fileAbsolutePath);
      } else {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to seal file %s, because %s.",
            receiverId.get(),
            fileAbsolutePath,
            status.getMessage());
      }
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s from req %s.",
          receiverId.get(),
          writingFile,
          req,
          e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod (if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(false);
      deleteCurrentWritingFile();
    }
  }

  // Support null in fileName list, which means that this file is optional and is currently absent
  protected final TPipeTransferResp handleTransferFileSealV2(final PipeTransferFileSealReqV2 req) {
    final List<String> fileNames = req.getFileNames();
    final List<File> files =
        fileNames.stream()
            .map(
                fileName ->
                    Objects.nonNull(fileName)
                        ? new File(receiverFileDirWithIdSuffix.get(), fileName)
                        : null)
            .collect(Collectors.toList());
    try {
      if (!isWritingFileAvailable()) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, because writing file %s is not available.",
                    req.getFileNames(), writingFile));
        PipeLogger.log(LOGGER::warn, status.getMessage());
        return new TPipeTransferResp(status);
      }

      // Any of the transferred files cannot be empty, or else the receiver
      // will not sense this file because no pieces are sent
      for (int i = 0; i < fileNames.size(); ++i) {
        final String fileName = fileNames.get(i);
        if (Objects.nonNull(fileName)) {
          final TPipeTransferResp resp =
              i == fileNames.size() - 1
                  ? checkFinalFileSeal(fileName, req.getFileLengths().get(i))
                  : checkNonFinalFileSeal(files.get(i), fileName, req.getFileLengths().get(i));
          if (Objects.nonNull(resp)) {
            return resp;
          }
        }
      }

      // Sync here is necessary to ensure that the data is written to the disk. Or data region may
      // load the file before the data is written to the disk and cause unexpected behavior after
      // system restart. (e.g., empty file in data region's data directory)
      if (PIPE_CONFIG.getPipeFileReceiverFsyncEnabled()) {
        writingFileWriter.getFD().sync();
      }
      // 1. The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2. The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.close();
      writingFileWriter = null;

      // WritingFile will be deleted after load if no exception occurs
      writingFile = null;

      final List<String> fileAbsolutePaths =
          files.stream()
              .map(file -> Objects.nonNull(file) ? file.getAbsolutePath() : null)
              .collect(Collectors.toList());

      final TSStatus status = loadFileV2(req, fileAbsolutePaths);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "Receiver id = {}: Seal file {} successfully.", receiverId.get(), fileAbsolutePaths);
      } else {
        PipeLogger.log(
            LOGGER::warn,
            "Receiver id = %s: Failed to seal file %s, status is %s.",
            receiverId.get(),
            fileAbsolutePaths,
            status);
      }
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s from req %s.",
          receiverId.get(),
          files,
          req,
          e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", files, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod(if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(false);
      // Clear the directory instead of only deleting the referenced files in seal request
      // to avoid previously undeleted file being redundant when transferring multi files
      IoTDBReceiverAgent.cleanPipeReceiverDir(receiverFileDirWithIdSuffix.get());
    }
  }

  private TPipeTransferResp checkNonFinalFileSeal(
      final File file, final String fileName, final long fileLength) throws IOException {
    if (!file.exists()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s, the file does not exist.", fileName));
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s, because the file does not exist.",
          receiverId.get(),
          fileName);
      return new TPipeTransferResp(status);
    }

    if (fileLength != file.length()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s, because the length of file is not correct. "
              + "The original file has length %s, but receiver file has length %s.",
          receiverId.get(),
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeTransferResp(status);
    }

    return null;
  }

  private TPipeTransferResp checkFinalFileSeal(final String fileName, final long fileLength)
      throws IOException {
    if (!isFileExistedAndNameCorrect(fileName)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because writing file is %s.", fileName, writingFile));
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s, because writing file is %s.",
          receiverId.get(),
          fileName,
          writingFile);
      return new TPipeTransferResp(status);
    }

    if (!isWritingFileOffsetCorrect(fileLength)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(
                  "Failed to seal file %s, because the length of file is not correct. "
                      + "The original file has length %s, but receiver file has length %s.",
                  fileName, fileLength, writingFileWriter.length()));
      PipeLogger.log(
          LOGGER::warn,
          "Receiver id = %s: Failed to seal file %s, because the length of file is not correct. "
              + "The original file has length %s, but receiver file has length %s.",
          receiverId.get(),
          fileName,
          fileLength,
          writingFileWriter.length());
      return new TPipeTransferResp(status);
    }

    return null;
  }

  private boolean isWritingFileAvailable() {
    final boolean isWritingFileAvailable =
        writingFile != null && writingFile.exists() && writingFileWriter != null;
    if (!isWritingFileAvailable) {
      LOGGER.info(
          "Receiver id = {}: Writing file {} is not available. "
              + "Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.",
          receiverId.get(),
          writingFile,
          writingFile == null,
          writingFile != null && writingFile.exists(),
          writingFileWriter == null);
    }
    return isWritingFileAvailable;
  }

  protected abstract TSStatus loadFileV1(
      final PipeTransferFileSealReqV1 req, final String fileAbsolutePath) throws IOException;

  protected abstract TSStatus loadFileV2(
      final PipeTransferFileSealReqV2 req, final List<String> fileAbsolutePaths)
      throws IOException, IllegalPathException;

  @Override
  public synchronized void handleExit() {
    if (writingFileWriter != null) {
      try {
        writingFileWriter.close();
        LOGGER.info(
            "Receiver id = {}: Handling exit: Writing file writer was closed.", receiverId.get());
      } catch (Exception e) {
        LOGGER.warn(
            "Receiver id = {}: Handling exit: Close writing file writer error.",
            receiverId.get(),
            e);
      }
      writingFileWriter = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Handling exit: Writing file writer is null. No need to close.",
            receiverId.get());
      }
    }

    if (writingFile != null) {
      try {
        RetryUtils.retryOnException(() -> FileUtils.delete(writingFile));
        LOGGER.info(
            "Receiver id = {}: Handling exit: Writing file {} was deleted.",
            receiverId.get(),
            writingFile.getPath());
      } catch (Exception e) {
        LOGGER.warn(
            "Receiver id = {}: Handling exit: Delete writing file {} error.",
            receiverId.get(),
            writingFile.getPath(),
            e);
      }
      writingFile = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Handling exit: Writing file is null. No need to delete.",
            receiverId.get());
      }
    }

    // Clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          RetryUtils.retryOnException(
              () -> {
                FileUtils.deleteDirectory(receiverFileDirWithIdSuffix.get());
                return null;
              });
          LOGGER.info(
              "Receiver id = {}: Handling exit: Original receiver file dir {} was deleted.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (Exception e) {
          LOGGER.warn(
              "Receiver id = {}: Handling exit: Delete original receiver file dir {} error.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Receiver id = {}: Handling exit: Original receiver file dir {} does not exist. No need to delete.",
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receiver id = {}: Handling exit: Original receiver file dir is null. No need to delete.",
            receiverId.get());
      }
    }

    // Close the session
    closeSession();

    LOGGER.info("Receiver id = {}: Handling exit: Receiver exited.", receiverId.get());

    if (originalThreadName != null) {
      Thread.currentThread().setName(originalThreadName);
    }
  }

  protected abstract void closeSession();
}
