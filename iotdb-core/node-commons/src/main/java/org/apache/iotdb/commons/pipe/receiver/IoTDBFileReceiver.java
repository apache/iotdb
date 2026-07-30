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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.log.LoggerPeriodicalLogReducer;
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
import java.nio.file.Path;
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
  protected boolean hasPipeHandshakeCredential = false;

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
    hasPipeHandshakeCredential = false;

    if (!CommonDescriptor.getInstance()
        .getConfig()
        .getTimestampPrecision()
        .equals(req.getTimestampPrecision())) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              String.format(
                  PipeMessages.RECEIVER_TIMESTAMP_PRECISION_MISMATCH,
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
                  req.getTimestampPrecision()));
      PipeLogger.log(LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED, status);
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

    // Handshake restarts the transfer session. Reset the current writing state before recycling the
    // old receiver dir, otherwise the old file handle can survive across handshakes.
    resetCurrentWritingFileState();

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
              PipeMessages.RECEIVER_ORIGINAL_DIR_DELETED,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (Exception e) {
          PipeLogger.log(
              LOGGER::warn,
              e,
              PipeMessages.RECEIVER_FAILED_DELETE_ORIGINAL_DIR,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e.getMessage());
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              PipeMessages.RECEIVER_ORIGINAL_DIR_NOT_EXIST,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_DIR_NULL_NO_DELETE, receiverId.get());
      }
    }

    String receiverFileBaseDir;
    File newReceiverDir = null;
    for (int retryTimes = 0; retryTimes <= 1; retryTimes++) {
      try {
        receiverFileBaseDir = getReceiverFileBaseDir();
        if (Objects.isNull(receiverFileBaseDir)) {
          if (LoggerPeriodicalLogReducer.shouldLog(PipeMessages.RECEIVER_FAILED_INIT_FOLDER_FULL)) {
            PipeLogger.log(
                LOGGER::warn, PipeMessages.RECEIVER_FAILED_INIT_FOLDER_FULL, receiverId.get());
          }
          return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
        }
      } catch (Exception e) {
        if (LoggerPeriodicalLogReducer.shouldLog(
            PipeMessages.RECEIVER_FAILED_CREATE_FOLDER_FULL
                + e.getClass().getName()
                + e.getMessage())) {
          PipeLogger.log(
              LOGGER::warn, e, PipeMessages.RECEIVER_FAILED_CREATE_FOLDER_FULL, receiverId.get());
        }
        return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
      }

      try {
        // Create a new receiver file dir
        newReceiverDir = new File(receiverFileBaseDir, Long.toString(receiverId.get()));
        if (newReceiverDir.exists() || newReceiverDir.mkdirs()) {
          receiverFileDirWithIdSuffix.set(newReceiverDir);
          LOGGER.info(
              PipeMessages.RECEIVER_HANDSHAKE_SUCCESS,
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
          PipeMessages.RECEIVER_FAILED_CREATE_DIR,
          receiverId.get(),
          Objects.nonNull(newReceiverDir) ? newReceiverDir.getPath() : null);
      markFileBaseDirStateAbnormal(receiverFileBaseDir);
    }
    return new TPipeTransferResp(
        RpcUtils.getStatus(
            TSStatusCode.PIPE_HANDSHAKE_ERROR,
            String.format(
                PipeMessages.RECEIVER_FAILED_CREATE_DIR_STATUS, newReceiverDir.getPath())));
  }

  protected abstract String getReceiverFileBaseDir() throws Exception;

  protected abstract void markFileBaseDirStateAbnormal(String dir);

  protected abstract String getSenderHost();

  protected abstract String getSenderPort();

  protected TPipeTransferResp handleTransferHandshakeV2(final PipeTransferHandshakeV2Req req)
      throws IOException {
    hasPipeHandshakeCredential = false;

    // Reject to handshake if the receiver can not take clusterId from config node.
    final String clusterIdFromConfigNode = getClusterId();
    if (clusterIdFromConfigNode == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR, PipeMessages.RECEIVER_CANNOT_GET_CLUSTER_ID);
      PipeLogger.log(
          LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED_WITH_ID, receiverId.get(), status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the request does not contain sender's clusterId.
    final String clusterIdFromHandshakeRequest =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID);
    if (clusterIdFromHandshakeRequest == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR, PipeMessages.RECEIVER_NO_CLUSTER_ID_IN_REQUEST);
      PipeLogger.log(
          LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED_WITH_ID, receiverId.get(), status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the receiver and sender are from the same cluster.
    if (Objects.equals(clusterIdFromConfigNode, clusterIdFromHandshakeRequest)) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              String.format(PipeMessages.RECEIVER_SAME_CLUSTER, clusterIdFromHandshakeRequest));
      PipeLogger.log(
          LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED_WITH_ID, receiverId.get(), status);
      return new TPipeTransferResp(status);
    }

    // Reject to handshake if the request does not contain timestampPrecision.
    final String timestampPrecision =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION);
    if (timestampPrecision == null) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR, PipeMessages.RECEIVER_NO_TIMESTAMP_PRECISION);
      PipeLogger.log(
          LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED_WITH_ID, receiverId.get(), status);
      return new TPipeTransferResp(status);
    }

    long userId = -1;
    String cliHostname = "";

    final String userIdString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID);
    if (userIdString != null) {
      userId = Long.parseLong(userIdString);
    }
    final String cliHostnameString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME);
    if (cliHostnameString != null) {
      cliHostname = cliHostnameString;
    }

    final String usernameString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME);
    final String passwordString =
        req.getParams().get(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD);
    if (usernameString == null || passwordString == null) {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.NOT_LOGIN, "Pipe handshake missing username or password."));
    }

    username = usernameString;
    password = passwordString;
    userEntity = new UserEntity(userId, username, cliHostname);
    hasPipeHandshakeCredential = true;

    final TSStatus status;
    try {
      status = login();
    } catch (final Exception e) {
      hasPipeHandshakeCredential = false;
      throw e;
    }
    if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      hasPipeHandshakeCredential = false;
      PipeLogger.log(
          LOGGER::warn, PipeMessages.RECEIVER_HANDSHAKE_FAILED_LOGIN, receiverId.get(), status);
      return new TPipeTransferResp(status);
    } else {
      lastSuccessfulLoginTime = System.currentTimeMillis();
      LOGGER.info(PipeMessages.RECEIVER_USER_LOGIN_SUCCESS, receiverId.get(), username);
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
    final TPipeTransferResp handshakeResp =
        handleTransferHandshakeV1(
            new PipeTransferHandshakeV1Req() {
              @Override
              protected PipeRequestType getPlanType() {
                return PipeRequestType.HANDSHAKE_DATANODE_V1;
              }
            }.convertToTPipeTransferReq(timestampPrecision));
    hasPipeHandshakeCredential =
        handshakeResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    return handshakeResp;
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
            PipeMessages.RECEIVER_FAILED_LOGIN,
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

  protected TSStatus getNotLoggedInStatus() {
    return RpcUtils.getStatus(
        TSStatusCode.NOT_LOGIN,
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  protected TSStatus getUnsupportedHandshakeV1Status() {
    return RpcUtils.getStatus(
        TSStatusCode.PIPE_HANDSHAKE_ERROR,
        "Pipe handshake V1 is no longer supported. Please use handshake V2 with username and password.");
  }

  protected abstract TSStatus login();

  protected final TPipeTransferResp handleTransferFilePiece(
      final PipeTransferFilePieceReq req,
      final boolean isRequestThroughAirGap,
      final boolean isSingleFile) {
    try (final AutoCloseable ignored = tryAllocateMemoryForFilePiece(req)) {
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
                    PipeMessages.REQUEST_SENDER_RESET_OFFSET,
                    req.getStartWritingOffset(),
                    writingFileWriter.length()));
        PipeLogger.log(
            LOGGER::warn, PipeMessages.RECEIVER_FILE_OFFSET_RESET, receiverId.get(), status);
        return PipeTransferFilePieceResp.toTPipeTransferResp(status, writingFileWriter.length());
      }

      writingFileWriter.write(req.getFilePiece());
      return PipeTransferFilePieceResp.toTPipeTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      final TSStatus status =
          getReceiverTemporaryUnavailableStatus(
              "receiving pipe file piece", getFilePieceSizeInBytes(req), e);
      PipeLogger.log(
          LOGGER::warn, e, PipeMessages.RECEIVER_FAILED_WRITE_FILE_PIECE, receiverId.get(), req);
      try {
        return PipeTransferFilePieceResp.toTPipeTransferResp(
            status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
      } catch (Exception ex) {
        return PipeTransferFilePieceResp.toTPipeTransferResp(status);
      }
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn, e, PipeMessages.RECEIVER_FAILED_WRITE_FILE_PIECE, receiverId.get(), req);
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(PipeMessages.FAILED_TO_WRITE_FILE_PIECE, e.getMessage()));
      try {
        return PipeTransferFilePieceResp.toTPipeTransferResp(
            status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
      } catch (Exception ex) {
        return PipeTransferFilePieceResp.toTPipeTransferResp(status);
      }
    }
  }

  protected AutoCloseable tryAllocateMemoryForFilePiece(final PipeTransferFilePieceReq req)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return () -> {};
  }

  protected TSStatus getReceiverTemporaryUnavailableStatus(
      final String action,
      final long requestedMemorySizeInBytes,
      final PipeRuntimeOutOfMemoryCriticalException e) {
    return new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
        .setMessage(
            String.format(
                PipeMessages.RECEIVER_TEMPORARILY_OUT_OF_MEMORY_FORMAT,
                action,
                requestedMemorySizeInBytes,
                e.getMessage()));
  }

  private static long getFilePieceSizeInBytes(final PipeTransferFilePieceReq req) {
    return req.getFilePiece() == null ? 0 : req.getFilePiece().length;
  }

  protected final void updateWritingFileIfNeeded(final String fileName, final boolean isSingleFile)
      throws IOException {
    if (isFileExistedAndNameCorrect(fileName)) {
      return;
    }

    LOGGER.debug(
        PipeMessages.RECEIVER_WRITING_FILE_NOT_EXIST,
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
        LOGGER.debug(
            PipeMessages.RECEIVER_FILE_DIR_CREATED,
            receiverId.get(),
            receiverFileDirWithIdSuffix.get().getPath());
      } else {
        LOGGER.error(
            PipeMessages.RECEIVER_FAILED_CREATE_FILE_DIR,
            receiverId.get(),
            receiverFileDirWithIdSuffix.get().getPath());
      }
    }
    final Path targetPath = resolveReceiverFilePath(fileName);

    writingFile = targetPath.toFile();
    writingFileWriter = new RandomAccessFile(writingFile, "rw");
    LOGGER.debug(
        PipeMessages.RECEIVER_WRITING_FILE_CREATED, receiverId.get(), writingFile.getPath());
  }

  private boolean isFileExistedAndNameCorrect(final String fileName) {
    try {
      return writingFile != null
          && writingFile.exists()
          && receiverFileDirWithIdSuffix.get() != null
          && writingFile
              .toPath()
              .toAbsolutePath()
              .normalize()
              .equals(resolveReceiverFilePath(fileName));
    } catch (final IOException e) {
      PipeLogger.log(
          LOGGER::warn, e, PipeMessages.RECEIVER_ILLEGAL_FILENAME, receiverId.get(), fileName);
      return false;
    }
  }

  private Path resolveReceiverFilePath(final String fileName) throws IOException {
    try {
      return PipeReceiverFilePathUtils.resolveFilePath(
          receiverFileDirWithIdSuffix.get().toPath(), fileName);
    } catch (final IOException e) {
      LOGGER.error(PipeMessages.RECEIVER_PATH_TRAVERSAL, receiverId.get(), fileName);
      throw e;
    }
  }

  private void closeCurrentWritingFileWriter(final boolean fsyncBeforeClose) {
    if (writingFileWriter != null) {
      try {
        if (PIPE_CONFIG.getPipeFileReceiverFsyncEnabled() && fsyncBeforeClose) {
          writingFileWriter.getFD().sync();
        }
        writingFileWriter.close();
        LOGGER.info(
            PipeMessages.RECEIVER_WRITER_CLOSED,
            receiverId.get(),
            writingFile == null ? "null" : writingFile.getPath(),
            writingFile == null ? 0 : writingFile.length());
      } catch (final Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            e,
            PipeMessages.RECEIVER_FAILED_CLOSE_WRITER,
            receiverId.get(),
            writingFile == null ? "null" : writingFile.getPath(),
            e.getMessage());
      }
      writingFileWriter = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_WRITER_NULL, receiverId.get());
      }
    }
  }

  private void deleteCurrentWritingFile() {
    if (writingFile != null) {
      deleteFile(writingFile);
      writingFile = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_FILE_NULL, receiverId.get());
      }
    }
  }

  private void resetCurrentWritingFileState() {
    closeCurrentWritingFileWriter(false);
    writingFile = null;
  }

  private void deleteFile(final File file) {
    if (file.exists()) {
      try {
        RetryUtils.retryOnException(() -> FileUtils.delete(file));
        LOGGER.info(PipeMessages.RECEIVER_ORIGINAL_FILE_DELETED, receiverId.get(), file.getPath());
      } catch (final Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            e,
            PipeMessages.RECEIVER_FAILED_DELETE_FILE,
            receiverId.get(),
            file.getPath(),
            e.getMessage());
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            PipeMessages.RECEIVER_ORIGINAL_FILE_NOT_EXIST, receiverId.get(), file.getPath());
      }
    }
  }

  private boolean isWritingFileOffsetCorrect(final long offset) throws IOException {
    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.RECEIVER_FILE_OFFSET_MISMATCH,
          receiverId.get(),
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return offsetCorrect;
  }

  protected final TPipeTransferResp handleTransferFileSealV1(final PipeTransferFileSealReqV1 req) {
    File sealedWritingFile = null;
    boolean shouldDeleteSealedFile = true;
    try {
      if (!isWritingFileAvailable()) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(PipeMessages.FAILED_TO_SEAL_FILE_NOT_AVAILABLE, writingFile));
        PipeLogger.log(LOGGER::warn, status.getMessage());
        return new TPipeTransferResp(status);
      }

      final TPipeTransferResp resp = checkFinalFileSeal(req.getFileName(), req.getFileLength());
      if (Objects.nonNull(resp)) {
        return resp;
      }

      sealedWritingFile = writingFile;
      final String fileAbsolutePath = sealedWritingFile.getAbsolutePath();

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

      // Clear the reference before loading so the next file transfer can not reuse the same path.
      // The loader owns cleanup after a successful load.
      writingFile = null;

      final TSStatus status = loadFileV1(req, fileAbsolutePath);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        shouldDeleteSealedFile = false;
        LOGGER.debug(PipeMessages.RECEIVER_SEAL_FILE_SUCCESS, receiverId.get(), fileAbsolutePath);
      } else {
        PipeLogger.log(
            LOGGER::warn,
            PipeMessages.RECEIVER_FAILED_SEAL_FILE,
            receiverId.get(),
            fileAbsolutePath,
            status.getMessage());
      }
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_FROM_REQ,
          receiverId.get(),
          writingFile,
          req);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(PipeMessages.FAILED_TO_SEAL_FILE, writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file and its mod (if exists) should be retransmitted by the
      // sender.
      closeCurrentWritingFileWriter(false);
      if (shouldDeleteSealedFile) {
        if (writingFile != null) {
          deleteCurrentWritingFile();
        } else if (sealedWritingFile != null) {
          deleteFile(sealedWritingFile);
        }
      }
    }
  }

  // Support null in fileName list, which means that this file is optional and is currently absent
  protected final TPipeTransferResp handleTransferFileSealV2(final PipeTransferFileSealReqV2 req) {
    final List<String> fileNames = req.getFileNames();
    try {
      final List<File> files =
          fileNames.stream()
              .map(
                  fileName -> {
                    if (Objects.isNull(fileName)) {
                      return null;
                    }
                    try {
                      return resolveReceiverFilePath(fileName).toFile();
                    } catch (final IOException e) {
                      throw new IllegalArgumentException(e);
                    }
                  })
              .collect(Collectors.toList());

      if (!isWritingFileAvailable()) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    PipeMessages.FAILED_TO_SEAL_FILE_MULTI, req.getFileNames(), writingFile));
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
        LOGGER.debug(PipeMessages.RECEIVER_SEAL_FILE_SUCCESS, receiverId.get(), fileAbsolutePaths);
      } else {
        PipeLogger.log(
            LOGGER::warn,
            PipeMessages.RECEIVER_FAILED_SEAL_FILE_STATUS,
            receiverId.get(),
            fileAbsolutePaths,
            status);
      }
      return new TPipeTransferResp(status);
    } catch (final Exception e) {
      final Throwable rootCause = e instanceof IllegalArgumentException ? e.getCause() : e;
      PipeLogger.log(
          LOGGER::warn,
          rootCause,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_FROM_REQ,
          receiverId.get(),
          fileNames,
          req);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(
                  PipeMessages.FAILED_TO_SEAL_FILE,
                  fileNames,
                  rootCause == null ? e.getMessage() : rootCause.getMessage())));
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
              String.format(PipeMessages.FAILED_TO_SEAL_FILE_NOT_EXIST, fileName));
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_NOT_EXIST,
          receiverId.get(),
          fileName);
      return new TPipeTransferResp(status);
    }

    if (fileLength != file.length()) {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format(
                  PipeMessages.FAILED_TO_SEAL_FILE_LENGTH_INCORRECT,
                  fileName,
                  fileLength,
                  file.length()));
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_LENGTH_INCORRECT,
          receiverId.get(),
          fileName,
          fileLength,
          file.length());
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
                  PipeMessages.FAILED_TO_SEAL_FILE_WRITING_FILE_MISMATCH, fileName, writingFile));
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_WRITING,
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
                  PipeMessages.FAILED_TO_SEAL_FILE_LENGTH_INCORRECT,
                  fileName,
                  fileLength,
                  writingFileWriter.length()));
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.RECEIVER_FAILED_SEAL_FILE_LENGTH_INCORRECT,
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
          PipeMessages.RECEIVER_FILE_NOT_AVAILABLE,
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
        LOGGER.info(PipeMessages.RECEIVER_EXIT_WRITER_CLOSED, receiverId.get());
      } catch (Exception e) {
        LOGGER.warn(PipeMessages.RECEIVER_EXIT_CLOSE_WRITER_ERROR, receiverId.get(), e);
      }
      writingFileWriter = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_EXIT_WRITER_NULL, receiverId.get());
      }
    }

    if (writingFile != null) {
      try {
        RetryUtils.retryOnException(() -> FileUtils.delete(writingFile));
        LOGGER.info(
            PipeMessages.RECEIVER_EXIT_FILE_DELETED, receiverId.get(), writingFile.getPath());
      } catch (Exception e) {
        LOGGER.warn(
            PipeMessages.RECEIVER_EXIT_DELETE_FILE_ERROR,
            receiverId.get(),
            writingFile.getPath(),
            e);
      }
      writingFile = null;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_EXIT_FILE_NULL, receiverId.get());
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
              PipeMessages.RECEIVER_EXIT_DIR_DELETED,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (Exception e) {
          LOGGER.warn(
              PipeMessages.RECEIVER_EXIT_DELETE_DIR_ERROR,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              PipeMessages.RECEIVER_EXIT_DIR_NOT_EXIST,
              receiverId.get(),
              receiverFileDirWithIdSuffix.get().getPath());
        }
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.RECEIVER_EXIT_DIR_NULL, receiverId.get());
      }
    }

    // Close the session
    closeSession();

    LOGGER.info(PipeMessages.RECEIVER_EXITED, receiverId.get());

    if (originalThreadName != null) {
      Thread.currentThread().setName(originalThreadName);
    }
  }

  protected abstract void closeSession();
}
