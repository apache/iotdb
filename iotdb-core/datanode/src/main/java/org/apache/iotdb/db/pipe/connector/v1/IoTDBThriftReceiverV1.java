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

package org.apache.iotdb.db.pipe.connector.v1;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.receiver.IoTDBThriftReceiver;
import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBThriftReceiverV1 implements IoTDBThriftReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftReceiverV1.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String RECEIVER_FILE_BASE_DIR = IOTDB_CONFIG.getPipeReceiverFileDir();
  private final AtomicReference<File> receiverFileDirWithIdSuffix = new AtomicReference<>();

  // Used to generate transfer id, which is used to identify a receiver thread.
  private static final AtomicLong RECEIVER_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong receiverId = new AtomicLong(0);

  private File writingFile;
  private RandomAccessFile writingFileWriter;

  @Override
  public synchronized TPipeTransferResp receive(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    final short rawRequestType = req.getType();
    if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
      switch (PipeRequestType.valueOf(rawRequestType)) {
        case HANDSHAKE:
          return handleTransferHandshake(PipeTransferHandshakeReq.fromTPipeTransferReq(req));
        case TRANSFER_INSERT_NODE:
          return handleTransferInsertNode(
              PipeTransferInsertNodeReq.fromTPipeTransferReq(req), partitionFetcher, schemaFetcher);
        case TRANSFER_TABLET:
          return handleTransferTablet(
              PipeTransferTabletReq.fromTPipeTransferReq(req), partitionFetcher, schemaFetcher);
        case TRANSFER_FILE_PIECE:
          return handleTransferFilePiece(PipeTransferFilePieceReq.fromTPipeTransferReq(req));
        case TRANSFER_FILE_SEAL:
          return handleTransferFileSeal(
              PipeTransferFileSealReq.fromTPipeTransferReq(req), partitionFetcher, schemaFetcher);
        default:
          break;
      }
    }

    // unknown request type, which means the request can not be handled by this receiver,
    // maybe the version of the receiver is not compatible with the sender
    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("Unknown PipeRequestType %s.", rawRequestType));
    LOGGER.warn("Unknown PipeRequestType, response status = {}.", status);
    return new TPipeTransferResp(status);
  }

  private TPipeTransferResp handleTransferHandshake(PipeTransferHandshakeReq req) {
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

    // create a new receiver file dir
    final File newReceiverDir = new File(RECEIVER_FILE_BASE_DIR, Long.toString(receiverId.get()));
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

  private TPipeTransferResp handleTransferInsertNode(
      PipeTransferInsertNodeReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    return new TPipeTransferResp(
        executeStatement(req.constructStatement(), partitionFetcher, schemaFetcher));
  }

  private TPipeTransferResp handleTransferTablet(
      PipeTransferTabletReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    InsertTabletStatement statement = req.constructStatement();
    return new TPipeTransferResp(
        statement.isEmpty()
            ? RpcUtils.SUCCESS_STATUS
            : executeStatement(statement, partitionFetcher, schemaFetcher));
  }

  private TPipeTransferResp handleTransferFilePiece(PipeTransferFilePieceReq req) {
    try {
      updateWritingFileIfNeeded(req.getFileName());

      if (!isWritingFileOffsetCorrect(req.getStartWritingOffset())) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET,
                String.format(
                    "Request sender to reset file reader's offset from %s to %s.",
                    req.getStartWritingOffset(), writingFileWriter.length()));
        LOGGER.warn("File offset reset requested by receiver, response status = {}.", status);
        return PipeTransferFilePieceResp.toTPipeTransferResp(status, writingFileWriter.length());
      }

      writingFileWriter.write(req.getFilePiece());
      return PipeTransferFilePieceResp.toTPipeTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (Exception e) {
      LOGGER.warn(String.format("Failed to write file piece from req %s.", req), e);
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to write file piece, because %s", e.getMessage()));
      try {
        return PipeTransferFilePieceResp.toTPipeTransferResp(
            status, PipeTransferFilePieceResp.ERROR_END_OFFSET);
      } catch (IOException ex) {
        return PipeTransferFilePieceResp.toTPipeTransferResp(status);
      }
    }
  }

  private void updateWritingFileIfNeeded(String fileName) throws IOException {
    if (isFileExistedAndNameCorrect(fileName)) {
      return;
    }

    LOGGER.info(
        "Writing file {} is not existed or name is not correct, try to create it. "
            + "Current writing file is {}.",
        fileName,
        writingFile == null ? "null" : writingFile.getPath());

    closeCurrentWritingFileWriter();
    deleteCurrentWritingFile();

    // make sure receiver file dir exists
    // this may be useless, because receiver file dir is created when handshake. just in case.
    if (!receiverFileDirWithIdSuffix.get().exists()) {
      if (receiverFileDirWithIdSuffix.get().mkdirs()) {
        LOGGER.info(
            "Receiver file dir {} was created.", receiverFileDirWithIdSuffix.get().getPath());
      } else {
        LOGGER.error(
            "Failed to create receiver file dir {}.", receiverFileDirWithIdSuffix.get().getPath());
      }
    }

    writingFile = new File(receiverFileDirWithIdSuffix.get(), fileName);
    writingFileWriter = new RandomAccessFile(writingFile, "rw");
    LOGGER.info("Writing file {} was created. Ready to write file pieces.", writingFile.getPath());
  }

  private boolean isFileExistedAndNameCorrect(String fileName) {
    return writingFile != null && writingFile.getName().equals(fileName);
  }

  private void closeCurrentWritingFileWriter() {
    if (writingFileWriter != null) {
      try {
        writingFileWriter.close();
        LOGGER.info(
            "Current writing file writer {} was closed.",
            writingFile == null ? "null" : writingFile.getPath());
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to close current writing file writer {}, because {}.",
            writingFile == null ? "null" : writingFile.getPath(),
            e.getMessage());
      }
      writingFileWriter = null;
    } else {
      LOGGER.info("Current writing file writer is null. No need to close.");
    }
  }

  private void deleteCurrentWritingFile() {
    if (writingFile != null) {
      if (writingFile.exists()) {
        try {
          Files.delete(writingFile.toPath());
          LOGGER.info("Original writing file {} was deleted.", writingFile.getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "Failed to delete original writing file {}, because {}.",
              writingFile.getPath(),
              e.getMessage());
        }
      } else {
        LOGGER.info("Original file {} is not existed. No need to delete.", writingFile.getPath());
      }
      writingFile = null;
    } else {
      LOGGER.info("Current writing file is null. No need to delete.");
    }
  }

  private boolean isWritingFileOffsetCorrect(long offset) throws IOException {
    final boolean offsetCorrect = writingFileWriter.length() == offset;
    if (!offsetCorrect) {
      LOGGER.warn(
          "Writing file {}'s offset is {}, but request sender's offset is {}.",
          writingFile.getPath(),
          writingFileWriter.length(),
          offset);
    }
    return offsetCorrect;
  }

  private TPipeTransferResp handleTransferFileSeal(
      PipeTransferFileSealReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    try {
      if (!isWritingFileAvailable()) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file, because writing file %s is not available.",
                    req.getFileName()));
        LOGGER.warn(status.getMessage());
        return new TPipeTransferResp(status);
      }

      if (!isFileExistedAndNameCorrect(req.getFileName())) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, but writing file is %s.",
                    req.getFileName(), writingFile));
        LOGGER.warn(status.getMessage());
        return new TPipeTransferResp(status);
      }

      if (!isWritingFileOffsetCorrect(req.getFileLength())) {
        final TSStatus status =
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Failed to seal file %s, because the length of file is not correct. "
                        + "The original file has length %s, but receiver file has length %s.",
                    req.getFileName(), req.getFileLength(), writingFileWriter.length()));
        LOGGER.warn(status.getMessage());
        return new TPipeTransferResp(status);
      }

      final String fileAbsolutePath = writingFile.getAbsolutePath();
      final LoadTsFileStatement statement = new LoadTsFileStatement(fileAbsolutePath);

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

      statement.setDeleteAfterLoad(true);
      statement.setVerifySchema(true);
      statement.setAutoCreateDatabase(false);

      final TSStatus status = executeStatement(statement, partitionFetcher, schemaFetcher);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "Seal file {} successfully. Receiver id is {}.", fileAbsolutePath, receiverId.get());
      } else {
        LOGGER.warn(
            "Failed to seal file {}, because {}. Receiver id is {}.",
            fileAbsolutePath,
            status.getMessage(),
            receiverId.get());
      }
      return new TPipeTransferResp(status);
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Failed to seal file %s from req %s. Receiver id is %d.",
              writingFile, req, receiverId.get()),
          e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Failed to seal file %s because %s", writingFile, e.getMessage())));
    } finally {
      // If the writing file is not sealed successfully, the writing file will be deleted.
      // All pieces of the writing file should be retransmitted by the sender.
      closeCurrentWritingFileWriter();
      deleteCurrentWritingFile();
    }
  }

  private boolean isWritingFileAvailable() {
    final boolean isWritingFileAvailable =
        writingFile != null && writingFile.exists() && writingFileWriter != null;
    if (!isWritingFileAvailable) {
      LOGGER.info(
          "Writing file {} is not available. Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.",
          writingFile,
          writingFile == null,
          writingFile != null && writingFile.exists(),
          writingFileWriter == null);
    }
    return isWritingFileAvailable;
  }

  private TSStatus executeStatement(
      Statement statement, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    if (statement == null) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR, "Execute null statement.");
    }

    final long queryId = SessionManager.getInstance().requestQueryId();
    final ExecutionResult result =
        Coordinator.getInstance()
            .execute(
                statement,
                queryId,
                null,
                "",
                partitionFetcher,
                schemaFetcher,
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "failed to execute statement, statement: {}, result status is: {}",
          statement,
          result.status);
    }
    return result.status;
  }

  @Override
  public synchronized void handleExit() {
    if (writingFileWriter != null) {
      try {
        writingFileWriter.close();
        LOGGER.info("IoTDBThriftReceiverV1#handleExit: writing file writer was closed.");
      } catch (Exception e) {
        LOGGER.warn("IoTDBThriftReceiverV1#handleExit: close writing file writer error.", e);
      }
      writingFileWriter = null;
    } else {
      LOGGER.info(
          "IoTDBThriftReceiverV1#handleExit: writing file writer is null. No need to close.");
    }

    if (writingFile != null) {
      try {
        Files.delete(writingFile.toPath());
        LOGGER.info(
            "IoTDBThriftReceiverV1#handleExit: writing file {} was deleted.",
            writingFile.getPath());
      } catch (Exception e) {
        LOGGER.warn(
            "IoTDBThriftReceiverV1#handleExit: delete file {} error.", writingFile.getPath());
      }
      writingFile = null;
    } else {
      LOGGER.info("IoTDBThriftReceiverV1#handleExit: writing file is null. No need to delete.");
    }

    // clear the original receiver file dir if exists
    if (receiverFileDirWithIdSuffix.get() != null) {
      if (receiverFileDirWithIdSuffix.get().exists()) {
        try {
          Files.delete(receiverFileDirWithIdSuffix.get().toPath());
          LOGGER.info(
              "IoTDBThriftReceiverV1#handleExit: original receiver file dir {} was deleted.",
              receiverFileDirWithIdSuffix.get().getPath());
        } catch (IOException e) {
          LOGGER.warn(
              "IoTDBThriftReceiverV1#handleExit: delete original receiver file dir {} error.",
              receiverFileDirWithIdSuffix.get().getPath());
        }
      } else {
        LOGGER.info(
            "IoTDBThriftReceiverV1#handleExit: original receiver file dir {} does not exist. No need to delete.",
            receiverFileDirWithIdSuffix.get().getPath());
      }
      receiverFileDirWithIdSuffix.set(null);
    } else {
      LOGGER.info(
          "IoTDBThriftReceiverV1#handleExit: original receiver file dir is null. No need to delete.");
    }

    LOGGER.info("IoTDBThriftReceiverV1#handleExit: receiver exited.");
  }

  @Override
  public IoTDBThriftConnectorRequestVersion getVersion() {
    return IoTDBThriftConnectorRequestVersion.VERSION_1;
  }
}
