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
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.pipe.agent.receiver.IoTDBThriftReceiver;
import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class IoTDBThriftReceiverV1 implements IoTDBThriftReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftReceiverV1.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String RECEIVER_FILE_DIR = IOTDB_CONFIG.getPipeReceiverFileDir();

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
    return new TPipeTransferResp(
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("Unknown transfer type %s.", rawRequestType)));
  }

  private TPipeTransferResp handleTransferHandshake(PipeTransferHandshakeReq req) {
    if (!CommonDescriptor.getInstance()
        .getConfig()
        .getTimestampPrecision()
        .equals(req.getTimestampPrecision())) {
      String msg =
          String.format(
              "IoTDB receiver's timestamp precision %s, connector's timestamp precision %s. validation fails.",
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
              req.getTimestampPrecision());
      LOGGER.warn(msg);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_HANDSHAKE_ERROR, msg));
    }

    LOGGER.info("Handshake successfully.");
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
        return PipeTransferFilePieceResp.toTPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET,
                String.format(
                    "request sender reset file reader's offset from %s to %s.",
                    req.getStartWritingOffset(), writingFileWriter.length())),
            writingFileWriter.length());
      }

      writingFileWriter.write(req.getFilePiece());
      return PipeTransferFilePieceResp.toTPipeTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (Exception e) {
      LOGGER.warn(String.format("failed to write file piece from req %s.", req), e);
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("failed to write file piece, because %s", e.getMessage()));
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

    if (writingFileWriter != null) {
      writingFileWriter.close();
      writingFileWriter = null;
    }
    if (writingFile != null && writingFile.exists()) {
      if (writingFile.delete()) {
        LOGGER.info("original file {} was deleted.", writingFile.getPath());
      } else {
        LOGGER.warn("failed to delete original file {}.", writingFile.getPath());
      }
      writingFile = null;
    }

    final File receiveDir = new File(RECEIVER_FILE_DIR);
    if (!receiveDir.exists()) {
      if (receiveDir.mkdirs()) {
        LOGGER.info("receiver file dir {} was created.", receiveDir.getPath());
      } else {
        LOGGER.warn("failed to create receiver file dir {}.", receiveDir.getPath());
      }
    }
    writingFile = new File(RECEIVER_FILE_DIR, fileName);
    writingFileWriter = new RandomAccessFile(writingFile, "rw");
    LOGGER.info("start to write transferring file {}.", writingFile.getPath());
  }

  private boolean isFileExistedAndNameCorrect(String fileName) {
    return writingFile != null && writingFile.getName().equals(fileName);
  }

  private boolean isWritingFileOffsetCorrect(long offset) throws IOException {
    return writingFileWriter.length() == offset;
  }

  private TPipeTransferResp handleTransferFileSeal(
      PipeTransferFileSealReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    try {
      if (!isWritingFileAvailable()) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "failed to seal file, because writing file %s is not available.",
                    req.getFileName())));
      }

      if (!isFileExistedAndNameCorrect(req.getFileName())) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "failed to seal file %s, but writing file is %s.",
                    req.getFileName(), writingFile)));
      }

      if (!isWritingFileOffsetCorrect(req.getFileLength())) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "failed to seal file because the length of file is not correct. "
                        + "the original file has length %s, but receiver file has length %s.",
                    req.getFileLength(), writingFileWriter.length())));
      }

      final LoadTsFileStatement statement = new LoadTsFileStatement(writingFile.getAbsolutePath());

      // 1.The writing file writer must be closed, otherwise it may cause concurrent errors during
      // the process of loading tsfile when parsing tsfile.
      //
      // 2.The writing file must be set to null, otherwise if the next passed tsfile has the same
      // name as the current tsfile, it will bypass the judgment logic of
      // updateWritingFileIfNeeded#isFileExistedAndNameCorrect, and continue to write to the already
      // loaded file. Since the writing file writer has already been closed, it will throw a Stream
      // Close exception.
      writingFileWriter.close();
      writingFile = null;

      statement.setDeleteAfterLoad(true);
      statement.setVerifySchema(true);
      statement.setAutoCreateDatabase(false);
      return new TPipeTransferResp(executeStatement(statement, partitionFetcher, schemaFetcher));
    } catch (IOException e) {
      LOGGER.warn(String.format("failed to seal file %s from req %s.", writingFile, req), e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("failed to seal file %s because %s", writingFile, e.getMessage())));
    }
  }

  private boolean isWritingFileAvailable() {
    return writingFile != null && writingFile.exists() && writingFileWriter != null;
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
    try {
      if (writingFileWriter != null) {
        writingFileWriter.close();
      }
      if (writingFile != null && !writingFile.delete()) {
        LOGGER.warn(
            "IoTDBThriftReceiverV1#handleExit: delete file {} error.", writingFile.getPath());
      }
    } catch (IOException e) {
      LOGGER.warn("IoTDBThriftReceiverV1#handleExit: meeting errors on handleExit().", e);
    }
  }

  @Override
  public IoTDBThriftConnectorRequestVersion getVersion() {
    return IoTDBThriftConnectorRequestVersion.VERSION_1;
  }
}
