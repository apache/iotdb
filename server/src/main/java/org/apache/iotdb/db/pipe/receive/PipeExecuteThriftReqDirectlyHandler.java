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

package org.apache.iotdb.db.pipe.receive;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.receive.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.receive.request.PipeValidateHandshakeReq;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class PipeExecuteThriftReqDirectlyHandler implements PipeThriftReqHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeExecuteThriftReqDirectlyHandler.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private File writingFile;
  private RandomAccessFile writer;

  @Override
  public TPipeHandshakeResp handleHandshakeReq(TPipeHandshakeReq req) {
    PipeValidateHandshakeReq validateReq = PipeValidateHandshakeReq.fromTPipeHandshakeReq(req);
    if (!config.getIoTDBMajorVersion().equals(validateReq.iotdbVersion)) {
      String msg =
          String.format(
              "IoTDB version %s, handshake pipe version %s, validate error.",
              config.getIoTDBMajorVersion(), validateReq.iotdbVersion);
      LOGGER.warn(msg);
      return new TPipeHandshakeResp(RpcUtils.getStatus(TSStatusCode.PIPE_HANDSHAKE_ERROR, msg));
    } else if (!config.getTimestampPrecision().equals(validateReq.getTimestampPrecision())) {
      String msg =
          String.format(
              "IoTDB timestamp precision %s, handshake timestamp precision %s, validate error.",
              config.getTimestampPrecision(), validateReq.getTimestampPrecision());
      LOGGER.warn(msg);
      return new TPipeHandshakeResp(RpcUtils.getStatus(TSStatusCode.PIPE_HANDSHAKE_ERROR, msg));
    }
    return new TPipeHandshakeResp(RpcUtils.SUCCESS_STATUS);
  }

  @Override
  public TPipeHeartbeatResp handleHeartbeatReq(TPipeHeartbeatReq req) {
    throw new NotImplementedException("Not implement for pipe heartbeat thrift request.");
  }

  @Override
  public TPipeTransferResp handleTransferReq(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    switch (req.getType()) {
      case 0:
        return handleTransferInsertNode(
            PipeTransferInsertNodeReq.fromTPipeTransferReq(req), partitionFetcher, schemaFetcher);
      case 1:
        return handleTransferFilePiece(PipeTransferFilePieceReq.fromTPipeTransferReq(req));
      case 2:
        return handleTransferFileSeal(
            PipeTransferFileSealReq.fromTPipeTransferReq(req), partitionFetcher, schemaFetcher);
      default:
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format("Unknown transfer type %s.", req.getType())));
    }
  }

  private TPipeTransferResp handleTransferInsertNode(
      PipeTransferInsertNodeReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    return new TPipeTransferResp(
        executeStatement(req.constructStatement(), partitionFetcher, schemaFetcher));
  }

  private TPipeTransferResp handleTransferFilePiece(PipeTransferFilePieceReq req) {
    try {
      validateAndCreateFile(req.getFileName());
      if (!validateOffset(req.getStartOffset())) {
        return new PipeTransferFilePieceResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_REDIRECTION,
                String.format(
                    "Redirection offset from %s to %s.", req.getStartOffset(), writer.length())),
            writer.length());
      }

      writer.write(req.getBody());
      return new PipeTransferFilePieceResp(RpcUtils.SUCCESS_STATUS, writer.length());
    } catch (IOException e) {
      LOGGER.warn(String.format("Write file piece form req %s error.", req), e);
      return new PipeTransferFilePieceResp(
              RpcUtils.getStatus(
                  TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                  String.format("Write file piece error, because %s", e.getMessage())),
              PipeTransferFilePieceResp.ERROR_END_OFFSET)
          .toTPipeTransferResp();
    }
  }

  private void validateAndCreateFile(String fileName) throws IOException {
    if (writingFile == null || !writingFile.getName().equals(fileName)) {
      if (writer != null) {
        writer.close();
      }
      if (writingFile != null && writingFile.exists()) {
        writingFile.deleteOnExit();
        LOGGER.info(String.format("Delete origin file %s.", writingFile.getPath()));
      }
      writingFile =
          new File(PipeConfig.getInstance().getReceiveFileDir() + File.separator + fileName);
      writer = new RandomAccessFile(writingFile, "rw");
      LOGGER.info(String.format("Start to write transferring file %s.", writingFile.getPath()));
    }
  }

  private boolean validateOffset(long offset) throws IOException {
    return offset == writer.length();
  }

  private TPipeTransferResp handleTransferFileSeal(
      PipeTransferFileSealReq req,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    try {
      if (!validateFileName(req.getFileName())) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Seal wrong file, want to seal %s, but %s exits.",
                    req.getFileName(), writingFile)));
      }
      if (!validateFileLength(req.getFileLength())) {
        return new TPipeTransferResp(
            RpcUtils.getStatus(
                TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
                String.format(
                    "Seal file length error, origin file has length %s, but receive file has length %s.",
                    req.getFileLength(), writer.length())));
      }

      writingFile = null;
      writer.close();

      LoadTsFileStatement statement = new LoadTsFileStatement(writingFile.getAbsolutePath());
      statement.setDeleteAfterLoad(true);
      statement.setVerifySchema(true);
      statement.setAutoCreateDatabase(false);
      return new TPipeTransferResp(executeStatement(statement, partitionFetcher, schemaFetcher));
    } catch (IOException e) {
      LOGGER.warn(String.format("Seal file %s form req %s error.", writingFile, req), e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TRANSFER_FILE_ERROR,
              String.format("Seal file error, because %s", e.getMessage())));
    }
  }

  private boolean validateFileName(String fileName) {
    return writingFile != null && writingFile.getName().equals(fileName);
  }

  private boolean validateFileLength(long fileLength) throws IOException {
    return writer.length() == fileLength;
  }

  private TSStatus executeStatement(
      Statement statement, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    long queryId = SessionManager.getInstance().requestQueryId();
    ExecutionResult result =
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
          "Execute Statement error, statement: {}, result status is: {}", statement, result.status);
    }
    return result.status;
  }
}
