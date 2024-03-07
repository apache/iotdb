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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link IoTDBFileReceiver} is the parent class of receiver on both configNode and DataNode,
 * handling all the logic of parallel file receiving.
 */
public abstract class IoTDBFileReceiver implements IoTDBReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBFileReceiver.class);
  private final AtomicReference<File> receiverFileDirWithIdSuffix = new AtomicReference<>();

  // Used to generate transfer id, which is used to identify a receiver thread.
  private static final AtomicLong RECEIVER_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong receiverId = new AtomicLong(0);

  private File writingFile;
  private RandomAccessFile writingFileWriter;

  @Override
  public IoTDBConnectorRequestVersion getVersion() {
    return IoTDBConnectorRequestVersion.VERSION_1;
  }

  protected TPipeTransferResp handleTransferHandshakeV1(PipeTransferHandshakeV1Req req) {
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

    String receiverFileBaseDir;
    try {
      receiverFileBaseDir = getReceiverFileBaseDir();
      if (Objects.isNull(receiverFileBaseDir)) {
        LOGGER.error(
            "Failed to init pipe receiver file folder manager because all disks of folders are full.");
        return new TPipeTransferResp(StatusUtils.getStatus(TSStatusCode.DISK_SPACE_INSUFFICIENT));
      }
    } catch (Exception e) {
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

  protected abstract String getReceiverFileBaseDir() throws Exception;

  protected TPipeTransferResp handleTransferHandshakeV2(PipeTransferHandshakeV2Req req)
      throws IOException {
    // Reject to handshake if the receiver can not take clusterId from config node.
    final String clusterIdFromConfigNode = getClusterId();
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

  protected final TPipeTransferResp handleTransferFilePiece(
      PipeTransferFilePieceReq req, boolean isRequestThroughAirGap) throws IOException {
    try {
      updateWritingFileIfNeeded(req.getFileName());

      if (!isWritingFileOffsetCorrect(req.getStartWritingOffset())) {
        if (isRequestThroughAirGap) {
          // If the request is through air gap, the sender will resend the file piece from the
          // beginning of the file.
          // So the receiver should reset the offset of the writing file to the beginning of the
          // file.
          writingFileWriter.setLength(0);
        }

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
      writingFileWriter.getFD().sync();
      return PipeTransferFilePieceResp.toTPipeTransferResp(
          RpcUtils.SUCCESS_STATUS, writingFileWriter.length());
    } catch (Exception e) {
      LOGGER.warn("Failed to write file piece from req {}.", req, e);
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

  protected final void updateWritingFileIfNeeded(String fileName) throws IOException {
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

  protected final TPipeTransferResp handleTransferFileSeal(PipeTransferFileSealReq req) {
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
      writingFileWriter = null;

      // writingFile will be deleted after load if no exception occurs
      writingFile = null;

      final TSStatus status = loadFile(req, fileAbsolutePath);
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

  protected abstract TSStatus loadFile(
      final PipeTransferFileSealReq req, final String fileAbsolutePath) throws IOException;

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
}
