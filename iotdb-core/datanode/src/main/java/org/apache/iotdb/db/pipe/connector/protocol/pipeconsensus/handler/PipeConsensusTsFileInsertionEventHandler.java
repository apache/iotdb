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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.handler;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.response.PipeConsensusTransferFilePieceResp;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusConnectorMetrics;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeConsensusTsFileInsertionEventHandler
    implements AsyncMethodCallback<TPipeConsensusTransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusTsFileInsertionEventHandler.class);

  private final PipeTsFileInsertionEvent event;
  private final PipeConsensusAsyncConnector connector;
  private final TCommitId commitId;
  private final TConsensusGroupId consensusGroupId;
  private final int thisDataNodeId;
  private final File tsFile;
  private final File exclusiveModFile;
  private final File sharedModFile;
  private File currentFile;
  private File targetFile;

  private final boolean transferExclusiveMod;
  private final boolean transferSharedMod;

  private final int readFileBufferSize;
  private final byte[] readBuffer;
  private long position;
  private long targetOffset = 0;
  private final long sharedModFileOffset;

  private RandomAccessFile reader;

  private final AtomicBoolean isSealSignalSent;

  private AsyncPipeConsensusServiceClient client;

  private final PipeConsensusConnectorMetrics metric;

  private final long createTime;

  private long startTransferPieceTime;

  public PipeConsensusTsFileInsertionEventHandler(
      final PipeTsFileInsertionEvent event,
      final PipeConsensusAsyncConnector connector,
      final TCommitId commitId,
      final TConsensusGroupId consensusGroupId,
      final int thisDataNodeId,
      final PipeConsensusConnectorMetrics metric)
      throws IOException {
    this.event = event;
    this.connector = connector;
    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.thisDataNodeId = thisDataNodeId;

    tsFile = event.getTsFile();
    exclusiveModFile = event.getExclusiveModFile();
    transferExclusiveMod = event.isWithExclusiveMod();
    sharedModFile = event.getSharedModFile();
    transferSharedMod = event.isWithSharedMod();
    sharedModFileOffset = event.getSharedModFileOffset();

    if (transferExclusiveMod) {
      currentFile = exclusiveModFile;
      targetFile = ModificationFile.getExclusiveMods(tsFile);
    } else {
      if (transferSharedMod) {
        currentFile = sharedModFile;
        targetFile = ModificationFile.getExclusiveMods(tsFile);
      } else {
        currentFile = tsFile;
        targetFile = tsFile;
      }
    }

    readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    readBuffer = new byte[readFileBufferSize];
    position = 0;

    if (Objects.nonNull(exclusiveModFile)) {
      reader = new RandomAccessFile(exclusiveModFile, "r");
    } else {
      if (Objects.nonNull(sharedModFile)) {
        reader = new RandomAccessFile(sharedModFile, "r");
        reader.seek(sharedModFileOffset);
      } else {
        reader = new RandomAccessFile(tsFile, "r");
      }
    }

    isSealSignalSent = new AtomicBoolean(false);

    this.metric = metric;
    this.createTime = System.nanoTime();
  }

  private void switchToSharedModFile() throws IOException {
    // append the shared mod file to the target's exclusive mod file
    // target file is still the exclusive mod file
    currentFile = sharedModFile;
    targetOffset = position;
    position = 0;
    try {
      reader.close();
    } catch (final IOException e) {
      LOGGER.warn(
          "Failed to close file reader when successfully transferred exclusive mod file.", e);
    }
    reader = new RandomAccessFile(sharedModFile, "r");
    reader.seek(sharedModFileOffset);
  }

  private void switchToTsFile() throws IOException {
    currentFile = tsFile;
    targetFile = tsFile;
    targetOffset = 0;
    position = 0;
    try {
      reader.close();
    } catch (final IOException e) {
      LOGGER.warn("Failed to close file reader when successfully transferred mod file.", e);
    }
    reader = new RandomAccessFile(tsFile, "r");
  }

  private void switchToNextFile() throws TException, IOException {
    if (currentFile == exclusiveModFile) {
      if (transferSharedMod) {
        switchToSharedModFile();
      } else {
        switchToTsFile();
      }
      transfer(client);
    } else if (currentFile == sharedModFile) {
      switchToTsFile();
      transfer(client);
    } else if (currentFile == tsFile) {
      isSealSignalSent.set(true);
      long modFileTotalSize = transferExclusiveMod ? exclusiveModFile.length() : 0;
      modFileTotalSize += transferSharedMod ? sharedModFile.length() - sharedModFileOffset : 0;
      client.pipeConsensusTransfer(
          transferExclusiveMod || transferSharedMod
              ? PipeConsensusTsFileSealWithModReq.toTPipeConsensusTransferReq(
                  ModificationFile.getExclusiveMods(tsFile).getName(),
                  modFileTotalSize,
                  tsFile.getName(),
                  tsFile.length(),
                  event.getFlushPointCount(),
                  commitId,
                  consensusGroupId,
                  event.getProgressIndex(),
                  thisDataNodeId)
              : PipeConsensusTsFileSealReq.toTPipeConsensusTransferReq(
                  tsFile.getName(),
                  tsFile.length(),
                  event.getFlushPointCount(),
                  commitId,
                  consensusGroupId,
                  event.getProgressIndex(),
                  thisDataNodeId),
          this);
    }
  }

  public void transfer(final AsyncPipeConsensusServiceClient client)
      throws TException, IOException {
    startTransferPieceTime = System.nanoTime();

    this.client = client;
    client.setShouldReturnSelf(false);

    final int readLength = reader.read(readBuffer);
    if (readLength == -1) {
      switchToNextFile();
      return;
    }

    // for save some mem
    final byte[] payload =
        readLength == readFileBufferSize
            ? readBuffer
            : Arrays.copyOfRange(readBuffer, 0, readLength);
    client.pipeConsensusTransfer(
        transferExclusiveMod
            ? PipeConsensusTsFilePieceWithModReq.toTPipeConsensusTransferReq(
                targetFile.getName(),
                position + targetOffset,
                payload,
                commitId,
                consensusGroupId,
                thisDataNodeId)
            : PipeConsensusTsFilePieceReq.toTPipeConsensusTransferReq(
                targetFile.getName(),
                position,
                payload,
                commitId,
                consensusGroupId,
                thisDataNodeId),
        this);
    position += readLength;
  }

  @Override
  public void onComplete(final TPipeConsensusTransferResp response) {
    if (isSealSignalSent.get()) {
      try {
        final TSStatus status = response.getStatus();
        // Only handle the failed statuses to avoid string format performance overhead
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          connector
              .statusHandler()
              .handle(
                  status,
                  String.format(
                      "Seal file %s error, result status %s.", tsFile, response.getStatus()),
                  tsFile.getName());
        }

        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          connector.removeEventFromBuffer(event);
        }
      } catch (final Exception e) {
        onError(e);
        return;
      }

      try {
        if (reader != null) {
          reader.close();
        }
      } catch (final IOException e) {
        LOGGER.warn("Failed to close file reader when successfully transferred file.", e);
      } finally {
        event.decreaseReferenceCount(
            PipeConsensusTsFileInsertionEventHandler.class.getName(), true);

        LOGGER.info(
            "Successfully transferred file {} (committer key={}, replicate index={}).",
            tsFile,
            event.getCommitterKey(),
            event.getReplicateIndexForIoTV2());

        if (client != null) {
          client.setShouldReturnSelf(true);
          client.returnSelf();
        }

        long duration = System.nanoTime() - createTime;
        metric.recordConnectorTsFileTransferTimer(duration);
      }
      return;
    }

    // If the isSealSignalSent is false, then the response must be a
    // PipeConsensusTransferFilePieceResp
    try {
      final PipeConsensusTransferFilePieceResp resp =
          PipeConsensusTransferFilePieceResp.fromTPipeConsensusTransferResp(response);

      // This case only happens when the connection is broken, and the connector is reconnected
      // to the receiver, then the receiver will redirect the file position to the last position
      final long code = resp.getStatus().getCode();

      if (code == TSStatusCode.PIPE_CONSENSUS_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
        if (currentFile == sharedModFile) {
          // the exclusive mod file has been written to remote
          // the local position should subtract the length of exclusive mod file
          position = resp.getEndWritingOffset() - targetOffset;
        } else {
          position = resp.getEndWritingOffset();
        }
        reader.seek(position);
        LOGGER.info("Redirect file position to {}.", position);
      } else {
        final TSStatus status = response.getStatus();
        // Only handle the failed statuses to avoid string format performance overhead
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          connector
              .statusHandler()
              .handle(status, response.getStatus().getMessage(), tsFile.getName());
        }
      }
      long duration = System.nanoTime() - startTransferPieceTime;
      metric.recordConnectorTsFilePieceTransferTimer(duration);

      transfer(client);
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    LOGGER.warn(
        "Failed to transfer TsFileInsertionEvent {} (committer key {}, replicate index {}).",
        tsFile,
        event.getCommitterKey(),
        event.getReplicateIndexForIoTV2(),
        exception);

    try {
      if (reader != null) {
        reader.close();
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to close file reader when failed to transfer file.", e);
    } finally {
      connector.addFailureEventToRetryQueue(event);
      metric.recordRetryCounter();

      if (client != null) {
        client.setShouldReturnSelf(true);
        client.returnSelf();
      }
    }
  }
}
