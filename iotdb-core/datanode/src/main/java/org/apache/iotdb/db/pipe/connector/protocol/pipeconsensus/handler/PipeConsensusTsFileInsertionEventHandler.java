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
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
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
  private final File modFile;
  private File currentFile;

  private final boolean transferMod;

  private final int readFileBufferSize;
  private final byte[] readBuffer;
  private long position;

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
      throws FileNotFoundException {
    this.event = event;
    this.connector = connector;
    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.thisDataNodeId = thisDataNodeId;

    tsFile = event.getTsFile();
    modFile = event.getModFile();
    transferMod = event.isWithMod();
    currentFile = transferMod ? modFile : tsFile;

    readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    readBuffer = new byte[readFileBufferSize];
    position = 0;

    reader =
        Objects.nonNull(modFile)
            ? new RandomAccessFile(modFile, "r")
            : new RandomAccessFile(tsFile, "r");

    isSealSignalSent = new AtomicBoolean(false);

    this.metric = metric;
    this.createTime = System.nanoTime();
  }

  public void transfer(final AsyncPipeConsensusServiceClient client)
      throws TException, IOException {
    startTransferPieceTime = System.nanoTime();

    this.client = client;
    client.setShouldReturnSelf(false);

    final int readLength = reader.read(readBuffer);
    if (readLength == -1) {
      if (currentFile == modFile) {
        currentFile = tsFile;
        position = 0;
        try {
          reader.close();
        } catch (final IOException e) {
          LOGGER.warn("Failed to close file reader when successfully transferred mod file.", e);
        }
        reader = new RandomAccessFile(tsFile, "r");
        transfer(client);
      } else if (currentFile == tsFile) {
        isSealSignalSent.set(true);
        client.pipeConsensusTransfer(
            transferMod
                ? PipeConsensusTsFileSealWithModReq.toTPipeConsensusTransferReq(
                    modFile.getName(),
                    modFile.length(),
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
      return;
    }

    // for save some mem
    final byte[] payload =
        readLength == readFileBufferSize
            ? readBuffer
            : Arrays.copyOfRange(readBuffer, 0, readLength);
    client.pipeConsensusTransfer(
        transferMod
            ? PipeConsensusTsFilePieceWithModReq.toTPipeConsensusTransferReq(
                currentFile.getName(),
                position,
                payload,
                commitId,
                consensusGroupId,
                thisDataNodeId)
            : PipeConsensusTsFilePieceReq.toTPipeConsensusTransferReq(
                currentFile.getName(),
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
            "Successfully transferred file {} (committer key={}, commit id={}).",
            tsFile,
            event.getCommitterKey(),
            event.getCommitId());

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
        position = resp.getEndWritingOffset();
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
        "Failed to transfer TsFileInsertionEvent {} (committer key {}, commit id {}).",
        tsFile,
        event.getCommitterKey(),
        event.getCommitId(),
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
