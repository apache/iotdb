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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

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

public class PipeTransferTsFileInsertionEventHandler
    implements AsyncMethodCallback<TPipeTransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTsFileInsertionEventHandler.class);

  private final PipeTsFileInsertionEvent event;
  private final IoTDBDataRegionAsyncConnector connector;

  private final File tsFile;
  private final File modFile;
  private File currentFile;

  private final boolean transferMod;

  private final int readFileBufferSize;
  private final byte[] readBuffer;
  private long position;

  private RandomAccessFile reader;

  private final AtomicBoolean isSealSignalSent;

  private AsyncPipeDataTransferServiceClient client;

  public PipeTransferTsFileInsertionEventHandler(
      final PipeTsFileInsertionEvent event, final IoTDBDataRegionAsyncConnector connector)
      throws FileNotFoundException {
    this.event = event;
    this.connector = connector;

    tsFile = event.getTsFile();
    modFile = event.getModFile();
    transferMod = event.isWithMod() && connector.supportModsIfIsDataNodeReceiver();
    currentFile = transferMod ? modFile : tsFile;

    readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    readBuffer = new byte[readFileBufferSize];
    position = 0;

    reader =
        Objects.nonNull(modFile)
            ? new RandomAccessFile(modFile, "r")
            : new RandomAccessFile(tsFile, "r");

    isSealSignalSent = new AtomicBoolean(false);
  }

  public void transfer(final AsyncPipeDataTransferServiceClient client)
      throws TException, IOException {
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
        client.pipeTransfer(
            transferMod
                ? PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                    modFile.getName(), modFile.length(), tsFile.getName(), tsFile.length())
                : PipeTransferTsFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()),
            this);
      }
      return;
    }

    final byte[] payload =
        readLength == readFileBufferSize
            ? readBuffer
            : Arrays.copyOfRange(readBuffer, 0, readLength);
    client.pipeTransfer(
        transferMod
            ? PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(
                currentFile.getName(), position, payload)
            : PipeTransferTsFilePieceReq.toTPipeTransferReq(
                currentFile.getName(), position, payload),
        this);
    position += readLength;
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
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
        event.decreaseReferenceCount(PipeTransferTsFileInsertionEventHandler.class.getName(), true);

        LOGGER.info(
            "Successfully transferred file {} (committer key={}, commit id={}).",
            tsFile,
            event.getCommitterKey(),
            event.getCommitId());

        if (client != null) {
          client.setShouldReturnSelf(true);
          client.returnSelf();
        }
      }
      return;
    }

    // If the isSealSignalSent is false, then the response must be a PipeTransferFilePieceResp
    try {
      final PipeTransferFilePieceResp resp =
          PipeTransferFilePieceResp.fromTPipeTransferResp(response);

      // This case only happens when the connection is broken, and the connector is reconnected
      // to the receiver, then the receiver will redirect the file position to the last position
      final long code = resp.getStatus().getCode();

      if (code == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
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

      if (client != null) {
        client.setShouldReturnSelf(true);
        client.returnSelf();
      }
    }
  }
}
