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

import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTransferTsFileInsertionEventHandler
    implements AsyncMethodCallback<TPipeTransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTsFileInsertionEventHandler.class);

  private final long requestCommitId;
  private final PipeTsFileInsertionEvent event;
  private final IoTDBThriftAsyncConnector connector;

  private final File tsFile;
  private final int readFileBufferSize;
  private final byte[] readBuffer;
  private long position;

  private final RandomAccessFile reader;

  private final AtomicBoolean isSealSignalSent;

  private AsyncPipeDataTransferServiceClient client;

  public PipeTransferTsFileInsertionEventHandler(
      long requestCommitId, PipeTsFileInsertionEvent event, IoTDBThriftAsyncConnector connector)
      throws FileNotFoundException {
    this.requestCommitId = requestCommitId;
    this.event = event;
    this.connector = connector;

    tsFile = event.getTsFile();
    readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    readBuffer = new byte[readFileBufferSize];
    position = 0;

    reader = new RandomAccessFile(tsFile, "r");

    isSealSignalSent = new AtomicBoolean(false);

    event.increaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName());
  }

  public void transfer(AsyncPipeDataTransferServiceClient client) throws TException, IOException {
    this.client = client;
    client.setShouldReturnSelf(false);

    final int readLength = reader.read(readBuffer);

    if (readLength == -1) {
      isSealSignalSent.set(true);
      client.pipeTransfer(
          PipeTransferFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()), this);
      return;
    }

    client.pipeTransfer(
        PipeTransferFilePieceReq.toTPipeTransferReq(
            tsFile.getName(),
            position,
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength)),
        this);
    position += readLength;
  }

  @Override
  public void onComplete(TPipeTransferResp response) {
    if (isSealSignalSent.get()) {
      if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        onError(
            new PipeException(
                String.format(
                    "Seal file %s error, result status %s.", tsFile, response.getStatus())));
        return;
      }

      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to close file reader when successfully transferred file.", e);
      } finally {
        connector.commit(requestCommitId, event);

        LOGGER.info(
            "Successfully transferred file {}. Request commit id is {}.", tsFile, requestCommitId);

        if (client != null) {
          client.setShouldReturnSelf(true);
          client.returnSelf();
        }
      }
      return;
    }

    // if the isSealSignalSent is false, then the response must be a PipeTransferFilePieceResp
    try {
      final PipeTransferFilePieceResp resp =
          PipeTransferFilePieceResp.fromTPipeTransferResp(response);

      // this case only happens when the connection is broken, and the connector is reconnected
      // to the receiver, then the receiver will redirect the file position to the last position
      final long code = resp.getStatus().getCode();

      if (code == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
        position = resp.getEndWritingOffset();
        reader.seek(position);
        LOGGER.info("Redirect file position to {}.", position);
      } else if (code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(
            String.format("Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
      }

      transfer(client);
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    LOGGER.warn(
        "Failed to transfer tsfile {} (request commit id {}).", tsFile, requestCommitId, exception);

    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close file reader when failed to transfer file.", e);
    } finally {
      connector.addFailureEventToRetryQueue(requestCommitId, event);

      if (client != null) {
        client.setShouldReturnSelf(true);
        client.returnSelf();
      }
    }
  }
}
