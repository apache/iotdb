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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.limiter.TsFileSendRateLimiter;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTsFileMemoryBlock;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PipeTransferTsFileHandler extends PipeTransferTrackableHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTsFileHandler.class);

  // Used to rate limit the transfer
  private final Map<Pair<String, Long>, Double> pipeName2WeightMap;

  // The original events to be transferred, can be multiple events if
  // the file is batched with other events
  private final List<EnrichedEvent> events;
  private final AtomicInteger eventsReferenceCount;
  private final AtomicBoolean eventsHadBeenAddedToRetryQueue;

  private final File tsFile;
  private final File modFile;
  private File currentFile;

  private final boolean transferMod;

  private final String dataBaseName;

  private final int readFileBufferSize;
  private PipeTsFileMemoryBlock memoryBlock;
  private byte[] readBuffer;
  private long position;

  private RandomAccessFile reader;

  private final AtomicBoolean isSealSignalSent;

  private IoTDBDataNodeAsyncClientManager clientManager;

  public PipeTransferTsFileHandler(
      final IoTDBDataRegionAsyncSink connector,
      final Map<Pair<String, Long>, Double> pipeName2WeightMap,
      final List<EnrichedEvent> events,
      final AtomicInteger eventsReferenceCount,
      final AtomicBoolean eventsHadBeenAddedToRetryQueue,
      final File tsFile,
      final File modFile,
      final boolean transferMod,
      final String dataBaseName)
      throws InterruptedException {
    super(connector);

    this.pipeName2WeightMap = pipeName2WeightMap;

    this.events = events;
    this.eventsReferenceCount = eventsReferenceCount;
    this.eventsHadBeenAddedToRetryQueue = eventsHadBeenAddedToRetryQueue;

    this.tsFile = tsFile;
    this.modFile = modFile;
    this.transferMod = transferMod;
    this.dataBaseName = dataBaseName;
    currentFile = transferMod ? modFile : tsFile;

    // NOTE: Waiting for resource enough for slicing here may cause deadlock!
    // TsFile events are producing and consuming at the same time, and the memory of a TsFile
    // event is not released until the TsFile is sealed. So if the memory is not enough for slicing,
    // the TsFile event will be blocked and waiting for the memory to be released. At the same time,
    // the memory of the TsFile event is not released, so the memory is not enough for slicing. This
    // will cause a deadlock.
    waitForResourceEnough4Slicing((long) ((1 + Math.random()) * 20 * 1000)); // 20 - 40 seconds
    readFileBufferSize =
        (int)
            Math.min(
                PipeConfig.getInstance().getPipeConnectorReadFileBufferSize(),
                transferMod ? Math.max(tsFile.length(), modFile.length()) : tsFile.length());
    position = 0;

    isSealSignalSent = new AtomicBoolean(false);
  }

  public File getTsFile() {
    return tsFile;
  }

  public void transfer(
      final IoTDBDataNodeAsyncClientManager clientManager,
      final AsyncPipeDataTransferServiceClient client)
      throws TException, IOException {
    // Delay creation of resources to avoid OOM or too many open files
    if (readBuffer == null) {
      memoryBlock =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTsFileWithRetry(
                  PipeConfig.getInstance().isPipeConnectorReadFileBufferMemoryControlEnabled()
                      ? readFileBufferSize
                      : 0);
      readBuffer = new byte[readFileBufferSize];
    }

    if (reader == null) {
      reader =
          Objects.nonNull(modFile)
              ? new RandomAccessFile(modFile, "r")
              : new RandomAccessFile(tsFile, "r");
    }

    this.clientManager = clientManager;
    this.client = client;

    if (client == null) {
      LOGGER.warn(
          "Client has been returned to the pool. Current handler status is {}. Will not transfer {}.",
          connector.isClosed() ? "CLOSED" : "NOT CLOSED",
          tsFile);
      return;
    }

    client.setShouldReturnSelf(false);
    client.setTimeoutDynamically(clientManager.getConnectionTimeout());

    PipeResourceMetrics.getInstance().recordDiskIO(readFileBufferSize);
    if (connector.isEnableSendTsFileLimit()) {
      TsFileSendRateLimiter.getInstance().acquire(readFileBufferSize);
    }
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
        transfer(clientManager, client);
      } else if (currentFile == tsFile) {
        isSealSignalSent.set(true);

        final TPipeTransferReq uncompressedReq =
            transferMod
                ? PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                    modFile.getName(),
                    modFile.length(),
                    tsFile.getName(),
                    tsFile.length(),
                    dataBaseName)
                : PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                    tsFile.getName(), tsFile.length(), dataBaseName);
        final TPipeTransferReq req = connector.compressIfNeeded(uncompressedReq);

        pipeName2WeightMap.forEach(
            (pipePair, weight) ->
                connector.rateLimitIfNeeded(
                    pipePair.getLeft(),
                    pipePair.getRight(),
                    client.getEndPoint(),
                    (long) (req.getBody().length * weight)));

        if (!tryTransfer(client, req)) {
          return;
        }
      }
      return;
    }

    final byte[] payload =
        readLength == readFileBufferSize
            ? readBuffer
            : Arrays.copyOfRange(readBuffer, 0, readLength);
    final TPipeTransferReq uncompressedReq =
        transferMod
            ? PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(
                currentFile.getName(), position, payload)
            : PipeTransferTsFilePieceReq.toTPipeTransferReq(
                currentFile.getName(), position, payload);
    final TPipeTransferReq req = connector.compressIfNeeded(uncompressedReq);

    pipeName2WeightMap.forEach(
        (pipePair, weight) ->
            connector.rateLimitIfNeeded(
                pipePair.getLeft(),
                pipePair.getRight(),
                client.getEndPoint(),
                (long) (req.getBody().length * weight)));

    if (!tryTransfer(client, req)) {
      return;
    }

    position += readLength;
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
    try {
      super.onComplete(response);
    } finally {
      if (connector.isClosed()) {
        returnClientIfNecessary();
      }
    }
  }

  @Override
  protected boolean onCompleteInternal(final TPipeTransferResp response) {
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
        return false;
      }

      try {
        if (reader != null) {
          reader.close();
        }

        // Delete current file when using tsFile as batch
        if (events.stream().anyMatch(event -> !(event instanceof PipeTsFileInsertionEvent))) {
          RetryUtils.retryOnException(
              () -> {
                FileUtils.delete(currentFile);
                return null;
              });
        }
      } catch (final IOException e) {
        LOGGER.warn(
            "Failed to close file reader or delete tsFile when successfully transferred file.", e);
      } finally {
        final int referenceCount = eventsReferenceCount.decrementAndGet();
        if (referenceCount <= 0) {
          events.forEach(
              event ->
                  event.decreaseReferenceCount(PipeTransferTsFileHandler.class.getName(), true));
        }

        if (events.size() <= 1 || LOGGER.isDebugEnabled()) {
          LOGGER.info(
              "Successfully transferred file {} (committer key={}, commit id={}, reference count={}).",
              tsFile,
              events.stream().map(EnrichedEvent::getCommitterKey).collect(Collectors.toList()),
              events.stream().map(EnrichedEvent::getCommitIds).collect(Collectors.toList()),
              referenceCount);
        } else {
          LOGGER.info(
              "Successfully transferred file {} (batched TableInsertionEvents, reference count={}).",
              tsFile,
              referenceCount);
        }

        returnClientIfNecessary();
      }

      return true;
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

      transfer(clientManager, client);
    } catch (final Exception e) {
      onError(e);
      return false;
    }

    return false; // due to seal transfer not yet completed
  }

  @Override
  public void onError(final Exception exception) {
    try {
      super.onError(exception);
    } finally {
      returnClientIfNecessary();
    }
  }

  @Override
  protected void onErrorInternal(final Exception exception) {
    try {
      if (events.size() <= 1 || LOGGER.isDebugEnabled()) {
        PipeLogger.log(
            LOGGER::warn,
            exception,
            "Failed to transfer TsFileInsertionEvent %s (committer key %s, commit id %s).",
            tsFile,
            events.stream().map(EnrichedEvent::getCommitterKey).collect(Collectors.toList()),
            events.stream().map(EnrichedEvent::getCommitIds).collect(Collectors.toList()));
      } else {
        PipeLogger.log(
            LOGGER::warn,
            exception,
            "Failed to transfer TsFileInsertionEvent %s (batched TableInsertionEvents).",
            tsFile);
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to log error when failed to transfer file.", e);
    }

    try {
      if (Objects.nonNull(clientManager)) {
        clientManager.adjustTimeoutIfNecessary(exception);
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to adjust timeout when failed to transfer file.", e);
    }

    try {
      if (reader != null) {
        reader.close();
      }

      // Delete current file when using tsFile as batch
      if (events.stream().anyMatch(event -> !(event instanceof PipeTsFileInsertionEvent))) {
        RetryUtils.retryOnException(
            () -> {
              FileUtils.delete(currentFile);
              return null;
            });
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to close file reader or delete tsFile when failed to transfer file.", e);
    } finally {
      try {
        returnClientIfNecessary();
      } finally {
        if (eventsHadBeenAddedToRetryQueue.compareAndSet(false, true)) {
          connector.addFailureEventsToRetryQueue(events);
        }
      }
    }
  }

  private void returnClientIfNecessary() {
    if (client == null) {
      return;
    }

    if (connector.isClosed()) {
      closeClient();
    }

    client.setShouldReturnSelf(true);
    client.returnSelf(
        (e) -> {
          if (e instanceof IllegalStateException) {
            LOGGER.info(
                "Illegal state when return the client to object pool, maybe the pool is already cleared. Will ignore.");
            return true;
          }
          return false;
        });
    client = null;
  }

  @Override
  protected void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    if (client == null) {
      LOGGER.warn(
          "Client has been returned to the pool. Current handler status is {}. Will not transfer {}.",
          connector.isClosed() ? "CLOSED" : "NOT CLOSED",
          tsFile);
      return;
    }

    client.pipeTransfer(req, this);
  }

  @Override
  public void clearEventsReferenceCount() {
    events.forEach(event -> event.clearReferenceCount(PipeTransferTsFileHandler.class.getName()));
  }

  @Override
  public void close() {
    super.close();

    if (memoryBlock != null) {
      memoryBlock.close();
      memoryBlock = null;
    }
  }

  /**
   * @param timeoutMs CAN NOT BE UNLIMITED, otherwise it may cause deadlock.
   */
  private void waitForResourceEnough4Slicing(final long timeoutMs) throws InterruptedException {
    if (!PipeConfig.getInstance().isPipeConnectorReadFileBufferMemoryControlEnabled()) {
      return;
    }

    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (memoryManager.isEnough4TsFileSlicing()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    final long memoryCheckIntervalMs =
        PipeConfig.getInstance().getPipeCheckMemoryEnoughIntervalMs();
    while (!memoryManager.isEnough4TsFileSlicing()) {
      Thread.sleep(memoryCheckIntervalMs);

      final long currentTime = System.currentTimeMillis();
      final double elapsedRecordTimeSeconds = (currentTime - lastRecordTime) / 1000.0;
      final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
      if (elapsedRecordTimeSeconds > 10.0) {
        LOGGER.info(
            "Wait for resource enough for slicing tsfile {} for {} seconds.",
            tsFile,
            waitTimeSeconds);
        lastRecordTime = currentTime;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Wait for resource enough for slicing tsfile {} for {} seconds.",
            tsFile,
            waitTimeSeconds);
      }

      if (waitTimeSeconds * 1000 > timeoutMs) {
        // should contain 'TimeoutException' in exception message
        throw new PipeException(
            String.format("TimeoutException: Waited %s seconds", waitTimeSeconds));
      }
    }

    final long currentTime = System.currentTimeMillis();
    final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
    LOGGER.info(
        "Wait for resource enough for slicing tsfile {} for {} seconds.", tsFile, waitTimeSeconds);
  }
}
