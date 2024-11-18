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

package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTsFileMemoryBlock;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code SubscriptionEventTsFileResponse} class extends {@link
 * SubscriptionEventExtendableResponse} to manage subscription responses related to time series
 * files. The actual payload can include {@link FileInitPayload}, {@link FilePiecePayload}, and
 * {@link FileSealPayload}, allowing for detailed control over file data streaming.
 */
public class SubscriptionEventTsFileResponse extends SubscriptionEventExtendableResponse {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTsFileResponse.class);

  private final File tsFile;
  private final SubscriptionCommitContext commitContext;

  public SubscriptionEventTsFileResponse(
      final File tsFile, final SubscriptionCommitContext commitContext) {
    super();

    this.tsFile = tsFile;
    this.commitContext = commitContext;

    init();
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse(final long offset) throws Exception {
    generateNextTsFileResponse(offset).ifPresent(super::offer);
    if (Objects.isNull(poll())) {
      LOGGER.warn(
          "SubscriptionEventTsFileResponse {} is empty when fetching next response (broken invariant)",
          this);
    }
  }

  @Override
  public synchronized void nack() {
    cleanUp();
    init();
  }

  @Override
  public synchronized void cleanUp() {
    super.cleanUp();
  }

  /////////////////////////////// utility ///////////////////////////////

  private void init() {
    if (!isEmpty()) {
      LOGGER.warn(
          "SubscriptionEventTsFileResponse {} is not empty when initializing (broken invariant)",
          this);
      return;
    }

    offer(
        new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            new FileInitPayload(tsFile.getName()),
            commitContext));
  }

  private synchronized Optional<CachedSubscriptionPollResponse> generateNextTsFileResponse(
      final long offset) throws SubscriptionException, IOException, InterruptedException {
    return Optional.of(generateResponseWithPieceOrSealPayload(offset));
  }

  private synchronized Optional<CachedSubscriptionPollResponse> generateNextTsFileResponse()
      throws SubscriptionException, IOException, InterruptedException {
    final SubscriptionPollResponse previousResponse = peekLast();
    if (Objects.isNull(previousResponse)) {
      LOGGER.warn(
          "SubscriptionEventTsFileResponse {} is empty when generating next response (broken invariant)",
          this);
      return Optional.empty();
    }
    final short responseType = previousResponse.getResponseType();
    final SubscriptionPollPayload payload = previousResponse.getPayload();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return Optional.empty();
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        return Optional.of(generateResponseWithPieceOrSealPayload(0));
      case FILE_PIECE:
        return Optional.of(
            generateResponseWithPieceOrSealPayload(
                ((FilePiecePayload) payload).getNextWritingOffset()));
      case FILE_SEAL:
        // not need to prefetch
        break;
      default:
        LOGGER.warn("unexpected message type: {}", responseType);
    }

    return Optional.empty();
  }

  private @NonNull CachedSubscriptionPollResponse generateResponseWithPieceOrSealPayload(
      final long writingOffset) throws SubscriptionException, IOException, InterruptedException {
    final long tsFileLength = tsFile.length();
    if (writingOffset >= tsFileLength) {
      // generate subscription poll response with seal payload
      hasNoMore = true;
      return new CachedSubscriptionPollResponse(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          new FileSealPayload(tsFile.getName(), tsFile.length()),
          commitContext);
    }

    final long readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final long bufferSize;
    if (writingOffset + readFileBufferSize >= tsFileLength) {
      // last piece
      bufferSize = tsFileLength - writingOffset;
    } else {
      // not last piece
      bufferSize = readFileBufferSize;
    }

    waitForResourceEnough4Slicing(SubscriptionAgent.receiver().remainingMs());
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      reader.seek(writingOffset);

      final PipeTsFileMemoryBlock memoryBlock =
          PipeDataNodeResourceManager.memory().forceAllocateForTsFileWithRetry(bufferSize);
      final byte[] readBuffer = new byte[(int) bufferSize];

      final int readLength = reader.read(readBuffer);
      if (readLength != bufferSize) {
        memoryBlock.close();
        throw new SubscriptionException(
            String.format(
                "inconsistent read length (broken invariant), expected: %s, actual: %s",
                bufferSize, readLength));
      }

      // generate subscription poll response with piece payload
      final CachedSubscriptionPollResponse response =
          new CachedSubscriptionPollResponse(
              SubscriptionPollResponseType.FILE_PIECE.getType(),
              new FilePiecePayload(tsFile.getName(), writingOffset + readLength, readBuffer),
              commitContext);
      response.setMemoryBlock(memoryBlock);
      return response;
    }
  }

  private void waitForResourceEnough4Slicing(final long timeoutMs) throws InterruptedException {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (memoryManager.isEnough4TsFileSlicing()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    final long memoryCheckIntervalMs =
        PipeConfig.getInstance().getPipeTsFileParserCheckMemoryEnoughIntervalMs();
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
        // see org.apache.iotdb.rpc.subscription.exception.SubscriptionTimeoutException.KEYWORD
        throw new InterruptedException(
            String.format("TimeoutException: Waited %s seconds", waitTimeSeconds));
      }
    }

    final long currentTime = System.currentTimeMillis();
    final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
    LOGGER.info(
        "Wait for resource enough for slicing tsfile {} for {} seconds.", tsFile, waitTimeSeconds);
  }
}
