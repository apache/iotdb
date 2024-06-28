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

package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public class SubscriptionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEvent.class);

  private static final long INVALID_TIMESTAMP = -1;

  private final SubscriptionPipeEvents pipeEvents;
  private final SubscriptionPollResponse[] responses;
  private int currentResponseIndex;

  private final ByteBuffer[] byteBuffers; // serialized responses
  private final SubscriptionCommitContext
      commitContext; // all responses have the same commit context

  private String lastPolledConsumerId;
  private long lastPolledTimestamp;
  private long committedTimestamp;

  public SubscriptionEvent(
      final SubscriptionPipeEvents pipeEvents, final SubscriptionPollResponse initialResponse) {
    this.pipeEvents = pipeEvents;
    final int responseLength = getResponseLength(initialResponse.getResponseType());
    this.responses = new SubscriptionPollResponse[responseLength];
    this.byteBuffers = new ByteBuffer[responseLength];
    this.responses[0] = initialResponse;
    this.currentResponseIndex = 0;
    this.commitContext = initialResponse.getCommitContext();

    this.lastPolledConsumerId = null;
    this.lastPolledTimestamp = INVALID_TIMESTAMP;
    this.committedTimestamp = INVALID_TIMESTAMP;
  }

  private int getResponseLength(final short responseType) {
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return 1;
    }
    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        final long tsFileLength = pipeEvents.getTsFile().length();
        final long readFileBufferSize =
            SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
        final int length = (int) (tsFileLength / readFileBufferSize);
        // add for init, last piece and seal
        return (tsFileLength % readFileBufferSize != 0) ? length + 3 : length + 2;
      case TABLETS:
      case ERROR:
      case TERMINATION:
        return 1;
      default:
        LOGGER.warn("unexpected response type: {}", responseType);
        return 1;
    }
  }

  public SubscriptionPollResponse getCurrentResponse() {
    return getResponse(currentResponseIndex);
  }

  public SubscriptionPollResponse getResponse(final int index) {
    return responses[index];
  }

  //////////////////////////// commit ////////////////////////////

  public void recordCommittedTimestamp() {
    committedTimestamp = System.currentTimeMillis();
  }

  public boolean isCommitted() {
    if (commitContext.getCommitId() == INVALID_COMMIT_ID) {
      // event with invalid commit id is committed
      return true;
    }
    return committedTimestamp != INVALID_TIMESTAMP;
  }

  public boolean isCommittable() {
    if (commitContext.getCommitId() == INVALID_COMMIT_ID) {
      // event with invalid commit id is uncommittable
      return false;
    }
    return currentResponseIndex >= responses.length - 1;
  }

  public void ack() {
    pipeEvents.ack();
  }

  public void cleanup() {
    // reset serialized responses
    resetByteBuffer(true);

    // clean up pipe events
    pipeEvents.cleanup();
  }

  //////////////////////////// pollable ////////////////////////////

  public void recordLastPolledTimestamp() {
    lastPolledTimestamp = Math.max(lastPolledTimestamp, System.currentTimeMillis());
  }

  public boolean pollable() {
    if (isCommitted()) {
      cleanup();
      return false;
    }
    if (lastPolledTimestamp == INVALID_TIMESTAMP) {
      return true;
    }
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return System.currentTimeMillis() - lastPolledTimestamp
        > SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs();
  }

  public void nack() {
    lastPolledConsumerId = null;
    lastPolledTimestamp = INVALID_TIMESTAMP;
  }

  public void recordLastPolledConsumerId(final String consumerId) {
    lastPolledConsumerId = consumerId;
  }

  public String getLastPolledConsumerId() {
    return lastPolledConsumerId;
  }

  //////////////////////////// byte buffer ////////////////////////////

  public void resetByteBuffer(final boolean resetAll) {
    if (resetAll) {
      Arrays.stream(responses)
          .forEach((response -> SubscriptionEventBinaryCache.getInstance().invalidate(response)));
      // maybe friendly for gc
      Arrays.fill(byteBuffers, null);
    } else {
      SubscriptionEventBinaryCache.getInstance().invalidate(responses[currentResponseIndex]);
      // maybe friendly for gc
      byteBuffers[currentResponseIndex] = null;
    }
  }

  /////////////////////////////// tsfile ///////////////////////////////

  public @NonNull SubscriptionPollResponse generateSubscriptionPollResponseWithPieceOrSealPayload(
      final long writingOffset) throws IOException {
    final File tsFile = pipeEvents.getTsFile();

    final long readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final byte[] readBuffer = new byte[(int) readFileBufferSize];
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        reader.seek(writingOffset);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] filePiece =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);

        // generate subscription poll response with piece payload
        return new SubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_PIECE.getType(),
            new FilePiecePayload(tsFile.getName(), writingOffset + readLength, filePiece),
            commitContext);
      }

      // generate subscription poll response with seal payload
      return new SubscriptionPollResponse(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          new FileSealPayload(tsFile.getName(), tsFile.length()),
          commitContext);
    }
  }

  private void prefetch(final int currentIndex) throws IOException {
    if (currentIndex >= responses.length || currentIndex <= 0) {
      return;
    }

    if (Objects.nonNull(responses[currentIndex])) {
      return;
    }

    final SubscriptionPollResponse response = this.getResponse(currentIndex - 1);
    final short responseType = response.getResponseType();
    final SubscriptionPollPayload payload = response.getPayload();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return;
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        responses[currentIndex] = generateSubscriptionPollResponseWithPieceOrSealPayload(0);
        break;
      case FILE_PIECE:
        responses[currentIndex] =
            generateSubscriptionPollResponseWithPieceOrSealPayload(
                ((FilePiecePayload) payload).getNextWritingOffset());
        break;
      case FILE_SEAL:
        // not need to prefetch
        return;
      default:
        LOGGER.warn("unexpected message type: {}", responseType);
    }
  }

  public void prefetchNext() throws IOException {
    for (int currentIndex = currentResponseIndex;
        currentIndex < responses.length - 1;
        currentIndex++) {
      if (Objects.isNull(responses[currentIndex + 1])) {
        prefetch(currentIndex + 1);
        return;
      }
    }
  }

  public void serializeNext() {
    for (int currentIndex = currentResponseIndex;
        currentIndex < responses.length - 1;
        currentIndex++) {
      if (Objects.nonNull(responses[currentIndex + 1])) {
        serialize(currentIndex + 1);
        return;
      }
    }
  }

  private void serialize(final int currentIndex) {
    if (currentIndex >= responses.length) {
      return;
    }

    if (Objects.isNull(responses[currentIndex])) {
      return;
    }

    SubscriptionEventBinaryCache.getInstance().trySerialize(responses[currentIndex]);
  }

  public void fetchNext() throws IOException {
    prefetchNext();
    currentResponseIndex++;
  }

  public void resetCurrentResponseIndex() {
    currentResponseIndex = 0;
  }

  public File getTsFile() {
    return pipeEvents.getTsFile();
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEvent{pipeEvents="
        + pipeEvents.toString()
        + ", responses="
        + Arrays.toString(responses)
        + ", responses' byte buffer size="
        + Arrays.stream(byteBuffers)
            .map(byteBuffer -> Objects.isNull(byteBuffer) ? "null" : byteBuffer.limit())
            .collect(Collectors.toList())
        + ", currentResponseIndex="
        + currentResponseIndex
        + ", lastPolledConsumerId="
        + lastPolledConsumerId
        + ", lastPolledTimestamp="
        + lastPolledTimestamp
        + ", committedTimestamp="
        + committedTimestamp
        + "}";
  }
}
