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
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public class SubscriptionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEvent.class);

  private static final long INVALID_TIMESTAMP = -1;

  private final SubscriptionPipeEvents pipeEvents;

  private final SubscriptionPollResponse[] responses;
  private int currentResponseIndex = 0;

  private final ByteBuffer[] byteBuffers; // serialized responses
  private final SubscriptionCommitContext
      commitContext; // all responses have the same commit context

  // lastPolledConsumerId is not used as a criterion for determining pollability
  private volatile String lastPolledConsumerId = null;
  private final AtomicLong lastPolledTimestamp = new AtomicLong(INVALID_TIMESTAMP);
  private final AtomicLong committedTimestamp = new AtomicLong(INVALID_TIMESTAMP);

  /**
   * Constructs a {@link SubscriptionEvent} with an initial response.
   *
   * @param pipeEvents The underlying pipe events corresponding to this {@link SubscriptionEvent}.
   * @param initialResponse The initial response which must be of type {@link FileInitPayload}. This
   *     indicates that subsequent responses need to be fetched using {@link
   *     SubscriptionEvent#prefetchRemainingResponses()}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeEvents pipeEvents, final SubscriptionPollResponse initialResponse) {
    this.pipeEvents = pipeEvents;

    final int responseLength = getResponseLength(initialResponse.getResponseType());
    this.responses = new SubscriptionPollResponse[responseLength];
    this.responses[0] = initialResponse;

    this.byteBuffers = new ByteBuffer[responseLength];
    this.commitContext = initialResponse.getCommitContext();
  }

  /**
   * Constructs a {@link SubscriptionEvent} with a list of responses.
   *
   * @param pipeEvents The underlying pipe events corresponding to this {@link SubscriptionEvent}.
   * @param responses A list of responses that can be of types {@link TabletsPayload}, {@link
   *     TerminationPayload}, or {@link ErrorPayload}. All responses are already generated at the
   *     time of construction, so {@link SubscriptionEvent#prefetchRemainingResponses()} is not
   *     required.
   */
  public SubscriptionEvent(
      final SubscriptionPipeEvents pipeEvents, final List<SubscriptionPollResponse> responses) {
    this.pipeEvents = pipeEvents;

    final int responseLength = responses.size();
    this.responses = new SubscriptionPollResponse[responseLength];
    for (int i = 0; i < responseLength; i++) {
      this.responses[i] = responses.get(i);
    }

    this.byteBuffers = new ByteBuffer[responseLength];
    this.commitContext = this.responses[0].getCommitContext();
  }

  private int getResponseLength(final short responseType) {
    if (!Objects.equals(SubscriptionPollResponseType.FILE_INIT.getType(), responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return 1;
    }
    final long fileLength = pipeEvents.getTsFile().length();
    final long readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final int length = (int) (fileLength / readFileBufferSize);
    // add for init, last piece and seal
    return (fileLength % readFileBufferSize != 0) ? length + 3 : length + 2;
  }

  public SubscriptionPollResponse getCurrentResponse() {
    return getResponse(currentResponseIndex);
  }

  private SubscriptionPollResponse getResponse(final int index) {
    return responses[index];
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  //////////////////////////// commit ////////////////////////////

  public void recordCommittedTimestamp() {
    committedTimestamp.set(System.currentTimeMillis());
  }

  public boolean isCommitted() {
    if (commitContext.getCommitId() == INVALID_COMMIT_ID) {
      // event with invalid commit id is committed
      return true;
    }
    return committedTimestamp.get() != INVALID_TIMESTAMP;
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

  /**
   * NOTE: To ensure idempotency, currently, it is only allowed to call this method within the
   * {@link ConcurrentHashMap#compute} method of inFlightEvents in {@link
   * SubscriptionPrefetchingQueue} or {@link SubscriptionPrefetchingQueue#cleanUp}.
   */
  public void cleanUp() {
    // reset serialized responses
    resetResponseByteBuffer(true);

    // clean up pipe events
    pipeEvents.cleanUp();
  }

  //////////////////////////// pollable ////////////////////////////

  public void recordLastPolledTimestamp() {
    long currentTimestamp;
    long newTimestamp;

    do {
      currentTimestamp = lastPolledTimestamp.get();
      newTimestamp = Math.max(currentTimestamp, System.currentTimeMillis());
    } while (!lastPolledTimestamp.compareAndSet(currentTimestamp, newTimestamp));
  }

  /**
   * @return {@code true} if this event is pollable, including eagerly pollable (by active nack) and
   *     lazily pollable (by inactive recycle); For events that have already been committed, they
   *     are not pollable.
   */
  public boolean pollable() {
    if (isCommitted()) {
      return false;
    }
    if (lastPolledTimestamp.get() == INVALID_TIMESTAMP) {
      return true;
    }
    return canRecycle();
  }

  /**
   * @return {@code true} if this event is eagerly pollable; For events that have already been
   *     committed, they are not pollable.
   */
  public boolean eagerlyPollable() {
    if (isCommitted()) {
      return false;
    }
    return lastPolledTimestamp.get() == INVALID_TIMESTAMP;
  }

  private boolean canRecycle() {
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return System.currentTimeMillis() - lastPolledTimestamp.get()
        > SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs();
  }

  public void nack() {
    // reset current response index
    currentResponseIndex = 0;

    // reset lastPolledTimestamp makes this event pollable
    lastPolledTimestamp.set(INVALID_TIMESTAMP);
  }

  public void recordLastPolledConsumerId(final String consumerId) {
    lastPolledConsumerId = consumerId;
  }

  public String getLastPolledConsumerId() {
    return lastPolledConsumerId;
  }

  //////////////////////////// prefetch & fetch ////////////////////////////

  /**
   * @param index the index of response to be prefetched
   */
  private void prefetchResponse(final int index) throws IOException {
    if (index >= responses.length || index <= 0) {
      return;
    }

    if (Objects.nonNull(responses[index])) {
      return;
    }

    final SubscriptionPollResponse previousResponse = this.getResponse(index - 1);
    final short responseType = previousResponse.getResponseType();
    final SubscriptionPollPayload payload = previousResponse.getPayload();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      LOGGER.warn("unexpected response type: {}", responseType);
      return;
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        responses[index] = generateSubscriptionPollResponseWithPieceOrSealPayload(0);
        break;
      case FILE_PIECE:
        responses[index] =
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

  public void prefetchRemainingResponses() throws IOException {
    for (int currentIndex = currentResponseIndex;
        currentIndex < responses.length - 1;
        currentIndex++) {
      if (Objects.isNull(responses[currentIndex + 1])) {
        prefetchResponse(currentIndex + 1);
        return;
      }
    }
  }

  public void fetchNextResponse() throws IOException {
    if (currentResponseIndex >= responses.length - 1) {
      LOGGER.warn("No more responses when fetching next response for {}, do nothing.", this);
      return;
    }
    if (Objects.isNull(responses[currentResponseIndex + 1])) {
      prefetchRemainingResponses();
    }
    currentResponseIndex++;
  }

  //////////////////////////// byte buffer ////////////////////////////

  public void trySerializeRemainingResponses() {
    for (int currentIndex = currentResponseIndex;
        currentIndex < responses.length - 1;
        currentIndex++) {
      if (Objects.nonNull(responses[currentIndex + 1]) && trySerializeResponse(currentIndex + 1)) {
        break;
      }
    }
  }

  public boolean trySerializeCurrentResponse() {
    return trySerializeResponse(currentResponseIndex);
  }

  /**
   * @param index the index of response to be serialized
   * @return {@code true} if a serialization operation was actually performed
   */
  private boolean trySerializeResponse(final int index) {
    if (index >= responses.length) {
      return false;
    }

    if (Objects.isNull(responses[index])) {
      return false;
    }

    if (Objects.nonNull(byteBuffers[index])) {
      return false;
    }

    final Optional<ByteBuffer> optionalByteBuffer =
        SubscriptionEventBinaryCache.getInstance().trySerialize(responses[index]);
    if (optionalByteBuffer.isPresent()) {
      byteBuffers[index] = optionalByteBuffer.get();
      return true;
    }

    return false;
  }

  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    if (Objects.nonNull(byteBuffers[currentResponseIndex])) {
      return byteBuffers[currentResponseIndex];
    }

    return byteBuffers[currentResponseIndex] =
        SubscriptionEventBinaryCache.getInstance().serialize(getCurrentResponse());
  }

  public void resetResponseByteBuffer(final boolean resetAll) {
    if (resetAll) {
      SubscriptionEventBinaryCache.getInstance()
          .invalidateAll(
              Arrays.stream(responses).filter(Objects::nonNull).collect(Collectors.toList()));
      // maybe friendly for gc
      Arrays.fill(byteBuffers, null);
    } else {
      if (Objects.nonNull(responses[currentResponseIndex])) {
        SubscriptionEventBinaryCache.getInstance().invalidate(responses[currentResponseIndex]);
      }
      // maybe friendly for gc
      byteBuffers[currentResponseIndex] = null;
    }
  }

  public int getCurrentResponseSize() throws IOException {
    final ByteBuffer byteBuffer = getCurrentResponseByteBuffer();
    // refer to org.apache.thrift.protocol.TBinaryProtocol.writeBinary
    return byteBuffer.limit() - byteBuffer.position();
  }

  /////////////////////////////// tsfile ///////////////////////////////

  private @NonNull SubscriptionPollResponse generateSubscriptionPollResponseWithPieceOrSealPayload(
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

  public String getFileName() {
    return pipeEvents.getTsFile().getName();
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEvent{responses="
        + Arrays.toString(responses)
        + ", responses' byte buffer size="
        + Arrays.stream(byteBuffers)
            .map(
                byteBuffer ->
                    Objects.isNull(byteBuffer)
                        ? "<unknown>"
                        : byteBuffer.limit() - byteBuffer.position())
            .collect(Collectors.toList())
        + ", currentResponseIndex="
        + currentResponseIndex
        + ", lastPolledConsumerId="
        + lastPolledConsumerId
        + ", lastPolledTimestamp="
        + lastPolledTimestamp
        + ", committedTimestamp="
        + committedTimestamp
        + ", pipeEvents="
        + pipeEvents
        + "}";
  }
}
