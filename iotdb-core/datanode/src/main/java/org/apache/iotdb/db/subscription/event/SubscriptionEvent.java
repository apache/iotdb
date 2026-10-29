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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEmptyEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;
import org.apache.iotdb.db.subscription.event.response.SubscriptionEventResponse;
import org.apache.iotdb.db.subscription.event.response.SubscriptionEventSingleResponse;
import org.apache.iotdb.db.subscription.event.response.SubscriptionEventTabletResponse;
import org.apache.iotdb.db.subscription.event.response.SubscriptionEventTsFileResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.thrift.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SubscriptionEvent implements Comparable<SubscriptionEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEvent.class);

  private static final long INVALID_TIMESTAMP = -1;

  private final SubscriptionPipeEvents pipeEvents;
  private final SubscriptionEventResponse<CachedSubscriptionPollResponse> response;
  private final SubscriptionCommitContext commitContext;

  private volatile boolean payloadDetached;
  private volatile Consumer<SubscriptionPollResponse> responseDeliveryListener;

  // lastPolledConsumerId is not used as a criterion for determining pollability
  private volatile String lastPolledConsumerId = null;
  private final AtomicLong lastPolledTimestamp = new AtomicLong(INVALID_TIMESTAMP);
  private final AtomicLong committedTimestamp = new AtomicLong(INVALID_TIMESTAMP);

  // record file name for file payload
  private volatile String fileName;

  // record for cross-event dataset payload
  private volatile SubscriptionCommitContext rootCommitContext;

  private static final long NACK_COUNT_REPORT_THRESHOLD = 3;

  private static final long POISON_MESSAGE_NACK_THRESHOLD = 10;

  private final AtomicLong nackCount = new AtomicLong();

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventSingleResponse}.
   */
  public SubscriptionEvent(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this(responseType, payload, commitContext, true);
  }

  public SubscriptionEvent(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext,
      final boolean timeSelected) {
    this(responseType, payload, commitContext, timeSelected, null);
  }

  public SubscriptionEvent(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext,
      final boolean timeSelected,
      final Map<String, Map<String, Boolean>> timeSelectedByTable) {
    this.pipeEvents = new SubscriptionPipeEmptyEvent();
    this.response =
        new SubscriptionEventSingleResponse(
            responseType, payload, commitContext, timeSelected, timeSelectedByTable);
    this.commitContext = commitContext;
  }

  @TestOnly
  public SubscriptionEvent(final SubscriptionPollResponse response) {
    this(
        response.getResponseType(),
        response.getPayload(),
        response.getCommitContext(),
        response.isTimeSelected(),
        response.getTimeSelectedByTable());
  }

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventTabletResponse}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeTabletEventBatch batch, final SubscriptionPrefetchingQueue queue) {
    final SubscriptionPipeTabletBatchEvents events = new SubscriptionPipeTabletBatchEvents(batch);
    final SubscriptionCommitContext commitContext = queue.generateSubscriptionCommitContext();

    this.pipeEvents = events;
    this.response =
        new SubscriptionEventTabletResponse(batch, queue, events, commitContext, commitContext);
    this.commitContext = commitContext;

    // root event for cross-event dataset payload
    this.rootCommitContext = commitContext;
  }

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventTabletResponse}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeTabletEventBatch batch,
      final SubscriptionPrefetchingQueue queue,
      final SubscriptionCommitContext rootCommitContext) {
    final SubscriptionPipeTabletBatchEvents events = new SubscriptionPipeTabletBatchEvents(batch);
    final SubscriptionCommitContext commitContext = queue.generateSubscriptionCommitContext();

    this.pipeEvents = events;
    this.response =
        new SubscriptionEventTabletResponse(batch, queue, events, commitContext, rootCommitContext);
    this.commitContext = commitContext;

    this.rootCommitContext = rootCommitContext;
  }

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventTsFileResponse}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeEvents pipeEvents,
      final File tsFile,
      @Nullable final String databaseName,
      final SubscriptionCommitContext commitContext) {
    this.pipeEvents = pipeEvents;
    this.response = new SubscriptionEventTsFileResponse(tsFile, databaseName, commitContext);
    this.commitContext = commitContext;

    this.fileName = tsFile.getName();
  }

  public SubscriptionPollResponse getCurrentResponse() {
    return response.getCurrentResponse();
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  //////////////////////////// commit ////////////////////////////

  public void recordCommittedTimestamp() {
    committedTimestamp.set(System.currentTimeMillis());
  }

  public boolean isCommitted() {
    if (!commitContext.isCommittable()) {
      // fire-and-forget events are treated as already committed
      return true;
    }
    return committedTimestamp.get() != INVALID_TIMESTAMP;
  }

  public boolean isCommittable() {
    if (!commitContext.isCommittable()) {
      return false;
    }
    return response.isCommittable();
  }

  public void ack() {
    pipeEvents.ack();
  }

  /**
   * NOTE: To ensure idempotency, currently, it is only allowed to call this method within the
   * {@link ConcurrentHashMap#compute} method of inFlightEvents in {@link
   * SubscriptionPrefetchingQueue} or {@link SubscriptionPrefetchingQueue#cleanUp}.
   */
  public synchronized void cleanUp(final boolean force) {
    pipeEvents.cleanUp(force);
    response.cleanUp();
    responseDeliveryListener = null;

    // TODO: clean more fields
  }

  /** Marks this event's serialized response as safe for a broker to detach, if supported. */
  public void markResponseDelivered(final SubscriptionPollResponse deliveredResponse) {
    final Consumer<SubscriptionPollResponse> listener = responseDeliveryListener;
    if (listener != null) {
      listener.accept(deliveredResponse);
    }
  }

  public void setResponseDeliveryListener(final Consumer<SubscriptionPollResponse> listener) {
    responseDeliveryListener = listener;
  }

  public boolean isPayloadDetached() {
    return payloadDetached;
  }

  /** Detaches the tablet payload while preserving commit and response metadata. */
  public synchronized boolean detachCurrentPayload(
      final SubscriptionPollResponse deliveredResponse) {
    final CachedSubscriptionPollResponse currentResponse = response.getCurrentResponse();
    if (payloadDetached
        || !(response instanceof SubscriptionEventSingleResponse)
        || currentResponse != deliveredResponse
        || !(currentResponse.getPayload() instanceof TabletsPayload)) {
      return false;
    }
    final int nextOffset = ((TabletsPayload) currentResponse.getPayload()).getNextOffset();
    ((SubscriptionEventSingleResponse) response)
        .replaceCurrentPayload(new TabletsPayload(Collections.emptyMap(), nextOffset));
    payloadDetached = true;
    return true;
  }

  /** Restores a detached payload before redelivery. */
  public synchronized void restorePayload(final SubscriptionPollPayload payload) {
    ((SubscriptionEventSingleResponse) response).replaceCurrentPayload(payload);
    payloadDetached = false;
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
    return pollable(
        SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs());
  }

  public boolean pollable(final long recycleUncommittedEventIntervalMs) {
    if (isCommitted()) {
      return false;
    }
    if (lastPolledTimestamp.get() == INVALID_TIMESTAMP) {
      return true;
    }
    return canRecycle(recycleUncommittedEventIntervalMs);
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

  private boolean canRecycle(final long recycleUncommittedEventIntervalMs) {
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return recycleUncommittedEventIntervalMs > 0L
        && System.currentTimeMillis() - lastPolledTimestamp.get()
            > recycleUncommittedEventIntervalMs;
  }

  public synchronized void nack() {
    // nack response
    response.nack();

    // reset lastPolledTimestamp makes this event pollable
    lastPolledTimestamp.set(INVALID_TIMESTAMP);

    // record nack count
    if (nackCount.getAndIncrement() > NACK_COUNT_REPORT_THRESHOLD) {
      LOGGER.warn(DataNodeMiscMessages.EVENT_NACKED_TIMES, this, nackCount);
    }
  }

  /** Rewinds a delivery after unsubscribe without counting it as a poison-message retry. */
  public synchronized void resetForRedelivery() {
    response.nack();
    lastPolledTimestamp.set(INVALID_TIMESTAMP);
  }

  /** Makes this event pollable again without treating local response-size control as a nack. */
  public void resetLastPolledTimestamp() {
    lastPolledTimestamp.set(INVALID_TIMESTAMP);
  }

  /** Returns the current nack count for this event. */
  public long getNackCount() {
    return nackCount.get();
  }

  public boolean isPoisoned() {
    return nackCount.get() >= POISON_MESSAGE_NACK_THRESHOLD;
  }

  public void recordLastPolledConsumerId(final String consumerId) {
    lastPolledConsumerId = consumerId;
  }

  public String getLastPolledConsumerId() {
    return lastPolledConsumerId;
  }

  //////////////////////////// prefetch & fetch ////////////////////////////

  public void prefetchRemainingResponses() {
    response.prefetchRemainingResponses();
  }

  public void fetchNextResponse(final long offset) throws Exception {
    response.fetchNextResponse(offset);
  }

  //////////////////////////// byte buffer ////////////////////////////

  public void trySerializeRemainingResponses() {
    response.trySerializeRemainingResponses();
  }

  public synchronized void trySerializeCurrentResponse() {
    if (!payloadDetached) {
      response.trySerializeCurrentResponse();
    }
  }

  public synchronized ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    if (payloadDetached) {
      throw new IOException(DataNodeMiscMessages.NULL_RESPONSE_WHEN_SERIALIZING);
    }
    return response.getCurrentResponseByteBuffer();
  }

  /** Serializes the selected response while fencing payload replacement and cleanup. */
  public synchronized ByteBuffer getResponseByteBuffer(
      final SubscriptionPollResponse selectedResponse) throws IOException {
    if (selectedResponse != response.getCurrentResponse()) {
      throw new IOException(DataNodeMiscMessages.NULL_RESPONSE_WHEN_SERIALIZING);
    }
    return getCurrentResponseByteBuffer();
  }

  public void invalidateCurrentResponseByteBuffer() {
    response.invalidateCurrentResponseByteBuffer();
  }

  public int getCurrentResponseSize() throws IOException {
    final ByteBuffer byteBuffer = getCurrentResponseByteBuffer();
    // refer to org.apache.thrift.protocol.TBinaryProtocol.writeBinary
    return byteBuffer.limit() - byteBuffer.position();
  }

  //////////////////////////// tsfile ////////////////////////////

  public String getFileName() {
    return fileName;
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getPipeEventCount() {
    return pipeEvents.getPipeEventCount();
  }

  //////////////////////////// object ////////////////////////////

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("commitContext", commitContext)
        .add("response", response)
        .add("lastPolledConsumerId", lastPolledConsumerId)
        .add("lastPolledTimestamp", lastPolledTimestamp)
        .add("committedTimestamp", committedTimestamp)
        .add("pipeEvents", pipeEvents)
        .add(
            "rootCommitContext",
            Objects.nonNull(rootCommitContext) ? rootCommitContext : "<unknown>")
        .toString();
  }

  @Override
  public int compareTo(final SubscriptionEvent that) {
    final SubscriptionCommitContext thisCommitContext =
        Objects.nonNull(this.rootCommitContext) ? this.rootCommitContext : this.commitContext;
    final SubscriptionCommitContext thatCommitContext =
        Objects.nonNull(that.rootCommitContext) ? that.rootCommitContext : that.commitContext;
    return thisCommitContext.compareTo(thatCommitContext);
  }
}
