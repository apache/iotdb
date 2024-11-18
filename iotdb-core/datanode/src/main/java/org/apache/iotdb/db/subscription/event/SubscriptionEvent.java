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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public class SubscriptionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEvent.class);

  private static final long INVALID_TIMESTAMP = -1;

  private final SubscriptionPipeEvents pipeEvents;
  private final SubscriptionEventResponse<CachedSubscriptionPollResponse> response;
  private final SubscriptionCommitContext commitContext;

  // lastPolledConsumerId is not used as a criterion for determining pollability
  private volatile String lastPolledConsumerId = null;
  private final AtomicLong lastPolledTimestamp = new AtomicLong(INVALID_TIMESTAMP);
  private final AtomicLong committedTimestamp = new AtomicLong(INVALID_TIMESTAMP);

  // record file name for file payload
  private String fileName;

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventSingleResponse}.
   */
  public SubscriptionEvent(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this.pipeEvents = new SubscriptionPipeEmptyEvent();
    this.response = new SubscriptionEventSingleResponse(responseType, payload, commitContext);
    this.commitContext = commitContext;
  }

  @TestOnly
  public SubscriptionEvent(final SubscriptionPollResponse response) {
    this(response.getResponseType(), response.getPayload(), response.getCommitContext());
  }

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventTabletResponse}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeTabletEventBatch batch, final SubscriptionCommitContext commitContext) {
    this.pipeEvents = new SubscriptionPipeTabletBatchEvents(batch);
    this.response = new SubscriptionEventTabletResponse(batch, commitContext);
    this.commitContext = commitContext;
  }

  /**
   * Constructs a {@link SubscriptionEvent} with the response type of {@link
   * SubscriptionEventTsFileResponse}.
   */
  public SubscriptionEvent(
      final SubscriptionPipeEvents pipeEvents,
      final File tsFile,
      final SubscriptionCommitContext commitContext) {
    this.pipeEvents = pipeEvents;
    this.response = new SubscriptionEventTsFileResponse(tsFile, commitContext);
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
  public void cleanUp() {
    // reset serialized responses
    response.cleanUp();

    // clean up pipe events
    pipeEvents.cleanUp();

    // TODO: clean more fields
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
    // nack response
    response.nack();

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

  public void prefetchRemainingResponses() throws Exception {
    response.prefetchRemainingResponses();
  }

  public void fetchNextResponse(final long offset) throws Exception {
    response.fetchNextResponse(offset);
  }

  //////////////////////////// byte buffer ////////////////////////////

  public void trySerializeRemainingResponses() {
    response.trySerializeRemainingResponses();
  }

  public void trySerializeCurrentResponse() {
    response.trySerializeCurrentResponse();
  }

  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return response.getCurrentResponseByteBuffer();
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
    return "SubscriptionEvent{response="
        + response
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
