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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEventBinaryCache;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionPrefetchingTabletQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTabletQueue.class);

  public SubscriptionPrefetchingTabletQueue(
      final String brokerId,
      final String topicName,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.currentBatchRef.set(
        new SubscriptionPipeTabletEventBatch(maxDelayInMs, maxBatchSizeInBytes));
  }

  @Override
  public void cleanup() {
    super.cleanup();

    // no need to clean up events in prefetchingQueue, since all events in prefetchingQueue are also
    // in uncommittedEvents
    prefetchingQueue.clear();
  }

  @Override
  public void executePrefetch() {
    prefetchOnce();
    serializeOnce();
  }

  @Override
  protected boolean prefetchTabletInsertionEvent(final TabletInsertionEvent event) {
    return prefetchEnrichedEvent((EnrichedEvent) event);
  }

  @Override
  protected boolean prefetchTsFileInsertionEvent(final PipeTsFileInsertionEvent event) {
    return prefetchEnrichedEvent(event);
  }

  private boolean prefetchEnrichedEvent(final EnrichedEvent event) {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          try {
            if (batch.onEvent(event)) {
              consumeBatch((SubscriptionPipeTabletEventBatch) batch);
              result.set(true);
              return new SubscriptionPipeTabletEventBatch(maxDelayInMs, maxBatchSizeInBytes);
            }
            return batch;
          } catch (final Exception ignored) {
            return batch;
          }
        });
    return result.get();
  }

  @Override
  protected boolean prefetchEnrichedEvent() {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          try {
            if (batch.shouldEmit()) {
              consumeBatch((SubscriptionPipeTabletEventBatch) batch);
              result.set(true);
              return new SubscriptionPipeTabletEventBatch(maxDelayInMs, maxBatchSizeInBytes);
            }
            return batch;
          } catch (final Exception ignored) {
            return batch;
          }
        });
    return result.get();
  }

  private void consumeBatch(final SubscriptionPipeTabletEventBatch batch) {
    final List<Tablet> tablets = batch.sealTablets();
    final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
    final SubscriptionEvent subscriptionEvent =
        new SubscriptionEvent(
            new SubscriptionPipeTabletBatchEvents(batch),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(tablets),
                commitContext));
    uncommittedEvents.put(commitContext, subscriptionEvent); // before enqueuing the event
    prefetchingQueue.add(subscriptionEvent);
  }

  private void serializeOnce() {
    final long size = prefetchingQueue.size();
    long count = 0;

    SubscriptionEvent event;
    try {
      while (count++ < size // limit control
          && Objects.nonNull(
              event =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        if (event.isCommitted()) {
          event.cleanup();
          continue;
        }
        // Serialize the uncommitted and pollable event.
        if (event.pollable()) {
          // No need to concern whether serialization is successful.
          SubscriptionEventBinaryCache.getInstance().trySerialize(event);
        }
        // Re-enqueue the uncommitted event at the end of the queue.
        prefetchingQueue.add(event);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingTabletQueue {} interrupted while serializing events.",
          this,
          e);
    }
  }
}
