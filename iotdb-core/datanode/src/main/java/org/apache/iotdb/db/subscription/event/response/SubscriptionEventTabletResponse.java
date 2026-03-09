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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@code SubscriptionEventTabletResponse} class extends {@link
 * SubscriptionEventExtendableResponse} to handle subscription responses specifically for tablet
 * data. The actual payload of the response includes a {@link TabletsPayload}, which contains the
 * tablet information being processed.
 */
public class SubscriptionEventTabletResponse extends SubscriptionEventExtendableResponse {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTabletResponse.class);

  private static final long READ_TABLET_BUFFER_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionReadTabletBufferSize();

  private static final long PREFETCH_TABLET_BUFFER_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTabletBatchMaxSizeInBytes();

  private final SubscriptionPipeTabletEventBatch batch;
  private final SubscriptionPrefetchingQueue queue;
  private final SubscriptionPipeTabletBatchEvents events;

  private final SubscriptionCommitContext commitContext;
  private final SubscriptionCommitContext rootCommitContext;

  private volatile int totalTablets;
  private final AtomicInteger nextOffset = new AtomicInteger(0);

  // use these variables to limit control for large message
  private volatile long totalBufferSize;
  private volatile boolean availableForNext = false;

  public SubscriptionEventTabletResponse(
      final SubscriptionPipeTabletEventBatch batch,
      final SubscriptionPrefetchingQueue queue,
      final SubscriptionPipeTabletBatchEvents events,
      final SubscriptionCommitContext commitContext,
      final SubscriptionCommitContext rootCommitContext) {
    this.batch = batch;
    this.queue = queue;
    this.events = events;

    this.commitContext = commitContext;
    this.rootCommitContext = rootCommitContext;

    init();
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse(final long offset /* unused */) throws Exception {
    // generate and offer next response
    offer(generateNextTabletResponse());

    // poll and clean previous response
    final CachedSubscriptionPollResponse previousResponse;
    if (Objects.isNull(previousResponse = poll())) {
      LOGGER.warn(
          "SubscriptionEventTabletResponse {} is empty when fetching next response (broken invariant)",
          this);
    } else {
      previousResponse.closeMemoryBlock();
    }
  }

  @Override
  public synchronized void nack() {
    cleanUp();

    // should not reset the iterator of batch when init
    // TODO: avoid completely rewinding the iterator
    batch.resetForIteration();
    init();
  }

  @Override
  public synchronized void cleanUp() {
    super.cleanUp();

    totalTablets = 0;
    nextOffset.set(0);

    totalBufferSize = 0;
    availableForNext = false;
  }

  @Override
  public boolean isCommittable() {
    return (availableForNext || hasNoMore) && size() == 1;
  }

  /////////////////////////////// utility ///////////////////////////////

  private void init() {
    if (!isEmpty()) {
      LOGGER.warn(
          "SubscriptionEventTabletResponse {} is not empty when initializing (broken invariant)",
          this);
      return;
    }

    offer(generateEmptyTabletResponse());
  }

  private synchronized CachedSubscriptionPollResponse generateEmptyTabletResponse() {
    return new CachedSubscriptionPollResponse(
        SubscriptionPollResponseType.TABLETS.getType(),
        new TabletsPayload(Collections.emptyList(), nextOffset.incrementAndGet()),
        commitContext);
  }

  private synchronized CachedSubscriptionPollResponse generateNextTabletResponse()
      throws InterruptedException, PipeRuntimeOutOfMemoryCriticalException {
    if (availableForNext) {
      // generate next subscription event with the same batch
      queue.prefetchEvent(new SubscriptionEvent(batch, queue, rootCommitContext));
      // frozen iterated enriched events
      transportIterationSnapshot();
      // return last response of this subscription event
      return new CachedSubscriptionPollResponse(
          SubscriptionPollResponseType.TABLETS.getType(),
          new TabletsPayload(Collections.emptyList(), -totalTablets),
          commitContext);
    }

    CachedSubscriptionPollResponse response = null;
    final Map<String, List<Tablet>> currentTablets = new HashMap<>();
    long currentBufferSize = 0;

    // TODO: TBD.
    // waitForResourceEnough4Parsing(SubscriptionAgent.receiver().remainingMs());

    while (batch.hasNext()) {
      final Pair<String, List<Tablet>> tablets = batch.next();
      if (Objects.isNull(tablets)) {
        continue;
      }

      currentTablets
          .computeIfAbsent(tablets.left, databaseName -> new ArrayList<>())
          .addAll(tablets.right);
      final long bufferSize =
          tablets.right.stream()
              .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
              .reduce(Long::sum)
              .orElse(0L);
      totalTablets += tablets.right.size();
      totalBufferSize += bufferSize;
      currentBufferSize += bufferSize;

      if (bufferSize > READ_TABLET_BUFFER_SIZE) {
        // TODO: split tablets
        LOGGER.warn(
            "Detect large tablets with {} byte(s), current tablets size {} byte(s)",
            bufferSize,
            currentTablets);
        response =
            new CachedSubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(new HashMap<>(currentTablets), nextOffset.incrementAndGet()),
                commitContext);
        break;
      }

      if (currentBufferSize > READ_TABLET_BUFFER_SIZE) {
        // TODO: split tablets
        response =
            new CachedSubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(new HashMap<>(currentTablets), nextOffset.incrementAndGet()),
                commitContext);
        break;
      }

      // limit control for large message
      if (totalBufferSize > PREFETCH_TABLET_BUFFER_SIZE && batch.hasNext()) {
        // we generate seal signal at next round
        availableForNext = true;
        break;
      }
    }

    if (Objects.isNull(response)) {
      if (currentTablets.isEmpty()) {
        // frozen iterated enriched events
        transportIterationSnapshot();
        // return last response of this subscription event
        response =
            new CachedSubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(Collections.emptyList(), -totalTablets),
                commitContext);
        hasNoMore = true;
      } else {
        response =
            new CachedSubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(new HashMap<>(currentTablets), nextOffset.incrementAndGet()),
                commitContext);
      }
    }

    // set fixed memory block for response
    final List<Tablet> tablets = ((TabletsPayload) response.getPayload()).getTablets();
    if (Objects.nonNull(tablets) && !tablets.isEmpty()) {
      final PipeTabletMemoryBlock memoryBlock =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(currentBufferSize);
      response.setMemoryBlock(memoryBlock);
    }

    return response;
  }

  private void waitForResourceEnough4Parsing(final long timeoutMs) throws InterruptedException {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (memoryManager.isEnough4TabletParsing()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    final long memoryCheckIntervalMs =
        SubscriptionConfig.getInstance().getSubscriptionCheckMemoryEnoughIntervalMs();
    while (!memoryManager.isEnough4TabletParsing()) {
      Thread.sleep(memoryCheckIntervalMs);

      final long currentTime = System.currentTimeMillis();
      final double elapsedRecordTimeSeconds = (currentTime - lastRecordTime) / 1000.0;
      final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
      if (elapsedRecordTimeSeconds > 10.0) {
        LOGGER.info(
            "SubscriptionEventTabletResponse {} wait for resource enough for parsing tablets {} seconds.",
            commitContext,
            waitTimeSeconds);
        lastRecordTime = currentTime;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "SubscriptionEventTabletResponse {} wait for resource enough for parsing tablets {} seconds.",
            commitContext,
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
        "SubscriptionEventTabletResponse {} wait for resource enough for parsing tablets {} seconds.",
        commitContext,
        waitTimeSeconds);
  }

  private void transportIterationSnapshot() {
    events.receiveIterationSnapshot(batch.sendIterationSnapshot());
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEventTabletResponse" + coreReportMessage();
  }

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = super.coreReportMessage();
    result.put("totalTablets", String.valueOf(totalTablets));
    result.put("nextOffset", String.valueOf(nextOffset));
    result.put("totalBufferSize", String.valueOf(totalBufferSize));
    result.put("availableForNext", String.valueOf(availableForNext));
    return result;
  }
}
