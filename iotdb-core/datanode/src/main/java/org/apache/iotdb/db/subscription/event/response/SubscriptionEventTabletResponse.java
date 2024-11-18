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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.cache.CachedSubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  private final SubscriptionPipeTabletEventBatch batch;
  private final SubscriptionCommitContext commitContext;

  private volatile int tabletsSize;
  private final AtomicInteger nextOffset = new AtomicInteger(0);

  public SubscriptionEventTabletResponse(
      final SubscriptionPipeTabletEventBatch batch, final SubscriptionCommitContext commitContext) {
    this.batch = batch;
    this.commitContext = commitContext;

    init();
  }

  @Override
  public void prefetchRemainingResponses() {
    // do nothing
  }

  @Override
  public void fetchNextResponse(final long offset /* unused */) {
    offer(generateNextTabletResponse());
    if (Objects.isNull(poll())) {
      LOGGER.warn(
          "SubscriptionEventTabletResponse {} is empty when fetching next response (broken invariant)",
          this);
    }
  }

  @Override
  public synchronized void nack() {
    if (nextOffset.get() == 1) {
      // do nothing if with complete tablets
      return;
    }
    cleanUp();
    init();
  }

  @Override
  public synchronized void cleanUp() {
    super.cleanUp();

    tabletsSize = 0;
    nextOffset.set(0);
  }

  /////////////////////////////// utility ///////////////////////////////

  private void init() {
    if (!isEmpty()) {
      LOGGER.warn(
          "SubscriptionEventTabletResponse {} is not empty when initializing (broken invariant)",
          this);
      return;
    }

    batch.resetIterator();
    offer(generateNextTabletResponse());
  }

  private synchronized CachedSubscriptionPollResponse generateNextTabletResponse() {
    final List<Tablet> currentTablets = new ArrayList<>();
    long currentBufferSize = 0;

    while (batch.hasNext()) {
      final List<Tablet> tablets = batch.next();
      if (Objects.isNull(tablets)) {
        continue;
      }

      currentTablets.addAll(tablets);
      final long bufferSize =
          tablets.stream()
              .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
              .reduce(Long::sum)
              .orElse(0L);
      tabletsSize += tablets.size();

      if (bufferSize > READ_TABLET_BUFFER_SIZE) {
        // TODO: split tablets
        LOGGER.warn("Detect large tablets with {} byte(s).", bufferSize);
        return new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(new ArrayList<>(currentTablets), nextOffset.incrementAndGet()),
            commitContext);
      }

      if (currentBufferSize + bufferSize > READ_TABLET_BUFFER_SIZE) {
        // TODO: split tablets
        return new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(new ArrayList<>(currentTablets), nextOffset.incrementAndGet()),
            commitContext);
      }

      currentBufferSize += bufferSize;
    }

    final CachedSubscriptionPollResponse response;
    if (currentTablets.isEmpty()) {
      response =
          new CachedSubscriptionPollResponse(
              SubscriptionPollResponseType.TABLETS.getType(),
              new TabletsPayload(Collections.emptyList(), -tabletsSize),
              commitContext);
      hasNoMore = true;
    } else {
      response =
          new CachedSubscriptionPollResponse(
              SubscriptionPollResponseType.TABLETS.getType(),
              new TabletsPayload(new ArrayList<>(currentTablets), nextOffset.incrementAndGet()),
              commitContext);
    }

    return response;
  }
}
