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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

  private volatile LinkedList<Tablet> tablets;
  private volatile int tabletsSize;
  private final AtomicInteger nextOffset = new AtomicInteger(0);

  public SubscriptionEventTabletResponse(
      final SubscriptionPipeTabletEventBatch batch, final SubscriptionCommitContext commitContext) {
    this.batch = batch;
    this.commitContext = commitContext;

    init(batch);
  }

  @Override
  public void prefetchRemainingResponses() {
    if (hasNoMore) {
      return;
    }

    offer(generateNextTabletResponse());
  }

  @Override
  public void nack() {
    cleanUp();
    init(batch);
  }

  @Override
  public void cleanUp() {
    super.cleanUp();

    tablets = null;
    tabletsSize = 0;
    nextOffset.set(0);
  }

  /////////////////////////////// utility ///////////////////////////////

  private void init(final SubscriptionPipeTabletEventBatch batch) {
    if (!isEmpty()) {
      LOGGER.warn(
          "SubscriptionEventTabletResponse {} is not empty when initializing (broken invariant)",
          this);
      return;
    }

    tablets = batch.moveTablets();
    tabletsSize = tablets.size();
    offer(generateNextTabletResponse());
  }

  private synchronized CachedSubscriptionPollResponse generateNextTabletResponse() {
    final List<Tablet> currentTablets = new ArrayList<>();
    final AtomicLong currentTotalBufferSize = new AtomicLong();

    Tablet currentTablet;
    while (!tablets.isEmpty() && Objects.nonNull(currentTablet = tablets.removeFirst())) {
      final long bufferSize = PipeMemoryWeightUtil.calculateTabletSizeInBytes(currentTablet);
      if (bufferSize > READ_TABLET_BUFFER_SIZE) {
        LOGGER.warn("Detect large tablet with {} byte(s).", bufferSize);
        tablets.addAll(currentTablets); // re-enqueue previous tablets
        currentTablets.clear();
        currentTotalBufferSize.set(0);
        return new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(
                Collections.singletonList(currentTablet), nextOffset.incrementAndGet()),
            commitContext);
      }
      if (currentTotalBufferSize.get() + bufferSize > READ_TABLET_BUFFER_SIZE) {
        final CachedSubscriptionPollResponse response =
            new CachedSubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(new ArrayList<>(currentTablets), nextOffset.incrementAndGet()),
                commitContext);
        tablets.add(currentTablet); // re-enqueue current tablet
        currentTablets.clear();
        currentTotalBufferSize.set(0);
        return response;
      }
      currentTablets.add(currentTablet);
      currentTotalBufferSize.addAndGet(bufferSize);
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
    currentTablets.clear();
    currentTotalBufferSize.set(0);
    return response;
  }
}
