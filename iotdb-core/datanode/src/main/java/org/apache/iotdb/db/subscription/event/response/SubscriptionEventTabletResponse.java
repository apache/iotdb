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
import org.apache.iotdb.db.subscription.event.cache.SubscriptionPollResponseCache;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionEventTabletResponse
    extends SubscriptionEventExtendableResponse<CachedSubscriptionPollResponse> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionEventTabletResponse.class);

  private static final long READ_TABLET_BUFFER_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionReadTabletBufferSize();

  private final SubscriptionPipeTabletEventBatch batch;
  private final SubscriptionCommitContext commitContext;
  private LinkedList<Tablet> tablets;
  private int tabletsSize;

  private final AtomicInteger nextOffset = new AtomicInteger(0);
  private volatile boolean hasNoMore = false;

  public SubscriptionEventTabletResponse(
      final SubscriptionPipeTabletEventBatch batch, final SubscriptionCommitContext commitContext) {
    this.batch = batch;
    this.commitContext = commitContext;
    init(batch);
  }

  private void init(final SubscriptionPipeTabletEventBatch batch) {
    this.tablets = batch.moveTablets();
    this.tabletsSize = this.tablets.size();
    offer(generateNextTabletResponse());
  }

  @Override
  public CachedSubscriptionPollResponse getCurrentResponse() {
    return peekFirst();
  }

  @Override
  public void prefetchRemainingResponses() {
    if (hasNoMore) {
      return;
    }

    offer(generateNextTabletResponse());
  }

  @Override
  public void fetchNextResponse() throws IOException {
    prefetchRemainingResponses();
    if (Objects.isNull(poll())) {
      LOGGER.warn("broken invariant");
    }
  }

  @Override
  public void trySerializeCurrentResponse() {
    SubscriptionPollResponseCache.getInstance().trySerialize(getCurrentResponse());
  }

  @Override
  public void trySerializeRemainingResponses() {
    stream()
        .filter(response -> Objects.isNull(response.getByteBuffer()))
        .findFirst()
        .ifPresent(response -> SubscriptionPollResponseCache.getInstance().trySerialize(response));
  }

  @Override
  public ByteBuffer getCurrentResponseByteBuffer() throws IOException {
    return SubscriptionPollResponseCache.getInstance().serialize(getCurrentResponse());
  }

  @Override
  public void resetCurrentResponseByteBuffer() {
    SubscriptionPollResponseCache.getInstance().invalidate(getCurrentResponse());
  }

  @Override
  public void reset() {
    cleanUp();
    init(batch);
  }

  @Override
  public void cleanUp() {
    CachedSubscriptionPollResponse response;
    while (Objects.nonNull(response = poll())) {
      SubscriptionPollResponseCache.getInstance().invalidate(response);
    }
    hasNoMore = false;
  }

  @Override
  public boolean isCommittable() {
    return hasNoMore && size() == 1;
  }

  /////////////////////////////// utility ///////////////////////////////

  private CachedSubscriptionPollResponse generateNextTabletResponse() {
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
        currentTablets.clear();
        currentTotalBufferSize.set(0);
        return response;
      }
      currentTablets.add(currentTablet);
      currentTotalBufferSize.addAndGet(bufferSize);
    }

    final CachedSubscriptionPollResponse response =
        new CachedSubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(new ArrayList<>(currentTablets), -tabletsSize),
            commitContext);
    hasNoMore = true;
    currentTablets.clear();
    currentTotalBufferSize.set(0);
    return response;
  }
}
