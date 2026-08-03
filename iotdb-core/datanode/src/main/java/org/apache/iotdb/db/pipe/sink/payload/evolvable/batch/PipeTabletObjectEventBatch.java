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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.Pair;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link PipeTabletEventBatch} for Object-bearing tablet events.
 *
 * <p>Object files are deliberately not read while accumulating events. The inherited batch
 * lifecycle uses the in-memory Tablet footprint for size accounting, and Object values are split
 * and serialized lazily when the async handler requests the next bounded RPC payload.
 */
public class PipeTabletObjectEventBatch extends PipeTabletEventBatch {

  private final long maxRequestSizeInBytes;
  private final long maxObjectFileSizeInBytes;
  private long totalObjectFileSizeInBytes;

  public PipeTabletObjectEventBatch(
      final int maxDelayInMs,
      final long maxBatchSizeInBytes,
      final long maxObjectFileSizeInBytes,
      final long maxRequestSizeInBytes,
      final TriLongConsumer recordMetric) {
    super(maxDelayInMs, maxBatchSizeInBytes, recordMetric);
    this.maxRequestSizeInBytes = Math.max(1, maxRequestSizeInBytes);
    this.maxObjectFileSizeInBytes = Math.max(1, maxObjectFileSizeInBytes);
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event) {
    increaseTotalBufferSizeAndUpdateMemoryBlock(estimateTabletMemoryUsageInBytes(event));
    totalObjectFileSizeInBytes =
        saturatedAdd(
            totalObjectFileSizeInBytes, estimateObjectFileSizeInBytes((PipeInsertionEvent) event));
    return true;
  }

  @Override
  public synchronized boolean shouldEmit() {
    return totalObjectFileSizeInBytes >= maxObjectFileSizeInBytes || super.shouldEmit();
  }

  @Override
  public synchronized void onSuccess() {
    resetAfterTransfer();
  }

  public synchronized void onFailure() {
    resetAfterTransfer();
  }

  private void resetAfterTransfer() {
    totalObjectFileSizeInBytes = 0;
    clearEventsAndResetMemoryUsage();
  }

  @Override
  protected void clearBatchData() {
    totalObjectFileSizeInBytes = 0;
  }

  private static long estimateTabletMemoryUsageInBytes(final TabletInsertionEvent event) {
    if (event instanceof PipeInsertNodeTabletInsertionEvent insertEvent) {
      return insertEvent.convertToTablets().stream()
          .mapToLong(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
          .sum();
    }
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(
        ((PipeRawTabletInsertionEvent) event).convertToTablet());
  }

  public synchronized int size() {
    return events.size();
  }

  public synchronized EmittedBatch emit(final long batchId) {
    if (events.isEmpty()) {
      return null;
    }

    return new EmittedBatch(this, deepCopyEvents(), batchId, maxRequestSizeInBytes);
  }

  private static long estimateObjectFileSizeInBytes(final PipeInsertionEvent event) {
    if (event.getObjectFileSizeInBytes() >= 0) {
      return event.getObjectFileSizeInBytes();
    }
    long totalSizeInBytes = 0;
    final Iterator<Pair<Long, String>> objectPathAndSizeIterator =
        event.objectPathAndSizeIterator();
    while (objectPathAndSizeIterator.hasNext()) {
      totalSizeInBytes = saturatedAdd(totalSizeInBytes, objectPathAndSizeIterator.next().getLeft());
    }
    event.setObjectFileSizeInBytes(totalSizeInBytes);
    return totalSizeInBytes;
  }

  private static long saturatedAdd(final long left, final long right) {
    return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
  }

  public static final class EmittedBatch {
    private final PipeTabletObjectEventBatch owner;
    private final List<EnrichedEvent> events;
    private final long batchId;
    private final long maxRequestSizeInBytes;

    private EmittedBatch(
        final PipeTabletObjectEventBatch owner,
        final List<EnrichedEvent> events,
        final long batchId,
        final long maxRequestSizeInBytes) {
      this.owner = owner;
      this.events = events;
      this.batchId = batchId;
      this.maxRequestSizeInBytes = maxRequestSizeInBytes;
    }

    public List<EnrichedEvent> getEvents() {
      return events;
    }

    public long getBatchId() {
      return batchId;
    }

    public long getMaxRequestSizeInBytes() {
      return maxRequestSizeInBytes;
    }

    public void onSuccess() {
      owner.onSuccess();
    }

    public void onFailure() {
      owner.onFailure();
    }
  }
}
