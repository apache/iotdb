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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.util.PipeObjectPathUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletObjectEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletObjectBatchReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.storageengine.load.converter.TabletObjectSplitIterator;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Splits one cross-event Object batch into an ACK-ordered sequence of bounded requests. */
public class PipeTabletObjectBatchRequestIterator
    implements Iterator<TPipeTransferReq>, AutoCloseable {

  private final List<EnrichedEvent> events;
  private final long batchId;
  private final long maxRequestSizeInBytes;
  private int nextEventIndex;
  private List<Tablet> currentTablets = Collections.emptyList();
  private List<Boolean> currentAlignments = Collections.emptyList();
  private PipeInsertionEvent currentEvent;
  private int nextTabletIndex;
  private TabletObjectSplitIterator currentTabletSplitIterator;
  private byte[] cachedSerializedTablet;
  private Pair<String, Long> cachedPipeIdentity;
  private final Map<Pair<String, Long>, Long> currentRequestPipeIdentity2Bytes = new HashMap<>();

  private int sequenceId;

  public PipeTabletObjectBatchRequestIterator(final PipeTabletObjectEventBatch.EmittedBatch batch) {
    events = batch.getEvents();
    batchId = batch.getBatchId();
    maxRequestSizeInBytes = batch.getMaxRequestSizeInBytes();
  }

  @Override
  public boolean hasNext() {
    return ensureCachedSerializedTablet();
  }

  @Override
  public TPipeTransferReq next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final List<byte[]> requestTablets = new ArrayList<>();
    currentRequestPipeIdentity2Bytes.clear();
    long requestBytes = PipeTransferTabletObjectBatchReq.getSerializedBatchHeaderSize();
    while (ensureCachedSerializedTablet()) {
      final byte[] tablet = cachedSerializedTablet;
      final long tabletBytes = PipeTransferTabletObjectBatchReq.getSerializedTabletSize(tablet);
      if (!requestTablets.isEmpty() && requestBytes + tabletBytes > maxRequestSizeInBytes) {
        break;
      }
      requestTablets.add(tablet);
      requestBytes += tabletBytes;
      currentRequestPipeIdentity2Bytes.merge(cachedPipeIdentity, tabletBytes, Long::sum);
      cachedSerializedTablet = null;
      cachedPipeIdentity = null;
    }
    final boolean last = !ensureCachedSerializedTablet();
    try {
      return PipeTransferTabletObjectBatchReq.toTPipeTransferReq(
          batchId, sequenceId++, last, requestTablets);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public Map<Pair<String, Long>, Long> getCurrentRequestPipeIdentity2Bytes() {
    return new HashMap<>(currentRequestPipeIdentity2Bytes);
  }

  /**
   * Rebuilds the lazy request stream and positions it at the requested sequence.
   *
   * @throws IllegalArgumentException if {@code expectedSequenceId} is negative or beyond the stream
   */
  public void resetToSequence(final int expectedSequenceId) {
    if (expectedSequenceId < 0) {
      throw new IllegalArgumentException();
    }

    closeCurrentTabletSplitIterator();
    nextEventIndex = 0;
    currentTablets = Collections.emptyList();
    currentAlignments = Collections.emptyList();
    currentEvent = null;
    nextTabletIndex = 0;
    cachedSerializedTablet = null;
    cachedPipeIdentity = null;
    currentRequestPipeIdentity2Bytes.clear();
    sequenceId = 0;

    for (int skipped = 0; skipped < expectedSequenceId; skipped++) {
      if (!hasNext()) {
        throw new IllegalArgumentException();
      }
      next();
    }
  }

  private boolean ensureCachedSerializedTablet() {
    if (cachedSerializedTablet != null) {
      return true;
    }

    try {
      while (true) {
        if (tryCacheFromCurrentSplitIterator()) {
          return true;
        }
        closeCurrentTabletSplitIterator();

        if (tryStartNextTabletSplitIterator()) {
          continue;
        }

        if (nextEventIndex >= events.size()) {
          return false;
        }
        prepareNextEvent((TabletInsertionEvent) events.get(nextEventIndex));
        nextEventIndex++;
      }
    } catch (final Exception e) {
      close();
      throw new IllegalStateException(e);
    }
  }

  private boolean tryCacheFromCurrentSplitIterator() throws IOException {
    if (currentTabletSplitIterator == null || !currentTabletSplitIterator.hasNext()) {
      return false;
    }
    cachedSerializedTablet =
        PipeTransferTabletRawReqV2.toTPipeTransferReq(
                currentTabletSplitIterator.next(),
                currentAlignments.get(nextTabletIndex - 1),
                currentEvent.getTableModelDatabaseName())
            .getBody();
    cachedPipeIdentity = currentPipeIdentity();
    return true;
  }

  private boolean tryStartNextTabletSplitIterator() {
    if (nextTabletIndex >= currentTablets.size()) {
      return false;
    }
    final Tablet tablet = currentTablets.get(nextTabletIndex++);
    currentTabletSplitIterator =
        new TabletObjectSplitIterator(
            tablet,
            currentEvent.getTsFileResource() == null
                ? null
                : currentEvent.getTsFileResource().getTsFile(),
            currentEvent.getTsFileResource() == null || currentEvent.getPipeName() == null
                ? null
                : PipeObjectPathUtil.resolveLinkedObjectDirectory(
                    currentEvent.getTsFileResource(), currentEvent.getPipeName()),
            true,
            (int) maxRequestSizeInBytes);
    return true;
  }

  private void prepareNextEvent(final TabletInsertionEvent event) throws IOException {
    currentEvent = (PipeInsertionEvent) event;
    nextTabletIndex = 0;
    if (event instanceof PipeRawTabletInsertionEvent rawEvent
        && rawEvent.isObjectValueContentEvent()) {
      cacheRawObjectValueContentTablet(rawEvent);
      return;
    }

    if (event instanceof PipeInsertNodeTabletInsertionEvent insertEvent) {
      currentTablets = insertEvent.convertToTablets();
      currentAlignments = new ArrayList<>(currentTablets.size());
      for (int i = 0; i < currentTablets.size(); i++) {
        currentAlignments.add(insertEvent.isAligned(i));
      }
      return;
    }

    final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) event;
    final Tablet tablet = rawEvent.convertToTablet();
    currentTablets = tablet == null ? Collections.emptyList() : Collections.singletonList(tablet);
    currentAlignments =
        tablet == null ? Collections.emptyList() : Collections.singletonList(rawEvent.isAligned());
  }

  private void cacheRawObjectValueContentTablet(final PipeRawTabletInsertionEvent rawEvent)
      throws IOException {
    final Tablet tablet = rawEvent.convertToTablet();
    cachedSerializedTablet =
        tablet == null
            ? null
            : PipeTransferTabletRawReqV2.toTPipeTransferReq(
                    tablet, rawEvent.isAligned(), rawEvent.getTableModelDatabaseName())
                .getBody();
    cachedPipeIdentity = currentPipeIdentity();
    currentTablets = Collections.emptyList();
    currentAlignments = Collections.emptyList();
  }

  private Pair<String, Long> currentPipeIdentity() {
    return new Pair<>(currentEvent.getPipeName(), currentEvent.getCreationTime());
  }

  @Override
  public void close() {
    closeCurrentTabletSplitIterator();
    cachedSerializedTablet = null;
    cachedPipeIdentity = null;
    currentTablets = Collections.emptyList();
    currentAlignments = Collections.emptyList();
    nextEventIndex = events.size();
  }

  private void closeCurrentTabletSplitIterator() {
    if (currentTabletSplitIterator != null) {
      currentTabletSplitIterator.close();
      currentTabletSplitIterator = null;
    }
  }
}
