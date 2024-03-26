/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.extractor;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public abstract class IoTDBNonDataRegionExtractor extends IoTDBExtractor {

  private List<PipeSnapshotEvent> historicalEvents = new LinkedList<>();

  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator iterator;

  protected abstract AbstractPipeListeningQueue getListeningQueue();

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    super.start();

    final ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    final long nextIndex =
        progressIndex instanceof MinimumProgressIndex
                // If the index is invalid, the queue is seen as cleared before and thus
                // needs snapshot re-transferring
                || !getListeningQueue()
                    .isGivenNextIndexValid(((MetaProgressIndex) progressIndex).getIndex() + 1)
            ? getNextIndexAfterSnapshot()
            : ((MetaProgressIndex) progressIndex).getIndex() + 1;
    iterator = getListeningQueue().newIterator(nextIndex);
  }

  private long getNextIndexAfterSnapshot() {
    final Pair<Long, List<PipeSnapshotEvent>> queueTailIndex2Snapshots =
        getListeningQueue().findAvailableSnapshots();
    // TODO: Trigger snapshot if not exists
    final long nextIndex =
        Objects.nonNull(queueTailIndex2Snapshots.getLeft())
                && queueTailIndex2Snapshots.getLeft() != Long.MIN_VALUE
            ? queueTailIndex2Snapshots.getLeft() + 1
            : 0;
    historicalEvents = new LinkedList<>(queueTailIndex2Snapshots.getRight());
    return nextIndex;
  }

  @Override
  public EnrichedEvent supply() throws Exception {
    // historical
    if (!historicalEvents.isEmpty()) {
      final PipeSnapshotEvent historicalEvent =
          (PipeSnapshotEvent)
              historicalEvents
                  .remove(0)
                  .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);

      if (historicalEvents.isEmpty()) {
        // We only report progress for the last snapshot event.
        historicalEvent.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
      }

      historicalEvent.increaseReferenceCount(IoTDBNonDataRegionExtractor.class.getName());
      return historicalEvent;
    }

    // realtime
    EnrichedEvent realtimeEvent;
    do {
      realtimeEvent = (EnrichedEvent) iterator.next(0);
      if (Objects.isNull(realtimeEvent)) {
        return null;
      }
    } while (!isTypeListened(realtimeEvent)
        || (!isForwardingPipeRequests && realtimeEvent.isGeneratedByPipe()));

    realtimeEvent =
        realtimeEvent.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
    realtimeEvent.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
    realtimeEvent.increaseReferenceCount(IoTDBNonDataRegionExtractor.class.getName());
    return realtimeEvent;
  }

  protected abstract boolean isTypeListened(Event event);

  @Override
  public void close() throws Exception {
    if (!hasBeenStarted.get()) {
      return;
    }

    getListeningQueue().returnIterator(iterator);
    historicalEvents.clear();
  }
}
