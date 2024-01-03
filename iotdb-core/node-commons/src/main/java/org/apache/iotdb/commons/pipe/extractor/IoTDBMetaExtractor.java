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
import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class IoTDBMetaExtractor extends IoTDBCommonExtractor {
  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr;
  private List<PipeSnapshotEvent> historicalEvents = new ArrayList<>();

  protected abstract AbstractPipeListeningQueue getListeningQueue();

  @Override
  public void start() throws Exception {
    super.start();
    ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    long index;
    if (progressIndex instanceof MinimumProgressIndex) {
      // TODO: Trigger snapshot if not exists
      Pair<Long, List<PipeSnapshotEvent>> eventPair = getListeningQueue().findAvailableSnapshots();
      index = !Objects.isNull(eventPair.getLeft()) ? eventPair.getLeft() + 1 : 0;
      historicalEvents = eventPair.getRight();
    } else {
      index = ((MetaProgressIndex) progressIndex).getIndex();
    }
    itr = getListeningQueue().newIterator(index);
  }

  @Override
  public EnrichedEvent supply() throws Exception {
    if (!historicalEvents.isEmpty()) {
      if (historicalEvents.size() != 1) {
        // Do not report progress for non-final snapshot events since we re-transmit all snapshot
        // files currently
        return historicalEvents.remove(0);
      } else {
        PipeSnapshotEvent event =
            (PipeSnapshotEvent)
                historicalEvents
                    .remove(0)
                    .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                        pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
        event.bindProgressIndex(new MetaProgressIndex(itr.getNextIndex() - 1));
        return event;
      }
    }

    EnrichedEvent event;
    do {
      // Return immediately
      event = (EnrichedEvent) itr.next(0);
    } while (!Objects.isNull(event)
        && (!isListenType(event) || !isForwardingPipeRequests && event.isGeneratedByPipe()));

    if (Objects.isNull(event)) {
      return null;
    }

    EnrichedEvent targetEvent =
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
    targetEvent.bindProgressIndex(new MetaProgressIndex(itr.getNextIndex() - 1));
    targetEvent.increaseReferenceCount(IoTDBMetaExtractor.class.getName());
    return targetEvent;
  }

  protected abstract boolean isListenType(Event event);

  @Override
  public void close() throws Exception {
    if (!hasBeenStarted.get()) {
      return;
    }
    getListeningQueue().returnIterator(itr);
  }
}
