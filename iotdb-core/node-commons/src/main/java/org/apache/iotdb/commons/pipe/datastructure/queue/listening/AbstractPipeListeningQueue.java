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

package org.apache.iotdb.commons.pipe.datastructure.queue.listening;

import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.queue.serializer.QueueSerializerType;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractPipeListeningQueue} is the encapsulation of the {@link
 * AbstractSerializableListeningQueue} to enable using reference count to control opening and
 * closing. This class also enables a {@link PipeTask} to find the snapshots close enough to send if
 * existed.
 */
public abstract class AbstractPipeListeningQueue extends AbstractSerializableListeningQueue<Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipeListeningQueue.class);

  // The cache of the snapshot events list and the tail index of the queue
  // when the snapshot events are generated.
  // The snapshot path won't be taken snapshot itself and will be re-assigned when
  // the node has been set up
  private final Pair<Long, List<PipeSnapshotEvent>> queueTailIndex2SnapshotsCache =
      new Pair<>(Long.MIN_VALUE, new ArrayList<>());

  protected AbstractPipeListeningQueue() {
    super(QueueSerializerType.PLAIN);
  }

  /////////////////////////////// Plan ///////////////////////////////

  protected synchronized void tryListen(final EnrichedEvent event) {
    if (super.tryListen(event)) {
      event.increaseReferenceCount(AbstractPipeListeningQueue.class.getName());
    }
  }

  /////////////////////////////// Snapshot Cache ///////////////////////////////

  protected synchronized void tryListen(final List<PipeSnapshotEvent> events) {
    if (!isClosed.get()) {
      clearSnapshots();
      queueTailIndex2SnapshotsCache.setLeft(queue.getTailIndex());
      events.forEach(
          event -> event.increaseReferenceCount(AbstractPipeListeningQueue.class.getName()));
      queueTailIndex2SnapshotsCache.setRight(events);
      LOGGER.info(
          "Pipe listening queue snapshot cache is updated: {}", queueTailIndex2SnapshotsCache);
    }
  }

  public synchronized Pair<Long, List<PipeSnapshotEvent>> findAvailableSnapshots() {
    if (queueTailIndex2SnapshotsCache.getLeft()
        < queue.getTailIndex()
            - PipeConfig.getInstance().getPipeListeningQueueTransferSnapshotThreshold()) {
      clearSnapshots();
    }
    return queueTailIndex2SnapshotsCache;
  }

  @Override
  public synchronized long removeBefore(final long newFirstIndex) {
    final long result = super.removeBefore(newFirstIndex);
    if (queueTailIndex2SnapshotsCache.getLeft() < result) {
      clearSnapshots();
    }
    return result;
  }

  private synchronized void clearSnapshots() {
    queueTailIndex2SnapshotsCache.setLeft(Long.MIN_VALUE);
    queueTailIndex2SnapshotsCache
        .getRight()
        .forEach(
            event ->
                event.decreaseReferenceCount(AbstractPipeListeningQueue.class.getName(), false));
    queueTailIndex2SnapshotsCache.setRight(new ArrayList<>());
  }

  /////////////////////////////// Close ///////////////////////////////

  @Override
  public synchronized void close() {
    clearSnapshots();
    super.close();
  }

  @Override
  protected void releaseResource(final Event event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event)
          .decreaseReferenceCount(AbstractPipeListeningQueue.class.getName(), false);
    }
  }
}
