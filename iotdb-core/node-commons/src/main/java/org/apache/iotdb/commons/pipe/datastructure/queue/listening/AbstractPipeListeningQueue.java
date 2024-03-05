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

import org.apache.iotdb.commons.pipe.datastructure.queue.serializer.QueueSerializerType;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

  private static final String SNAPSHOT_SUFFIX = ".snapshot";

  // The cache of the snapshot events list and the tail index of the queue
  // when the snapshot events are generated.
  private final Pair<Long, List<PipeSnapshotEvent>> queueTailIndex2SnapshotsCache =
      new Pair<>(Long.MIN_VALUE, new ArrayList<>());

  private volatile boolean leaderReady = false;

  protected AbstractPipeListeningQueue() {
    super(QueueSerializerType.PLAIN);
  }

  /////////////////////////////// Function ///////////////////////////////

  protected synchronized void tryListen(EnrichedEvent event) {
    if (super.tryListen(event)) {
      event.increaseReferenceCount(AbstractPipeListeningQueue.class.getName());
    }
  }

  /////////////////////////////// Leader ready ///////////////////////////////
  // TODO: remove

  /**
   * Get leader ready state, DO NOT use consensus layer's leader ready flag because SimpleConsensus'
   * ready flag is always {@code true}. Note that this flag has nothing to do with listening and a
   * {@link PipeTask} starts only iff the current node is a leader and ready.
   *
   * @return {@code true} iff the current node is a leader and ready
   */
  public boolean isLeaderReady() {
    return leaderReady;
  }

  // Leader ready flag has the following effect
  // 1. The linked list starts serving only after leader gets ready
  // 2. Config pipe task is only created after leader gets ready
  public void notifyLeaderReady() {
    leaderReady = true;
  }

  public void notifyLeaderUnavailable() {
    leaderReady = false;
  }

  /////////////////////////////// Snapshot Cache ///////////////////////////////

  protected synchronized void listenToSnapshots(List<PipeSnapshotEvent> events) {
    if (!isClosed.get()) {
      clearSnapshots();
      queueTailIndex2SnapshotsCache.setLeft(queue.getTailIndex());
      events.forEach(
          event -> event.increaseReferenceCount(AbstractPipeListeningQueue.class.getName()));
      queueTailIndex2SnapshotsCache.setRight(events);
    }
  }

  public synchronized Pair<Long, List<PipeSnapshotEvent>> findAvailableSnapshots() {
    // TODO: configure maximum number of events from snapshot to queue tail
    if (queueTailIndex2SnapshotsCache.getLeft() < queue.getTailIndex() - 1000) {
      clearSnapshots();
    }
    return queueTailIndex2SnapshotsCache;
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

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public final synchronized boolean serializeToFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(snapshotName + SNAPSHOT_SUFFIX);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file {} is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(queueTailIndex2SnapshotsCache.getLeft(), fileOutputStream);
      ReadWriteIOUtils.write(queueTailIndex2SnapshotsCache.getRight().size(), fileOutputStream);
      for (PipeSnapshotEvent event : queueTailIndex2SnapshotsCache.getRight()) {
        ByteBuffer planBuffer = serializeToByteBuffer(event);
        ReadWriteIOUtils.write(planBuffer, fileOutputStream);
      }
    }

    return super.serializeToFile(snapshotName);
  }

  @Override
  public final synchronized void deserializeFromFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(snapshotName + SNAPSHOT_SUFFIX);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot, snapshot file {} is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (final FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      try (FileChannel channel = inputStream.getChannel()) {
        queueTailIndex2SnapshotsCache.setLeft(ReadWriteIOUtils.readLong(inputStream));
        queueTailIndex2SnapshotsCache.setRight(new ArrayList<>());
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; ++i) {
          int capacity = ReadWriteIOUtils.readInt(inputStream);
          if (capacity == -1) {
            // EOF
            return;
          }
          ByteBuffer buffer = ByteBuffer.allocate(capacity);
          channel.read(buffer);
          queueTailIndex2SnapshotsCache
              .getRight()
              .add((PipeSnapshotEvent) deserializeFromByteBuffer(buffer));
        }
      }
    }

    super.deserializeFromFile(snapshotName);
  }

  /////////////////////////////// Close ///////////////////////////////

  @Override
  public synchronized void close() throws IOException {
    clearSnapshots();
    super.close();
  }

  @Override
  protected void releaseResource(Event event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event)
          .decreaseReferenceCount(AbstractPipeListeningQueue.class.getName(), false);
    }
  }
}
