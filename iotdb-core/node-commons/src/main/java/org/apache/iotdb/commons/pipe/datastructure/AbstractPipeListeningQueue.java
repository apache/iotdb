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

package org.apache.iotdb.commons.pipe.datastructure;

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
  private static final String SNAPSHOT_PREFIX = ".snapshot";
  private int referenceCount = 0;

  private final Pair<Long, List<PipeSnapshotEvent>> snapshotCache =
      new Pair<>(null, new ArrayList<>());

  protected AbstractPipeListeningQueue(LinkedQueueSerializerType serializerType) {
    super(serializerType);
  }

  public synchronized void increaseReferenceCount() {
    referenceCount++;
    if (referenceCount == 1) {
      open();
    }
  }

  public synchronized void decreaseReferenceCount() throws IOException {
    referenceCount--;
    if (referenceCount == 0) {
      close();
    }
  }

  /////////////////////////////// Snapshot Getter ///////////////////////////////

  // This method is thread-unsafe but snapshot must not be parallel with other
  // snapshots or write-plan.
  public void listenToSnapshots(List<PipeSnapshotEvent> events) {
    snapshotCache.setLeft(queue.getTailIndex());
    snapshotCache.setRight(events);
  }

  public Pair<Long, List<PipeSnapshotEvent>> findAvailableSnapshots() {
    // TODO: configure maximum number of events from snapshot to queue tail
    if (snapshotCache.getLeft() < queue.getTailIndex() - 1000) {
      snapshotCache.setLeft(null);
      snapshotCache.setRight(new ArrayList<>());
    }
    return snapshotCache;
  }

  /////////////////////////////// Snapshot ///////////////////////////////
  @Override
  public final boolean serializeToFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(snapshotName + SNAPSHOT_PREFIX);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(snapshotCache.getLeft(), fileOutputStream);
      ReadWriteIOUtils.write(snapshotCache.getRight().size(), fileOutputStream);
      for (PipeSnapshotEvent event : snapshotCache.getRight()) {
        ByteBuffer planBuffer = serializeToByteBuffer(event);
        ReadWriteIOUtils.write(planBuffer.capacity(), fileOutputStream);
        ReadWriteIOUtils.write(planBuffer, fileOutputStream);
      }
    }
    return super.serializeToFile(snapshotName);
  }

  @Override
  public final void deserializeFromFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(snapshotName + SNAPSHOT_PREFIX);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot, snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (final FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      try (FileChannel channel = inputStream.getChannel()) {
        snapshotCache.setLeft(ReadWriteIOUtils.readLong(inputStream));
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; ++i) {
          int capacity = ReadWriteIOUtils.readInt(inputStream);
          if (capacity == -1) {
            // EOF
            return;
          }
          ByteBuffer buffer = ByteBuffer.allocate(capacity);
          channel.read(buffer);
          snapshotCache.getRight().add((PipeSnapshotEvent) deserializeFromByteBuffer(buffer));
        }
      }
    }
    super.deserializeFromFile(snapshotName);
  }
}
