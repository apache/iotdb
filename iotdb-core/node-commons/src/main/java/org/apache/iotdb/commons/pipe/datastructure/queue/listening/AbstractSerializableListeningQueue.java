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

import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.datastructure.queue.serializer.PlainQueueSerializer;
import org.apache.iotdb.commons.pipe.datastructure.queue.serializer.QueueSerializer;
import org.apache.iotdb.commons.pipe.datastructure.queue.serializer.QueueSerializerType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * {@link AbstractSerializableListeningQueue} is the encapsulation of the {@link
 * ConcurrentIterableLinkedQueue} to enable flushing all the element to disk and reading from it. To
 * implement this, each element much be configured with its own ser/de method. Besides, this class
 * also provides a means of opening and closing the queue, and a queue will stay empty while closed.
 */
public abstract class AbstractSerializableListeningQueue<E> implements Closeable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSerializableListeningQueue.class);

  private final QueueSerializerType serializerType;
  private final EnumMap<QueueSerializerType, Supplier<QueueSerializer<E>>> serializers =
      new EnumMap<>(QueueSerializerType.class);

  protected final ConcurrentIterableLinkedQueue<E> queue = new ConcurrentIterableLinkedQueue<>();

  protected final AtomicBoolean isClosed = new AtomicBoolean(true);

  protected AbstractSerializableListeningQueue(final QueueSerializerType serializerType) {
    this.serializerType = serializerType;
    serializers.put(QueueSerializerType.PLAIN, PlainQueueSerializer::new);
  }

  /////////////////////////////// Function ///////////////////////////////

  protected synchronized boolean tryListen(final E element) {
    if (isClosed.get()) {
      return false;
    }
    queue.add(element);
    return true;
  }

  // Caller should ensure that the "newFirstIndex" is less than every iterators.
  public synchronized long removeBefore(final long newFirstIndex) {
    try (final ConcurrentIterableLinkedQueue<E>.DynamicIterator iterator =
        queue.iterateFromEarliest()) {
      while (iterator.getNextIndex() < newFirstIndex) {
        final E element = iterator.next(0);
        if (Objects.isNull(element)) {
          break;
        }
        releaseResource(element);
      }
    }
    return queue.tryRemoveBefore(newFirstIndex);
  }

  public synchronized boolean isGivenNextIndexValid(final long nextIndex) {
    // The "tailIndex" is permitted to listen to the next incoming element
    return queue.isNextIndexValid(nextIndex);
  }

  public synchronized ConcurrentIterableLinkedQueue<E>.DynamicIterator newIterator(
      final long nextIndex) {
    return queue.iterateFrom(nextIndex);
  }

  public synchronized void returnIterator(
      final ConcurrentIterableLinkedQueue<E>.DynamicIterator iterator) {
    iterator.close();
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public synchronized boolean serializeToFile(final File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.warn(
          "Failed to serialize to file, because file {} is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(isClosed.get(), fileOutputStream);
      ReadWriteIOUtils.write(serializerType.getType(), fileOutputStream);
      if (serializers.containsKey(serializerType)) {
        return serializers
            .get(serializerType)
            .get()
            .writeQueueToFile(fileOutputStream, queue, this::serializeToByteBuffer);
      } else {
        throw new UnsupportedOperationException(
            "Unknown serializer type: " + serializerType.getType());
      }
    }
  }

  public synchronized void deserializeFromFile(final File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.warn(
          "Failed to deserialize from file, file {} does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    queue.clear();
    try (final FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      isClosed.set(ReadWriteIOUtils.readBool(inputStream));
      final QueueSerializerType type =
          QueueSerializerType.deserialize(ReadWriteIOUtils.readByte(inputStream));
      if (serializers.containsKey(type)) {
        serializers
            .get(type)
            .get()
            .loadQueueFromFile(inputStream, queue, this::deserializeFromByteBuffer);
      } else {
        throw new UnsupportedOperationException("Unknown serializer type: " + type.getType());
      }
    }
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  protected abstract ByteBuffer serializeToByteBuffer(final E element);

  /**
   * Deserialize a single element from byteBuffer.
   *
   * @param byteBuffer the byteBuffer corresponding to an element
   * @return The deserialized element or {@code null} if a failure is encountered.
   */
  protected abstract E deserializeFromByteBuffer(final ByteBuffer byteBuffer);

  /////////////////////////////// Open & Close ///////////////////////////////

  public synchronized void open() {
    isClosed.set(false);
  }

  @Override
  public synchronized void close() {
    isClosed.set(true);

    try (final ConcurrentIterableLinkedQueue<E>.DynamicIterator iterator =
        queue.iterateFromEarliest()) {
      while (true) {
        final E element = iterator.next(0);
        if (Objects.isNull(element)) {
          break;
        }
        releaseResource(element);
      }
    }
    queue.clear();
  }

  protected abstract void releaseResource(final E element);

  public synchronized boolean isOpened() {
    return !isClosed.get();
  }

  /////////////////////////////// APIs provided for metric framework ///////////////////////////////

  public long getSize() {
    return queue.size();
  }

  public long getTailIndex() {
    return queue.getTailIndex();
  }
}
