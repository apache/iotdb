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

import org.apache.iotdb.commons.pipe.datastructure.serializer.PlainQueueSerializer;
import org.apache.iotdb.commons.pipe.datastructure.serializer.QueueSerializer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

  private final LinkedQueueSerializerType currentType;

  private final EnumMap<LinkedQueueSerializerType, Supplier<QueueSerializer<E>>> serializerMap =
      new EnumMap<>(LinkedQueueSerializerType.class);

  protected final ConcurrentIterableLinkedQueue<E> queue = new ConcurrentIterableLinkedQueue<>();

  protected final AtomicBoolean isSealed = new AtomicBoolean();

  protected AbstractSerializableListeningQueue(LinkedQueueSerializerType serializerType) {
    currentType = serializerType;
    // Always seal initially unless manually open it
    isSealed.set(true);
    serializerMap.put(LinkedQueueSerializerType.PLAIN, PlainQueueSerializer::new);
  }

  /////////////////////////////// Function ///////////////////////////////

  public boolean tryListenToElement(E element) {
    if (isSealed.get()) {
      return false;
    }
    queue.add(element);
    return true;
  }

  public ConcurrentIterableLinkedQueue<E>.DynamicIterator newIterator(long index) {
    return queue.iterateFrom(index);
  }

  public void returnIterator(ConcurrentIterableLinkedQueue<E>.DynamicIterator itr) {
    itr.close();
  }

  // Caller should ensure that the "newFirstIndex" is less than every iterators.
  public long removeBefore(long newFirstIndex) {
    try (ConcurrentIterableLinkedQueue<E>.DynamicIterator itr = queue.iterateFromEarliest()) {
      while (itr.getNextIndex() < newFirstIndex) {
        E element = itr.next(0);
        if (Objects.isNull(element)) {
          break;
        }
        releaseResource(element);
      }
    }
    return queue.tryRemoveBefore(newFirstIndex);
  }

  public boolean isValidIndex(long index) {
    // The "tailIndex" is permitted to listen to the next incoming element
    return queue.getFirstIndex() <= index && queue.getTailIndex() >= index;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public boolean serializeToFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to serialize to file, because file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(isSealed.get(), fileOutputStream);
      ReadWriteIOUtils.write(currentType.getType(), fileOutputStream);
      return serializerMap
          .get(currentType)
          .get()
          .writeQueueToFile(fileOutputStream, queue, this::serializeToByteBuffer);
    }
  }

  public void deserializeFromFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to deserialize from file, file [{}] does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    queue.clear();
    try (final FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      isSealed.set(ReadWriteIOUtils.readBool(inputStream));
      final LinkedQueueSerializerType type =
          LinkedQueueSerializerType.deserialize(ReadWriteIOUtils.readByte(inputStream));
      if (serializerMap.containsKey(type)) {
        serializerMap
            .get(type)
            .get()
            .loadQueueFromFile(inputStream, queue, this::deserializeFromByteBuffer);
      } else {
        throw new UnsupportedOperationException("Unknown serializer type: " + type.getType());
      }
    }
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  protected abstract ByteBuffer serializeToByteBuffer(E element);

  /**
   * Deserialize a single element from byteBuffer.
   *
   * @param byteBuffer the byteBuffer corresponding to an element
   * @return The deserialized element or {@code null} if a failure is encountered.
   */
  protected abstract E deserializeFromByteBuffer(ByteBuffer byteBuffer);

  /////////////////////////////// Open & Close ///////////////////////////////

  public synchronized void open() {
    isSealed.set(false);
  }

  @Override
  public synchronized void close() throws IOException {
    isSealed.set(true);
    try (ConcurrentIterableLinkedQueue<E>.DynamicIterator itr = queue.iterateFromEarliest()) {
      while (true) {
        E element = itr.next(0);
        if (Objects.isNull(element)) {
          break;
        }
        releaseResource(element);
      }
    }
    queue.clear();
  }

  protected abstract void releaseResource(E element);

  public boolean isOpened() {
    return !isSealed.get();
  }
}
