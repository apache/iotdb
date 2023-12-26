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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class AbstractSerializableListeningQueue<E> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSerializableListeningQueue.class);

  private final LinkedQueueSerializerType currentType;

  private final Map<LinkedQueueSerializerType, Supplier<QueueSerializer<E>>> serializerMap =
      new HashMap<>();

  protected final ConcurrentIterableLinkedQueue<E> queue = new ConcurrentIterableLinkedQueue<>();

  /////////////////////////////// Function ///////////////////////////////

  protected AbstractSerializableListeningQueue(LinkedQueueSerializerType serializerType) {
    currentType = serializerType;
    serializerMap.put(LinkedQueueSerializerType.PLAIN, PlainQueueSerializer::new);
  }

  /////////////////////////////// Function ///////////////////////////////

  public void listenToElement(E plan) {
    queue.add(plan);
  }

  public ConcurrentIterableLinkedQueue<E>.DynamicIterator newIterator(int index) {
    return queue.iterateFrom(index);
  }

  public void returnIterator(ConcurrentIterableLinkedQueue<E>.DynamicIterator itr) {
    itr.close();
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public final boolean serializeToFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(currentType.getType(), fileOutputStream);
      return serializerMap
          .get(currentType)
          .get()
          .writeQueueToFile(fileOutputStream, queue, this::serializeToByteBuffer);
    }
  }

  public final void deserializeFromFile(File snapshotName) throws IOException {
    final File snapshotFile = new File(String.valueOf(snapshotName));
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot, snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (final FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      final LinkedQueueSerializerType type =
          LinkedQueueSerializerType.deserialize(ReadWriteIOUtils.readByte(inputStream));
      if (serializerMap.containsKey(type)) {
        serializerMap
            .get(type)
            .get()
            .loadQueueFromFile(inputStream, queue, this::deserializeFromByteBuffer);
      } else {
        throw new UnsupportedOperationException("Unknown listening queue type: " + type.getType());
      }
    }
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  protected abstract ByteBuffer serializeToByteBuffer(E element);

  /**
   * Deserialize a single element from byteBuffer.
   *
   * @param byteBuffer the byteBuffer corresponding to an element
   * @return The deserialized element or null if a failure is encountered.
   */
  protected abstract E deserializeFromByteBuffer(ByteBuffer byteBuffer);
}
