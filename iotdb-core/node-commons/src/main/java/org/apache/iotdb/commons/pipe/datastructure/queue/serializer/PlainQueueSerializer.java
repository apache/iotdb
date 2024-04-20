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

package org.apache.iotdb.commons.pipe.datastructure.queue.serializer;

import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.function.Function;

public class PlainQueueSerializer<E> implements QueueSerializer<E> {
  @Override
  public boolean writeQueueToFile(
      FileOutputStream fileOutputStream,
      ConcurrentIterableLinkedQueue<E> queue,
      Function<E, ByteBuffer> elementSerializationFunction)
      throws IOException {
    ReadWriteIOUtils.write(queue.getFirstIndex(), fileOutputStream);
    try (ConcurrentIterableLinkedQueue<E>.DynamicIterator itr = queue.iterateFromEarliest()) {
      E element;
      while (true) {
        element = itr.next(0);
        if (Objects.isNull(element)) {
          break;
        }
        ByteBuffer planBuffer = elementSerializationFunction.apply(element);
        ReadWriteIOUtils.write(planBuffer, fileOutputStream);
      }
    }
    fileOutputStream.getFD().sync();
    return true;
  }

  @Override
  public void loadQueueFromFile(
      FileInputStream inputStream,
      ConcurrentIterableLinkedQueue<E> queue,
      Function<ByteBuffer, E> elementDeserializationFunction)
      throws IOException {
    try (FileChannel channel = inputStream.getChannel()) {
      queue.setFirstIndex(ReadWriteIOUtils.readLong(inputStream));
      while (true) {
        if (inputStream.available() == 0) {
          return;
        }
        int capacity = ReadWriteIOUtils.readInt(inputStream);
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        channel.read(buffer);
        buffer.flip();
        E element = elementDeserializationFunction.apply(buffer);
        if (element == null) {
          throw new IOException("Failed to load snapshot.");
        }
        queue.add(element);
      }
    }
  }
}
