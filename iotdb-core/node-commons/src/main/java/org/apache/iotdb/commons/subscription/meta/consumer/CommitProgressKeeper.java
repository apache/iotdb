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

package org.apache.iotdb.commons.subscription.meta.consumer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CommitProgressKeeper {

  private static final String KEY_SEPARATOR = "##";

  private final Map<String, ByteBuffer> regionProgressMap = new ConcurrentHashMap<>();

  public CommitProgressKeeper() {}

  public static String generateKey(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final int dataNodeId) {
    return consumerGroupId
        + KEY_SEPARATOR
        + topicName
        + KEY_SEPARATOR
        + regionId
        + KEY_SEPARATOR
        + dataNodeId;
  }

  public void updateRegionProgress(final String key, final ByteBuffer committedRegionProgress) {
    if (Objects.isNull(committedRegionProgress)) {
      return;
    }
    regionProgressMap.put(key, copyBuffer(committedRegionProgress));
  }

  public ByteBuffer getRegionProgress(final String key) {
    final ByteBuffer buffer = regionProgressMap.get(key);
    return Objects.nonNull(buffer) ? copyBuffer(buffer) : null;
  }

  public Map<String, ByteBuffer> getAllRegionProgress() {
    final Map<String, ByteBuffer> result = new HashMap<>(regionProgressMap.size());
    regionProgressMap.forEach((key, value) -> result.put(key, copyBuffer(value)));
    return result;
  }

  public void replaceAll(final Map<String, ByteBuffer> newRegionProgressMap) {
    regionProgressMap.clear();
    if (Objects.nonNull(newRegionProgressMap)) {
      for (final Map.Entry<String, ByteBuffer> entry : newRegionProgressMap.entrySet()) {
        if (Objects.nonNull(entry.getValue())) {
          regionProgressMap.put(entry.getKey(), copyBuffer(entry.getValue()));
        }
      }
    }
  }

  public boolean isEmpty() {
    return regionProgressMap.isEmpty();
  }

  public void processTakeSnapshot(final FileOutputStream fileOutputStream) throws IOException {
    final int regionSize = regionProgressMap.size();
    fileOutputStream.write(ByteBuffer.allocate(4).putInt(regionSize).array());
    for (final Map.Entry<String, ByteBuffer> entry : regionProgressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes("UTF-8");
      final ByteBuffer progressBuffer = copyBuffer(entry.getValue());
      final byte[] progressBytes = new byte[progressBuffer.remaining()];
      progressBuffer.get(progressBytes);
      final ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + progressBytes.length);
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);
      buffer.putInt(progressBytes.length);
      buffer.put(progressBytes);
      fileOutputStream.write(buffer.array());
    }
  }

  public void processLoadSnapshot(final FileInputStream fileInputStream) throws IOException {
    regionProgressMap.clear();
    final byte[] sizeBytes = new byte[4];
    if (fileInputStream.read(sizeBytes) != 4) {
      return;
    }
    final int regionSize = ByteBuffer.wrap(sizeBytes).getInt();
    for (int i = 0; i < regionSize; i++) {
      final byte[] keyLenBytes = new byte[4];
      if (fileInputStream.read(keyLenBytes) != 4) {
        throw new IOException("Unexpected EOF reading region progress key length");
      }
      final int keyLen = ByteBuffer.wrap(keyLenBytes).getInt();
      final byte[] keyBytes = new byte[keyLen];
      if (fileInputStream.read(keyBytes) != keyLen) {
        throw new IOException("Unexpected EOF reading region progress key");
      }
      final String key = new String(keyBytes, "UTF-8");
      final byte[] valueLenBytes = new byte[4];
      if (fileInputStream.read(valueLenBytes) != 4) {
        throw new IOException("Unexpected EOF reading region progress value length");
      }
      final int valueLen = ByteBuffer.wrap(valueLenBytes).getInt();
      final byte[] valueBytes = new byte[valueLen];
      if (fileInputStream.read(valueBytes) != valueLen) {
        throw new IOException("Unexpected EOF reading region progress value");
      }
      regionProgressMap.put(key, ByteBuffer.wrap(valueBytes).asReadOnlyBuffer());
    }
  }

  public void serializeToStream(final java.io.DataOutputStream stream) throws IOException {
    stream.writeInt(regionProgressMap.size());
    for (final Map.Entry<String, ByteBuffer> entry : regionProgressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes("UTF-8");
      final ByteBuffer progressBuffer = copyBuffer(entry.getValue());
      final byte[] progressBytes = new byte[progressBuffer.remaining()];
      progressBuffer.get(progressBytes);
      stream.writeInt(keyBytes.length);
      stream.write(keyBytes);
      stream.writeInt(progressBytes.length);
      stream.write(progressBytes);
    }
  }

  public static Map<String, ByteBuffer> deserializeRegionProgressFromBuffer(
      final ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return new HashMap<>();
    }
    final int size = buffer.getInt();
    final Map<String, ByteBuffer> result = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      final int keyLen = buffer.getInt();
      final byte[] keyBytes = new byte[keyLen];
      buffer.get(keyBytes);
      final String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
      final int valueLen = buffer.getInt();
      final byte[] valueBytes = new byte[valueLen];
      buffer.get(valueBytes);
      result.put(key, ByteBuffer.wrap(valueBytes).asReadOnlyBuffer());
    }
    return result;
  }

  private static ByteBuffer copyBuffer(final ByteBuffer buffer) {
    final ByteBuffer duplicate = buffer.asReadOnlyBuffer();
    duplicate.rewind();
    final byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CommitProgressKeeper that = (CommitProgressKeeper) o;
    return Objects.equals(this.regionProgressMap, that.regionProgressMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionProgressMap);
  }
}
