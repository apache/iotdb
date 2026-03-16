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

  private final Map<String, Long> progressMap = new ConcurrentHashMap<>();

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

  public void updateProgress(final String key, final long committedSearchIndex) {
    progressMap.merge(key, committedSearchIndex, Math::max);
  }

  public Long getProgress(final String key) {
    return progressMap.get(key);
  }

  public Map<String, Long> getAllProgress() {
    return new HashMap<>(progressMap);
  }

  public void replaceAll(final Map<String, Long> newProgressMap) {
    progressMap.clear();
    for (final Map.Entry<String, Long> entry : newProgressMap.entrySet()) {
      progressMap.merge(entry.getKey(), entry.getValue(), Math::max);
    }
  }

  public boolean isEmpty() {
    return progressMap.isEmpty();
  }

  public void processTakeSnapshot(final FileOutputStream fileOutputStream) throws IOException {
    final int size = progressMap.size();
    fileOutputStream.write(ByteBuffer.allocate(4).putInt(size).array());
    for (final Map.Entry<String, Long> entry : progressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes("UTF-8");
      final ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + 8);
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);
      buffer.putLong(entry.getValue());
      fileOutputStream.write(buffer.array());
    }
  }

  public void processLoadSnapshot(final FileInputStream fileInputStream) throws IOException {
    progressMap.clear();
    final byte[] sizeBytes = new byte[4];
    if (fileInputStream.read(sizeBytes) != 4) {
      return;
    }
    final int size = ByteBuffer.wrap(sizeBytes).getInt();
    for (int i = 0; i < size; i++) {
      final byte[] keyLenBytes = new byte[4];
      if (fileInputStream.read(keyLenBytes) != 4) {
        throw new IOException("Unexpected EOF reading commit progress key length");
      }
      final int keyLen = ByteBuffer.wrap(keyLenBytes).getInt();
      final byte[] keyBytes = new byte[keyLen];
      if (fileInputStream.read(keyBytes) != keyLen) {
        throw new IOException("Unexpected EOF reading commit progress key");
      }
      final String key = new String(keyBytes, "UTF-8");
      final byte[] valueBytes = new byte[8];
      if (fileInputStream.read(valueBytes) != 8) {
        throw new IOException("Unexpected EOF reading commit progress value");
      }
      final long value = ByteBuffer.wrap(valueBytes).getLong();
      progressMap.put(key, value);
    }
  }

  public void serializeToStream(final java.io.DataOutputStream stream) throws IOException {
    stream.writeInt(progressMap.size());
    for (final Map.Entry<String, Long> entry : progressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes("UTF-8");
      stream.writeInt(keyBytes.length);
      stream.write(keyBytes);
      stream.writeLong(entry.getValue());
    }
  }

  public static Map<String, Long> deserializeFromBuffer(final ByteBuffer buffer) {
    final int size = buffer.getInt();
    final Map<String, Long> result = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      final int keyLen = buffer.getInt();
      final byte[] keyBytes = new byte[keyLen];
      buffer.get(keyBytes);
      final String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
      final long value = buffer.getLong();
      result.put(key, value);
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommitProgressKeeper that = (CommitProgressKeeper) o;
    return Objects.equals(this.progressMap, that.progressMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(progressMap);
  }
}
