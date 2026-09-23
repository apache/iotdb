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

import org.apache.iotdb.commons.i18n.PipeMessages;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CommitProgressKeeper {

  private static final String KEY_SEPARATOR = "##";
  private static final String KEY_COMPONENT_SEPARATOR = ".";
  private static final String VERSIONED_KEY_PREFIX = "v2:";

  private final Map<String, ByteBuffer> regionProgressMap = new ConcurrentHashMap<>();

  public CommitProgressKeeper() {}

  public static String generateRegionKey(
      final String consumerGroupId, final String topicName, final String regionId) {
    return VERSIONED_KEY_PREFIX
        + encodeKeyComponent(consumerGroupId)
        + KEY_COMPONENT_SEPARATOR
        + encodeKeyComponent(topicName)
        + KEY_COMPONENT_SEPARATOR
        + encodeKeyComponent(regionId);
  }

  public static String generateRegionKeyPrefix(
      final String consumerGroupId, final String topicName, final String regionId) {
    return generateRegionKey(consumerGroupId, topicName, regionId) + KEY_SEPARATOR;
  }

  public static String generateKey(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final int dataNodeId) {
    return generateRegionKeyPrefix(consumerGroupId, topicName, regionId) + dataNodeId;
  }

  public static String generateLegacyRegionKeyPrefix(
      final String consumerGroupId, final String topicName, final String regionId) {
    return consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR + regionId + KEY_SEPARATOR;
  }

  public static String generateLegacyKey(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final int dataNodeId) {
    return generateLegacyRegionKeyPrefix(consumerGroupId, topicName, regionId) + dataNodeId;
  }

  public static boolean isLegacyKeyUnambiguous(
      final String consumerGroupId, final String topicName, final String regionId) {
    return !String.valueOf(consumerGroupId).contains(KEY_SEPARATOR)
        && !String.valueOf(topicName).contains(KEY_SEPARATOR)
        && !String.valueOf(regionId).contains(KEY_SEPARATOR);
  }

  public static boolean isValidDataNodeProgressKey(final String key, final String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return false;
    }
    return isCanonicalDataNodeId(key.substring(keyPrefix.length()));
  }

  public synchronized void removeTopicProgress(
      final String consumerGroupId, final String topicName) {
    final String versionedTopicKeyPrefix =
        VERSIONED_KEY_PREFIX
            + encodeKeyComponent(consumerGroupId)
            + KEY_COMPONENT_SEPARATOR
            + encodeKeyComponent(topicName)
            + KEY_COMPONENT_SEPARATOR;
    final String legacyTopicKeyPrefix =
        String.valueOf(consumerGroupId) + KEY_SEPARATOR + String.valueOf(topicName) + KEY_SEPARATOR;
    final boolean legacyTopicKeyIsUnambiguous =
        isLegacyKeyUnambiguous(consumerGroupId, topicName, "");
    regionProgressMap
        .keySet()
        .removeIf(
            key ->
                isValidRegionAndDataNodeProgressKey(key, versionedTopicKeyPrefix)
                    || (legacyTopicKeyIsUnambiguous
                        && isValidRegionAndDataNodeProgressKey(key, legacyTopicKeyPrefix)));
  }

  private static boolean isValidRegionAndDataNodeProgressKey(
      final String key, final String topicKeyPrefix) {
    if (!key.startsWith(topicKeyPrefix)) {
      return false;
    }
    final String suffix = key.substring(topicKeyPrefix.length());
    final int separatorIndex = suffix.indexOf(KEY_SEPARATOR);
    return separatorIndex > 0
        && separatorIndex == suffix.lastIndexOf(KEY_SEPARATOR)
        && isCanonicalDataNodeId(suffix.substring(separatorIndex + KEY_SEPARATOR.length()));
  }

  private static boolean isCanonicalDataNodeId(final String value) {
    try {
      final int dataNodeId = Integer.parseInt(value);
      return dataNodeId >= 0 && Integer.toString(dataNodeId).equals(value);
    } catch (final NumberFormatException e) {
      return false;
    }
  }

  private static String encodeKeyComponent(final String component) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(String.valueOf(component).getBytes(StandardCharsets.UTF_8));
  }

  public synchronized void updateRegionProgress(
      final String key, final ByteBuffer committedRegionProgress) {
    if (Objects.isNull(committedRegionProgress)) {
      return;
    }
    regionProgressMap.put(key, copyBuffer(committedRegionProgress));
  }

  public synchronized ByteBuffer getRegionProgress(final String key) {
    final ByteBuffer buffer = regionProgressMap.get(key);
    return Objects.nonNull(buffer) ? copyBuffer(buffer) : null;
  }

  public synchronized Map<String, ByteBuffer> getAllRegionProgress() {
    final Map<String, ByteBuffer> result = new HashMap<>(regionProgressMap.size());
    regionProgressMap.forEach((key, value) -> result.put(key, copyBuffer(value)));
    return result;
  }

  public synchronized void replaceAll(final Map<String, ByteBuffer> newRegionProgressMap) {
    regionProgressMap.clear();
    if (Objects.nonNull(newRegionProgressMap)) {
      for (final Map.Entry<String, ByteBuffer> entry : newRegionProgressMap.entrySet()) {
        if (Objects.nonNull(entry.getValue())) {
          regionProgressMap.put(entry.getKey(), copyBuffer(entry.getValue()));
        }
      }
    }
  }

  public synchronized boolean isEmpty() {
    return regionProgressMap.isEmpty();
  }

  public synchronized void processTakeSnapshot(final FileOutputStream fileOutputStream)
      throws IOException {
    serializeRegionProgressMapToStream(regionProgressMap, new DataOutputStream(fileOutputStream));
  }

  public synchronized void processLoadSnapshot(final FileInputStream fileInputStream)
      throws IOException {
    regionProgressMap.clear();
    final DataInputStream dataInputStream = new DataInputStream(fileInputStream);
    final byte[] sizeBytes = new byte[4];
    final int firstSizeByte = dataInputStream.read();
    if (firstSizeByte < 0) {
      return;
    }
    sizeBytes[0] = (byte) firstSizeByte;
    dataInputStream.readFully(sizeBytes, 1, sizeBytes.length - 1);
    final int regionSize = ByteBuffer.wrap(sizeBytes).getInt();
    validateLength(
        regionSize,
        dataInputStream.available() / (2 * Integer.BYTES),
        PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_ENTRY_COUNT_B43DED2F);
    for (int i = 0; i < regionSize; i++) {
      final byte[] keyLenBytes = new byte[4];
      if (!readFully(dataInputStream, keyLenBytes)) {
        throw new IOException(
            PipeMessages.EXCEPTION_UNEXPECTED_EOF_READING_REGION_PROGRESS_KEY_LENGTH_EBC10484);
      }
      final int keyLen = ByteBuffer.wrap(keyLenBytes).getInt();
      final int remainingEntryCount = regionSize - i - 1;
      validateLength(
          keyLen,
          dataInputStream.available() - Integer.BYTES - remainingEntryCount * 2 * Integer.BYTES,
          PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_KEY_LENGTH_7C3A3C98);
      final byte[] keyBytes = new byte[keyLen];
      if (!readFully(dataInputStream, keyBytes)) {
        throw new IOException(
            PipeMessages.EXCEPTION_UNEXPECTED_EOF_READING_REGION_PROGRESS_KEY_C1532EAE);
      }
      final String key = new String(keyBytes, StandardCharsets.UTF_8);
      final byte[] valueLenBytes = new byte[4];
      if (!readFully(dataInputStream, valueLenBytes)) {
        throw new IOException(
            PipeMessages.EXCEPTION_UNEXPECTED_EOF_READING_REGION_PROGRESS_VALUE_LENGTH_D95F9CE0);
      }
      final int valueLen = ByteBuffer.wrap(valueLenBytes).getInt();
      validateLength(
          valueLen,
          dataInputStream.available() - remainingEntryCount * 2 * Integer.BYTES,
          PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_VALUE_LENGTH_6192D17F);
      final byte[] valueBytes = new byte[valueLen];
      if (!readFully(dataInputStream, valueBytes)) {
        throw new IOException(
            PipeMessages.EXCEPTION_UNEXPECTED_EOF_READING_REGION_PROGRESS_VALUE_A459C521);
      }
      regionProgressMap.put(key, ByteBuffer.wrap(valueBytes).asReadOnlyBuffer());
    }
  }

  private static void validateLength(final int value, final int maximum, final String message)
      throws IOException {
    if (value < 0 || value > maximum) {
      throw new IOException(String.format(message, value));
    }
  }

  private static boolean readFully(final DataInputStream stream, final byte[] bytes)
      throws IOException {
    try {
      stream.readFully(bytes);
      return true;
    } catch (final EOFException ignored) {
      return false;
    }
  }

  public synchronized void serializeToStream(final DataOutputStream stream) throws IOException {
    serializeRegionProgressMapToStream(regionProgressMap, stream);
  }

  public static void serializeRegionProgressMapToStream(
      final Map<String, ByteBuffer> regionProgressMap, final DataOutputStream stream)
      throws IOException {
    stream.writeInt(Objects.nonNull(regionProgressMap) ? regionProgressMap.size() : 0);
    if (Objects.isNull(regionProgressMap)) {
      return;
    }
    for (final Map.Entry<String, ByteBuffer> entry : regionProgressMap.entrySet()) {
      final byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
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
    validateLengthArgument(
        size,
        buffer.remaining() / (2 * Integer.BYTES),
        PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_ENTRY_COUNT_B43DED2F);
    final Map<String, ByteBuffer> result = new HashMap<>();
    for (int i = 0; i < size; i++) {
      final int keyLen = buffer.getInt();
      final int remainingEntryCount = size - i - 1;
      validateLengthArgument(
          keyLen,
          buffer.remaining() - Integer.BYTES - remainingEntryCount * 2 * Integer.BYTES,
          PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_KEY_LENGTH_7C3A3C98);
      final byte[] keyBytes = new byte[keyLen];
      buffer.get(keyBytes);
      final String key = new String(keyBytes, StandardCharsets.UTF_8);
      final int valueLen = buffer.getInt();
      validateLengthArgument(
          valueLen,
          buffer.remaining() - remainingEntryCount * 2 * Integer.BYTES,
          PipeMessages.EXCEPTION_INVALID_REGION_PROGRESS_VALUE_LENGTH_6192D17F);
      final byte[] valueBytes = new byte[valueLen];
      buffer.get(valueBytes);
      result.put(key, ByteBuffer.wrap(valueBytes).asReadOnlyBuffer());
    }
    return result;
  }

  private static void validateLengthArgument(
      final int value, final int maximum, final String message) {
    if (value < 0 || value > maximum) {
      throw new IllegalArgumentException(String.format(message, value));
    }
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
