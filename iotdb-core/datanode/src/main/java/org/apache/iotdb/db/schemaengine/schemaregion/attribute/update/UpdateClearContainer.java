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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class UpdateClearContainer implements UpdateContainer {

  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class)
          + RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class)
          + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private final Set<String> tableNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public UpdateClearContainer() {
    // Public constructor
  }

  UpdateClearContainer(final Set<String> tableNames) {
    this.tableNames.addAll(tableNames);
  }

  @Override
  public long updateAttribute(
      final String tableName,
      final String[] deviceId,
      final Map<String, Binary> updatedAttributes) {
    return tableNames.add(tableName) ? RamUsageEstimator.sizeOf(tableName) : 0;
  }

  @Override
  public byte[] getUpdateContent(
      final @Nonnull AtomicInteger limitBytes, final @Nonnull AtomicBoolean hasRemaining) {
    final RewritableByteArrayOutputStream outputStream = new RewritableByteArrayOutputStream();
    try {
      serializeWithLimit(outputStream, limitBytes, hasRemaining);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return outputStream.toByteArray();
  }

  @Override
  public long invalidate(final String tableName) {
    if (tableNames.contains(tableName)) {
      tableNames.remove(tableName);
      return RamUsageEstimator.sizeOf(tableName);
    }
    return 0;
  }

  @Override
  public long invalidate(final String[] pathNodes) {
    // Do nothing
    return 0;
  }

  @Override
  public long invalidate(final String tableName, final String attributeName) {
    // Do nothing
    return 0;
  }

  private void serializeWithLimit(
      final RewritableByteArrayOutputStream outputStream,
      final AtomicInteger limitBytes,
      final AtomicBoolean hasRemaining)
      throws IOException {
    ReadWriteIOUtils.write((byte) 0, outputStream);
    final int setSizeOffset = outputStream.skipInt();
    int setEntryCount = 0;
    int newSize;
    for (final String tableName : tableNames) {
      final byte[] tableBytes = tableName.getBytes(TSFileConfig.STRING_CHARSET);
      newSize = Integer.BYTES + tableBytes.length;
      if (limitBytes.get() < newSize) {
        outputStream.rewrite(setEntryCount, setSizeOffset);
        hasRemaining.set(true);
        return;
      }
      limitBytes.addAndGet(-newSize);
      ++setEntryCount;
      outputStream.writeWithLength(tableBytes);
    }
    outputStream.rewrite(tableNames.size(), setSizeOffset);
  }

  @Override
  public Pair<Long, Boolean> updateSelfByCommitContainer(final UpdateContainer commitContainer) {
    if (!(commitContainer instanceof UpdateClearContainer)) {
      return new Pair<>(0L, false);
    }
    final AtomicLong reducedBytes = new AtomicLong(0);
    ((UpdateClearContainer) commitContainer)
        .getTableNames()
        .forEach(
            tableName -> {
              if (tableNames.contains(tableName)) {
                tableNames.remove(tableName);
                reducedBytes.addAndGet(RamUsageEstimator.sizeOf(tableName));
              }
            });
    return new Pair<>(reducedBytes.get(), tableNames.isEmpty());
  }

  @Override
  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) 0, outputStream);
    ReadWriteIOUtils.write(tableNames.size(), outputStream);
    for (final String tableName : tableNames) {
      ReadWriteIOUtils.write(tableName, outputStream);
    }
  }

  @Override
  public void deserialize(final InputStream inputStream) throws IOException {
    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      tableNames.add(ReadWriteIOUtils.readString(inputStream));
    }
  }

  public Set<String> getTableNames() {
    return tableNames;
  }

  long ramBytesUsed() {
    return INSTANCE_SIZE
        + tableNames.stream().mapToLong(RamUsageEstimator::sizeOf).reduce(0L, Long::sum);
  }
}
