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

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateClearContainer implements UpdateContainer {

  static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateClearContainer.class)
          + RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class)
          + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private final Set<String> tableNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  public long updateAttribute(
      final String tableName,
      final String[] deviceId,
      final Map<String, String> updatedAttributes) {
    return tableNames.add(tableName) ? RamUsageEstimator.sizeOf(tableName) : 0;
  }

  @Override
  public ByteBuffer getUpdateBuffer() {
    return null;
  }

  @Override
  public Pair<Integer, Boolean> updateSelfByCommitBuffer(final ByteBuffer commitBuffer) {
    return null;
  }

  @Override
  public void serialize(final OutputStream outputstream) throws IOException {
    ReadWriteIOUtils.write(tableNames.size(), outputstream);
    for (final String tableName : tableNames) {
      ReadWriteIOUtils.write(tableName, outputstream);
    }
  }

  @Override
  public void deserialize(final InputStream fileInputStream) throws IOException {
    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; ++i) {
      tableNames.add(ReadWriteIOUtils.readString(fileInputStream));
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + tableNames.stream().mapToLong(RamUsageEstimator::sizeOf).reduce(0L, Long::sum);
  }
}
