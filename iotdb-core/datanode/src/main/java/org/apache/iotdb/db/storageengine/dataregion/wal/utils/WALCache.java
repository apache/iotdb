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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.nio.ByteBuffer;
import java.util.List;

public interface WALCache {

  ByteBuffer load(final WALEntryPosition key);

  void invalidateAll();

  CacheStats stats();

  public static ByteBuffer getEntryBySegment(WALEntryPosition key, ByteBuffer segment) {
    List<Integer> list = key.getWalSegmentMetaBuffersSize();
    int pos = 0;
    for (int size : list) {
      if (key.getPosition() == pos) {
        final byte[] data = new byte[size];
        System.arraycopy(segment.array(), pos, data, 0, size);
        return ByteBuffer.wrap(data);
      }
      pos += size;
    }
    return null;
  }
}
