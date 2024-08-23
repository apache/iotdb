/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.LastCacheContainer;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TableDeviceLastCache {
  static final int EMPTY_INSTANCE_SIZE =
      RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + Long.BYTES
          + (int) RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

  private final Map<String, TimeValuePair> measurement2CachedLastMap = new ConcurrentHashMap<>();
  private long lastTime = Long.MIN_VALUE;

  public int update(
      final String database,
      final String tableName,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    final AtomicInteger diff = new AtomicInteger(0);
    measurementUpdateMap.forEach(
        (k, v) -> {
          if (!measurement2CachedLastMap.containsKey(k)) {
            k = DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, k);
            diff.addAndGet(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
          }
          if (lastTime < v.getTimestamp()) {
            lastTime = v.getTimestamp();
          }
          final TimeValuePair oldTV = measurement2CachedLastMap.put(k, v);
          diff.addAndGet(
              LastCacheContainer.getDiffSize(
                  Objects.nonNull(oldTV) ? oldTV.getValue() : null, v.getValue()));
        });
    return diff.get();
  }

  public TimeValuePair getTimeValuePair(final String measurement) {
    return measurement2CachedLastMap.get(measurement);
  }

  // Shall pass in "null" if last by time
  public Pair<Long, Map<String, TsPrimitiveType>> getLastRow(final String measurement) {
    final long alignTime =
        Objects.nonNull(measurement)
            ? measurement2CachedLastMap.get(measurement).getTimestamp()
            : lastTime;
    return new Pair<>(
        lastTime,
        measurement2CachedLastMap.entrySet().stream()
            .filter(entry -> entry.getValue().getTimestamp() == alignTime)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue())));
  }

  public int estimateSize() {
    return EMPTY_INSTANCE_SIZE
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF * measurement2CachedLastMap.size()
        + measurement2CachedLastMap.values().stream()
            .mapToInt(TimeValuePair::getSize)
            .reduce(0, Integer::sum);
  }
}
