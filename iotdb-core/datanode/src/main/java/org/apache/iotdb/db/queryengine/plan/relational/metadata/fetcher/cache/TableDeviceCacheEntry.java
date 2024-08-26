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

import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@ThreadSafe
public class TableDeviceCacheEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDeviceCacheEntry.class);

  // the cached attributeMap may not be the latest, but there won't be any correctness problems
  // because when missing getting the key-value from this attributeMap, caller will try to get or
  // create from remote
  // there may exist key is not null, but value is null in this map, which means that the key's
  // corresponding value is null, doesn't mean that the key doesn't exist
  private final AtomicReference<ConcurrentHashMap<String, String>> attributeMap =
      new AtomicReference<>();
  private final AtomicReference<TableDeviceLastCache> lastCache = new AtomicReference<>();

  /////////////////////////////// Attribute ///////////////////////////////

  public int setAttribute(
      final String database,
      final String tableName,
      final @Nonnull Map<String, String> attributeSetMap) {
    return (attributeMap.compareAndSet(null, new ConcurrentHashMap<>())
            ? (int) RamUsageEstimator.shallowSizeOf(attributeMap)
            : 0)
        + updateAttribute(database, tableName, attributeSetMap);
  }

  public int updateAttribute(
      final String database, final String tableName, final @Nonnull Map<String, String> updateMap) {
    final Map<String, String> map = attributeMap.get();
    if (Objects.isNull(map)) {
      return 0;
    }
    final AtomicInteger diff = new AtomicInteger(0);
    updateMap.forEach(
        (k, v) -> {
          if (Objects.nonNull(v)) {
            if (!map.containsKey(k)) {
              k = DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, k);
              diff.addAndGet(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            }
            diff.addAndGet(
                (int) (RamUsageEstimator.sizeOf(v) - RamUsageEstimator.sizeOf(map.put(k, v))));
          } else {
            map.remove(k);
            diff.addAndGet((int) (-RamUsageEstimator.sizeOf(k) - RamUsageEstimator.sizeOf(v)));
          }
        });
    // Typically the "update" and "invalidate" won't be concurrently called
    // Here we reserve the check for consistency and potential safety
    return Objects.nonNull(attributeMap.get()) ? diff.get() : 0;
  }

  public int invalidateAttribute() {
    final AtomicInteger size = new AtomicInteger(0);
    attributeMap.updateAndGet(
        map -> {
          if (Objects.nonNull(map)) {
            size.set(
                (int)
                    (RamUsageEstimator.NUM_BYTES_OBJECT_REF * map.size()
                        + map.values().stream()
                            .mapToLong(RamUsageEstimator::sizeOf)
                            .reduce(0, Long::sum)));
          }
          return null;
        });
    return size.get();
  }

  public String getAttribute(final String key) {
    final Map<String, String> map = attributeMap.get();
    return Objects.nonNull(map) ? map.get(key) : null;
  }

  public Map<String, String> getAttributeMap() {
    return attributeMap.get();
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  public int updateLastCache(
      final String database,
      final String tableName,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    return (lastCache.compareAndSet(null, new TableDeviceLastCache())
            ? TableDeviceLastCache.INSTANCE_SIZE
            : 0)
        + tryUpdate(database, tableName, measurementUpdateMap);
  }

  public int tryUpdate(
      final String database,
      final String tableName,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    final TableDeviceLastCache cache = lastCache.get();
    final int result =
        Objects.nonNull(cache) ? cache.update(database, tableName, measurementUpdateMap) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  public TimeValuePair getTimeValuePair(final String measurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getTimeValuePair(measurement) : null;
  }

  // Shall pass in "null" if last by time
  public Long getLastTime(final String measurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getLastTime(measurement) : null;
  }

  // Shall pass in "null" if last by time
  public TsPrimitiveType getLastBy(final String measurement, final String targetMeasurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getLastBy(measurement, targetMeasurement) : null;
  }

  // Shall pass in "null" if last by time
  public Pair<Long, Map<String, TsPrimitiveType>> getLastRow(final String measurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getLastRow(measurement) : null;
  }

  public int invalidateLastCache() {
    final AtomicInteger size = new AtomicInteger(0);
    lastCache.updateAndGet(
        cacheEntry -> {
          if (Objects.nonNull(cacheEntry)) {
            size.set(cacheEntry.estimateSize());
          }
          return null;
        });
    return size.get();
  }

  /////////////////////////////// Management ///////////////////////////////

  public int estimateSize() {
    final Map<String, String> map = attributeMap.get();
    final TableDeviceLastCache cache = lastCache.get();
    return (int)
        (INSTANCE_SIZE
            + (Objects.nonNull(map)
                ? RamUsageEstimator.NUM_BYTES_OBJECT_REF * map.size()
                    + map.values().stream()
                        .mapToInt(attrValue -> (int) RamUsageEstimator.sizeOf(attrValue))
                        .reduce(0, Integer::sum)
                : 0)
            + (Objects.nonNull(cache) ? cache.estimateSize() : 0));
  }
}
