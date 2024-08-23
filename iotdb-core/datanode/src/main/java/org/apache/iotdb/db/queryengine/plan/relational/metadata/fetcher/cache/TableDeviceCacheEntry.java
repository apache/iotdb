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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableDeviceCacheEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDeviceCacheEntry.class);

  // the cached attributeMap may not be the latest, but there won't be any correctness problems
  // because when missing getting the key-value from this attributeMap, caller will try to get or
  // create from remote
  // there may exist key is not null, but value is null in this map, which means that the key's
  // corresponding value is null, doesn't mean that the key doesn't exist
  private ConcurrentHashMap<String, String> attributeMap = null;
  private AtomicReference<TableDeviceLastCache> lastCache = new AtomicReference<>();

  /////////////////////////////// Attribute ///////////////////////////////

  public int setAttribute(
      final String database,
      final String tableName,
      final @Nonnull Map<String, String> attributeSetMap) {
    int result = 0;
    if (Objects.isNull(attributeMap)) {
      attributeMap = new ConcurrentHashMap<>();
      result += (int) RamUsageEstimator.shallowSizeOf(attributeMap);
    }
    return result + updateAttribute(database, tableName, attributeSetMap);
  }

  public int updateAttribute(
      final String database, final String tableName, final @Nonnull Map<String, String> updateMap) {
    final AtomicInteger diff = new AtomicInteger(0);
    updateMap.forEach(
        (k, v) -> {
          if (Objects.nonNull(v)) {
            if (!attributeMap.containsKey(k)) {
              k = DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, k);
              diff.addAndGet(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            }
            diff.addAndGet(
                (int)
                    (RamUsageEstimator.sizeOf(v)
                        - RamUsageEstimator.sizeOf(attributeMap.put(k, v))));
          } else {
            attributeMap.remove(k);
            diff.addAndGet((int) (-RamUsageEstimator.sizeOf(k) - RamUsageEstimator.sizeOf(v)));
          }
        });
    return diff.get();
  }

  public String getAttribute(final String key) {
    return attributeMap.get(key);
  }

  public Map<String, String> getAttributeMap() {
    return attributeMap;
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  public int updateLastCache(
      final String database,
      final String tableName,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    return (lastCache.compareAndSet(null, new TableDeviceLastCache())
            ? TableDeviceLastCache.EMPTY_INSTANCE_SIZE
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
  public Pair<Long, Map<String, TsPrimitiveType>> getLastRow(final String measurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getLastRow(measurement) : null;
  }

  public int invalidateLastCache() {
    final TableDeviceLastCache cache = lastCache.get();
    final int size = cache.estimateSize();
    lastCache = new AtomicReference<>();
    return size;
  }

  /////////////////////////////// Management ///////////////////////////////

  public int estimateSize() {
    final TableDeviceLastCache cache = lastCache.get();
    return (int)
        (INSTANCE_SIZE
            + RamUsageEstimator.sizeOfMap(attributeMap)
            + (Objects.nonNull(cache) ? cache.estimateSize() : 0));
  }
}
