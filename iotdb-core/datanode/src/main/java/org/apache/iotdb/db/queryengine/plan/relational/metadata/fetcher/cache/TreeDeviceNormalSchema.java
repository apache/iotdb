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

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.SchemaCacheEntry;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TreeDeviceNormalSchema implements IDeviceSchema {

  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TreeDeviceTemplateSchema.class)
          + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  private final String storageGroup;
  private final ConcurrentMap<String, SchemaCacheEntry> measurementMap = new ConcurrentHashMap<>();

  public TreeDeviceNormalSchema(final String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public String getDatabase() {
    return storageGroup;
  }

  public SchemaCacheEntry getSchemaCacheEntry(final String measurement) {
    return measurementMap.get(measurement);
  }

  public int update(final String measurement, final SchemaCacheEntry entry) {
    final SchemaCacheEntry cachedEntry = measurementMap.put(measurement, entry);
    return Objects.isNull(cachedEntry)
        ? (int) (RamUsageEstimator.sizeOf(measurement) + SchemaCacheEntry.estimateSize(entry))
        : SchemaCacheEntry.estimateSize(entry) - SchemaCacheEntry.estimateSize(cachedEntry);
  }

  @Override
  public int estimateSize() {
    return INSTANCE_SIZE
        + measurementMap.entrySet().stream()
            .mapToInt(
                entry ->
                    Math.toIntExact(
                        RamUsageEstimator.sizeOf(entry.getKey())
                            + SchemaCacheEntry.estimateSize(entry.getValue())))
            .reduce(0, Integer::sum);
  }
}
