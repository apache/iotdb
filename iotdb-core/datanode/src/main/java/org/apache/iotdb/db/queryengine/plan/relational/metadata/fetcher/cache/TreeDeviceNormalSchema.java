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
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TreeDeviceNormalSchema implements IDeviceSchema {

  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TreeDeviceTemplateSchema.class)
          + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  private final String storageGroup;
  private final boolean isAligned;

  private final ConcurrentMap<String, SchemaCacheEntry> measurementMap = new ConcurrentHashMap<>();

  public TreeDeviceNormalSchema(final String storageGroup, final boolean isAligned) {
    this.storageGroup = storageGroup;
    this.isAligned = isAligned;
  }

  public String getDatabase() {
    return storageGroup;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public SchemaCacheEntry getSchemaCacheEntry(final String measurement) {
    return measurementMap.get(measurement);
  }

  public int update(final String[] measurements, final MeasurementSchema[] schemas) {
    int diff = 0;
    final int length = measurements.length;

    for (int i = 0; i < length; ++i) {
      final SchemaCacheEntry putEntry = new SchemaCacheEntry(schemas[i], null, false);
      final SchemaCacheEntry cachedEntry = measurementMap.put(measurements[i], putEntry);
      diff +=
          Objects.isNull(cachedEntry)
              ? (int)
                  (RamUsageEstimator.sizeOf(measurements[i])
                      + SchemaCacheEntry.estimateSize(putEntry))
              : SchemaCacheEntry.estimateSize(putEntry)
                  - SchemaCacheEntry.estimateSize(cachedEntry);
    }
    return diff;
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
