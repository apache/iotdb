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

import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TreeDeviceNormalSchema implements IDeviceSchema {

  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TreeDeviceTemplateSchema.class)
          + (int) RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);
  private final String database;
  private final boolean isAligned;

  private final ConcurrentMap<String, SchemaCacheEntry> measurementMap = new ConcurrentHashMap<>();

  public TreeDeviceNormalSchema(final String database, final boolean isAligned) {
    this.database = database;
    this.isAligned = isAligned;
  }

  public String getDatabase() {
    return database;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public SchemaCacheEntry getSchemaCacheEntry(final String measurement) {
    return measurementMap.get(measurement);
  }

  public int update(final String[] measurements, final IMeasurementSchema[] schemas) {
    int diff = 0;
    final int length = measurements.length;

    for (int i = 0; i < length; ++i) {
      // Skip this to avoid instance creation/gc for writing performance
      if (measurementMap.containsKey(measurements[i])) {
        continue;
      }
      diff += putEntry(measurements[i], schemas[i], null);
    }
    return diff;
  }

  public int update(final List<IMeasurementSchemaInfo> schemaInfoList) {
    return schemaInfoList.stream()
        .mapToInt(
            schemaInfo ->
                putEntry(schemaInfo.getName(), schemaInfo.getSchema(), schemaInfo.getTagMap()))
        .reduce(0, Integer::sum);
  }

  private int putEntry(
      final String measurement, final IMeasurementSchema schema, final Map<String, String> tagMap) {
    final SchemaCacheEntry putEntry = new SchemaCacheEntry(schema, tagMap);
    final SchemaCacheEntry cachedEntry = measurementMap.put(measurement, putEntry);
    return Objects.isNull(cachedEntry)
        ? (int) (RamUsageEstimator.sizeOf(measurement) + SchemaCacheEntry.estimateSize(putEntry))
        : SchemaCacheEntry.estimateSize(putEntry) - SchemaCacheEntry.estimateSize(cachedEntry);
  }

  @Override
  public int estimateSize() {
    // Do not need to calculate database because it is interned
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
