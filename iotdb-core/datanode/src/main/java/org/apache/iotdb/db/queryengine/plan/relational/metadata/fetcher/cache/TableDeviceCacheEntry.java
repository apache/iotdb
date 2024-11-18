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

import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

@ThreadSafe
public class TableDeviceCacheEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDeviceCacheEntry.class)
          + 2 * RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class);

  // the cached attributeMap may not be the latest, but there won't be any correctness problems
  // because when missing getting the key-value from this attributeMap, caller will try to get or
  // create from remote
  // there may exist key is not null, but value is null in this map, which means that the key's
  // corresponding value is null, doesn't mean that the key doesn't exist
  private final AtomicReference<IDeviceSchema> deviceSchema = new AtomicReference<>();
  private final AtomicReference<TableDeviceLastCache> lastCache = new AtomicReference<>();

  /////////////////////////////// Attribute ///////////////////////////////

  int setAttribute(
      final String database,
      final String tableName,
      final @Nonnull Map<String, Binary> attributeSetMap) {
    return (deviceSchema.compareAndSet(null, new TableAttributeSchema())
            ? TableAttributeSchema.INSTANCE_SIZE
            : 0)
        + updateAttribute(database, tableName, attributeSetMap);
  }

  int updateAttribute(
      final String database, final String tableName, final @Nonnull Map<String, Binary> updateMap) {
    // Shall only call this for original table device
    final TableAttributeSchema schema = (TableAttributeSchema) deviceSchema.get();
    final int result =
        Objects.nonNull(schema) ? schema.updateAttribute(database, tableName, updateMap) : 0;
    return Objects.nonNull(deviceSchema.get()) ? result : 0;
  }

  int invalidateAttribute() {
    final AtomicInteger size = new AtomicInteger(0);
    deviceSchema.updateAndGet(
        schema -> {
          if (schema instanceof TableAttributeSchema) {
            size.set(schema.estimateSize());
            return null;
          }
          return schema;
        });
    return size.get();
  }

  int invalidateAttributeColumn(final String attribute) {
    final AtomicInteger size = new AtomicInteger(0);
    deviceSchema.updateAndGet(
        schema -> {
          if (schema instanceof TableAttributeSchema) {
            size.set(((TableAttributeSchema) schema).removeAttribute(attribute));
            return schema;
          }
          return schema;
        });
    return size.get();
  }

  Map<String, Binary> getAttributeMap() {
    final IDeviceSchema map = deviceSchema.get();
    // Cache miss
    if (Objects.isNull(map)) {
      return null;
    }
    return map instanceof TableAttributeSchema
        ? ((TableAttributeSchema) map).getAttributeMap()
        : Collections.emptyMap();
  }

  /////////////////////////////// Tree model ///////////////////////////////

  int setDeviceSchema(final String database, final DeviceSchemaInfo deviceSchemaInfo) {
    // Safe here because tree schema is invalidated by the whole entry
    if (deviceSchemaInfo.getTemplateId() == NON_TEMPLATE) {
      final int result =
          (deviceSchema.compareAndSet(
                  null, new TreeDeviceNormalSchema(database, deviceSchemaInfo.isAligned()))
              ? TreeDeviceNormalSchema.INSTANCE_SIZE
              : 0);
      return deviceSchema.get() instanceof TreeDeviceNormalSchema
          ? result
              + ((TreeDeviceNormalSchema) deviceSchema.get())
                  .update(deviceSchemaInfo.getMeasurementSchemaInfoList())
          : 0;
    } else {
      return deviceSchema.compareAndSet(
              null, new TreeDeviceTemplateSchema(database, deviceSchemaInfo.getTemplateId()))
          ? TreeDeviceTemplateSchema.INSTANCE_SIZE
          : 0;
    }
  }

  int setMeasurementSchema(
      final String database,
      final boolean isAligned,
      final String[] measurements,
      final IMeasurementSchema[] schemas) {
    // Safe here because tree schema is invalidated by the whole entry
    final int result =
        (deviceSchema.compareAndSet(null, new TreeDeviceNormalSchema(database, isAligned))
            ? TreeDeviceNormalSchema.INSTANCE_SIZE
            : 0);
    return deviceSchema.get() instanceof TreeDeviceNormalSchema
        ? result + ((TreeDeviceNormalSchema) deviceSchema.get()).update(measurements, schemas)
        : 0;
  }

  IDeviceSchema getDeviceSchema() {
    return deviceSchema.get();
  }

  int invalidateTreeSchema() {
    final AtomicInteger size = new AtomicInteger(0);
    deviceSchema.updateAndGet(
        schema -> {
          if (schema instanceof TreeDeviceNormalSchema
              || schema instanceof TreeDeviceTemplateSchema) {
            size.set(schema.estimateSize());
            return null;
          }
          return schema;
        });
    return size.get();
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  int initOrInvalidateLastCache(
      final String database,
      final String tableName,
      final String[] measurements,
      final boolean isInvalidate,
      final boolean isTableModel) {
    int result =
        lastCache.compareAndSet(null, new TableDeviceLastCache())
            ? TableDeviceLastCache.INSTANCE_SIZE
            : 0;
    final TableDeviceLastCache cache = lastCache.get();
    result +=
        Objects.nonNull(cache)
            ? cache.initOrInvalidate(database, tableName, measurements, isInvalidate, isTableModel)
            : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  int tryUpdateLastCache(final String[] measurements, final TimeValuePair[] timeValuePairs) {
    final TableDeviceLastCache cache = lastCache.get();
    final int result = Objects.nonNull(cache) ? cache.tryUpdate(measurements, timeValuePairs) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  int invalidateLastCache(final String measurement, final boolean isTableModel) {
    final TableDeviceLastCache cache = lastCache.get();
    final int result = Objects.nonNull(cache) ? cache.invalidate(measurement, isTableModel) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  TimeValuePair getTimeValuePair(final String measurement) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getTimeValuePair(measurement) : null;
  }

  // Shall pass in "" if last by time
  Optional<Pair<OptionalLong, TsPrimitiveType[]>> getLastRow(
      final String sourceMeasurement, final List<String> targetMeasurements) {
    final TableDeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache)
        ? cache.getLastRow(sourceMeasurement, targetMeasurements)
        : Optional.empty();
  }

  int invalidateLastCache() {
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

  int estimateSize() {
    final IDeviceSchema schema = deviceSchema.get();
    final TableDeviceLastCache cache = lastCache.get();
    return (int)
        (INSTANCE_SIZE
            + (Objects.nonNull(schema) ? schema.estimateSize() : 0)
            + (Objects.nonNull(cache) ? cache.estimateSize() : 0));
  }
}
