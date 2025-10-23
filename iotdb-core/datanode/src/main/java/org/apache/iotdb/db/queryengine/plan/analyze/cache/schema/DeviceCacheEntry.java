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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

@ThreadSafe
public class DeviceCacheEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceCacheEntry.class)
          + 2 * RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class);

  // the cached attributeMap may not be the latest, but there won't be any correctness problems
  // because when missing getting the key-value from this attributeMap, caller will try to get or
  // create from remote
  // there may exist key is not null, but value is null in this map, which means that the key's
  // corresponding value is null, doesn't mean that the key doesn't exist
  private final AtomicReference<IDeviceSchema> deviceSchema = new AtomicReference<>();
  private final AtomicReference<DeviceLastCache> lastCache = new AtomicReference<>();

  int setDeviceSchema(final String database, final DeviceSchemaInfo deviceSchemaInfo) {
    // Safe here because tree schema is invalidated by the whole entry
    if (deviceSchemaInfo.getTemplateId() == NON_TEMPLATE) {
      final int result =
          (deviceSchema.compareAndSet(
                  null, new DeviceNormalSchema(database, deviceSchemaInfo.isAligned()))
              ? DeviceNormalSchema.INSTANCE_SIZE
              : 0);
      return deviceSchema.get() instanceof DeviceNormalSchema
          ? result
              + ((DeviceNormalSchema) deviceSchema.get())
                  .update(deviceSchemaInfo.getMeasurementSchemaInfoList())
          : 0;
    } else {
      return deviceSchema.compareAndSet(
              null, new DeviceTemplateSchema(database, deviceSchemaInfo.getTemplateId()))
          ? DeviceTemplateSchema.INSTANCE_SIZE
          : 0;
    }
  }

  int setMeasurementSchema(
      final String database,
      final boolean isAligned,
      final String[] measurements,
      final IMeasurementSchema[] schemas) {
    if (schemas == null) {
      return 0;
    }
    // Safe here because schema is invalidated by the whole entry
    final int result =
        (deviceSchema.compareAndSet(null, new DeviceNormalSchema(database, isAligned))
            ? DeviceNormalSchema.INSTANCE_SIZE
            : 0);
    return deviceSchema.get() instanceof DeviceNormalSchema
        ? result + ((DeviceNormalSchema) deviceSchema.get()).update(measurements, schemas)
        : 0;
  }

  IDeviceSchema getDeviceSchema() {
    return deviceSchema.get();
  }

  int invalidateSchema() {
    final AtomicInteger size = new AtomicInteger(0);
    deviceSchema.updateAndGet(
        schema -> {
          size.set(schema.estimateSize());
          return schema;
        });
    return size.get();
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  int initOrInvalidateLastCache(final String[] measurements, final boolean isInvalidate) {
    int result =
        lastCache.compareAndSet(null, new DeviceLastCache()) ? DeviceLastCache.INSTANCE_SIZE : 0;
    final DeviceLastCache cache = lastCache.get();
    result += Objects.nonNull(cache) ? cache.initOrInvalidate(measurements, isInvalidate) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  int tryUpdateLastCache(
      final String[] measurements, final TimeValuePair[] timeValuePairs, boolean invalidateNull) {
    final DeviceLastCache cache = lastCache.get();
    final int result =
        Objects.nonNull(cache) ? cache.tryUpdate(measurements, timeValuePairs, invalidateNull) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  int tryUpdateLastCache(final String[] measurements, final TimeValuePair[] timeValuePairs) {
    return tryUpdateLastCache(measurements, timeValuePairs, false);
  }

  int invalidateLastCache(final String measurement) {
    final DeviceLastCache cache = lastCache.get();
    final int result = Objects.nonNull(cache) ? cache.invalidate(measurement) : 0;
    return Objects.nonNull(lastCache.get()) ? result : 0;
  }

  TimeValuePair getTimeValuePair(final String measurement) {
    final DeviceLastCache cache = lastCache.get();
    return Objects.nonNull(cache) ? cache.getTimeValuePair(measurement) : null;
  }

  boolean updateInputMap(final @Nonnull Map<String, TimeValuePair> updateMap) {
    for (final String measurement : updateMap.keySet()) {
      final TimeValuePair result = getTimeValuePair(measurement);
      if (result == null) {
        return false;
      }
      updateMap.put(measurement, result);
    }
    return true;
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
    final DeviceLastCache cache = lastCache.get();
    return (int)
        (INSTANCE_SIZE
            + (Objects.nonNull(schema) ? schema.estimateSize() : 0)
            + (Objects.nonNull(cache) ? cache.estimateSize() : 0));
  }
}
