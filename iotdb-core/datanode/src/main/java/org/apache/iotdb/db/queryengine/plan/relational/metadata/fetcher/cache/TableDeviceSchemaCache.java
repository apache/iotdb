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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The {@link TableDeviceSchemaCache} caches some of the devices and their attributes of tables.
 * Here are its semantics:
 *
 * <p>1. If a deviceId misses cache, it does not necessarily mean that the device does not exist,
 * Since the cache records only part of the devices.
 *
 * <p>2. If a device is in cache, the attributes will be finally identical to the {@link
 * SchemaRegion}'s version. In reading this may temporarily return false result, and in writing when
 * the new attribute differs from the original ones, we send the new attributes to the {@link
 * SchemaRegion} anyway. This may not update the attributes in {@link SchemaRegion} since the
 * attributes in cache may be stale, but it's okay and {@link SchemaRegion} will just do nothing.
 *
 * <p>3. When the attributeMap does not contain an attributeKey, then the value is {@code null}.
 * Note that we do not tell whether an attributeKey exists here, and it shall be judged from table
 * schema.
 */
@ThreadSafe
public class TableDeviceSchemaCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final IDualKeyCache<TableId, TableDeviceId, TableDeviceCacheEntry> dualKeyCache;

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  public TableDeviceSchemaCache() {
    final DualKeyCacheBuilder<TableId, TableDeviceId, TableDeviceCacheEntry> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(TableId::estimateSize)
            .secondKeySizeComputer(TableDeviceId::estimateSize)
            .valueSizeComputer(TableDeviceCacheEntry::estimateSize)
            .build();
  }

  /////////////////////////////// Attribute ///////////////////////////////

  // The input deviceId shall have its tailing nulls trimmed
  public Map<String, String> getDeviceAttribute(
      final String database, final String tableName, final String[] deviceId) {
    readWriteLock.readLock().lock();
    try {
      final TableDeviceCacheEntry entry =
          dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
      return entry == null ? null : entry.getAttributeMap();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // The input deviceId shall have its tailing nulls trimmed
  public void putAttributes(
      final String database,
      final String tableName,
      final String[] deviceId,
      final ConcurrentMap<String, String> attributeMap) {
    readWriteLock.readLock().lock();
    try {
      dualKeyCache.update(
          new TableId(database, tableName),
          new TableDeviceId(deviceId),
          new TableDeviceCacheEntry(),
          entry -> entry.setAttribute(database, tableName, attributeMap),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void updateAttributes(
      final String database,
      final String tableName,
      final String[] deviceId,
      final Map<String, String> attributeMap) {
    dualKeyCache.update(
        new TableId(database, tableName),
        new TableDeviceId(deviceId),
        new TableDeviceCacheEntry(),
        entry -> entry.updateAttribute(database, tableName, attributeMap),
        false);
  }

  /**
   * Invalidate the attribute cache of one device. The "deviceId" shall equal to {@code null} when
   * invalidate the cache of the whole table.
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param deviceId the deviceId without tableName
   */
  public void invalidateAttributes(
      final String database, final String tableName, final String[] deviceId) {
    dualKeyCache.update(
        new TableId(database, tableName),
        Objects.nonNull(deviceId) ? new TableDeviceId(deviceId) : null,
        new TableDeviceCacheEntry(),
        entry -> -entry.invalidateAttribute(),
        false);
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  /**
   * Update the last cache on query or data recover. The input "TimeValuePair" shall never be or
   * contain {@code null} unless the measurement is the time measurement "". If the measurements are
   * all {@code null}s, the timeValuePair shall be {@link
   * TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}.
   *
   * <p>If the global last time is queried or recovered, the measurement shall be an empty string
   * and time shall be in the timeValuePair's timestamp, or the device's last time won't be updated
   * because we cannot guarantee the completeness of the existing measurements in cache.
   *
   * <p>The input "TimeValuePair" shall never be or contain {@code null} unless the measurement is
   * the time measurement "".
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param deviceId the deviceId without tableName
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCache(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs) {
    readWriteLock.readLock().lock();
    try {
      dualKeyCache.update(
          new TableId(database, tableName),
          new TableDeviceId(deviceId),
          new TableDeviceCacheEntry(),
          entry -> entry.updateLastCache(database, tableName, measurements, timeValuePairs),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Update the last cache in writing. The input "TimeValuePair" shall never be or contain {@code
   * null}. For correctness, this will put the cache lazily and only update the existing last caches
   * of measurements.
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void mayUpdateLastCacheWithoutLock(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs) {
    dualKeyCache.update(
        new TableId(database, tableName),
        new TableDeviceId(deviceId),
        new TableDeviceCacheEntry(),
        entry -> entry.tryUpdate(database, tableName, measurements, timeValuePairs),
        false);
  }

  /**
   * Get the last entry of a measurement, the measurement shall never be "time".
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param deviceId the deviceId without tableName
   * @param measurement the measurement to get
   * @return {@code null} iff cache miss, {@link TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR} iff
   *     cache hit but result is {@code null}, and the result value otherwise.
   */
  public TimeValuePair getLastEntry(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String measurement) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
    return Objects.nonNull(entry) ? entry.getTimeValuePair(measurement) : null;
  }

  /**
   * Get the last value of measurements last by a target measurement. If the caller wants to last by
   * time or get the time last by another source measurement, the measurement shall be "" to
   * indicate the time column.
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param deviceId the deviceId without tableName
   * @param sourceMeasurement the measurement to get
   * @return {@code Optional.empty()} iff the last cache is miss at all; Or the optional of a pair,
   *     the {@link Pair#left} will be the source measurement's last time, (OptionalLong.empty() iff
   *     the source measurement is all {@code null}); {@link Pair#right} will be an {@link
   *     TsPrimitiveType} array, whose element will be {@code null} if cache miss, {@link
   *     TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE} iff cache hit and the measurement is {@code
   *     null} when last by the source measurement's time, and the result value otherwise.
   */
  public Optional<Pair<OptionalLong, TsPrimitiveType[]>> getLastRow(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String sourceMeasurement,
      final List<String> targetMeasurements) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
    return Objects.nonNull(entry)
        ? entry.getLastRow(sourceMeasurement, targetMeasurements)
        : Optional.empty();
  }

  /**
   * Invalidate the last cache of one device. Unlike time column, the "deviceId" shall equal to
   * {@code null} when invalidate the cache of the whole table.
   *
   * @param database the device's database, without "root"
   * @param tableName tableName
   * @param deviceId the deviceId without tableName
   */
  public void invalidateLastCache(
      final String database, final String tableName, final String[] deviceId) {
    dualKeyCache.update(
        new TableId(database, tableName),
        Objects.nonNull(deviceId) ? new TableDeviceId(deviceId) : null,
        new TableDeviceCacheEntry(),
        entry -> -entry.invalidateLastCache(),
        false);
  }

  public void invalidate(final String database) {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidateForTable(database);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidate(final String database, final String tableName) {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidate(new TableId(database, tableName));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }
}
