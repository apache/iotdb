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

import java.util.Map;
import java.util.Objects;
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
 * <p>3. When the attributeMap does not contain an attributeKey, then the value is {@link null}.
 * Note that we do not tell whether an attributeKey exists here, and it shall be judged from table
 * schema.
 */
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
  public void put(
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
          entry -> entry.setAttribute(attributeMap),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void update(
      final String database,
      final String tableName,
      final String[] deviceId,
      final Map<String, String> attributeMap) {
    readWriteLock.readLock().lock();
    try {
      final TableDeviceCacheEntry entry =
          dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
      if (Objects.nonNull(entry)) {
        entry.update(attributeMap);
      }
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void updateLastCache(
      final String database,
      final String tableName,
      final String[] deviceId,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    readWriteLock.readLock().lock();
    try {
      dualKeyCache.update(
          new TableId(database, tableName),
          new TableDeviceId(deviceId),
          new TableDeviceCacheEntry(),
          entry -> entry.update(database, tableName, measurementUpdateMap),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void tryUpdateLastCacheWithoutLock(
      final String database,
      final String tableName,
      final String[] deviceId,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    dualKeyCache.update(
        new TableId(database, tableName),
        new TableDeviceId(deviceId),
        new TableDeviceCacheEntry(),
        entry -> entry.tryUpdate(database, tableName, measurementUpdateMap),
        false);
  }

  public TimeValuePair getLastEntry(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String measurement) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
    return Objects.nonNull(entry) ? entry.getTimeValuePair(measurement) : null;
  }

  public Pair<Long, Map<String, TsPrimitiveType>> getLastRow(
      final String database,
      final String tableName,
      final String[] deviceId,
      final String measurement) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, tableName), new TableDeviceId(deviceId));
    return Objects.nonNull(entry) ? entry.getLastRow(measurement) : null;
  }

  // "deviceId" shall equal to null when invalidate the cache of the whole table
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
