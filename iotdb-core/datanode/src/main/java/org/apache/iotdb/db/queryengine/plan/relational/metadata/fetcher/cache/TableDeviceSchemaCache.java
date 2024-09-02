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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.SchemaCacheEntry;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.TableDeviceSchemaCacheMetrics;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

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
  private static final Logger logger = LoggerFactory.getLogger(TableDeviceSchemaCache.class);

  /**
   * In table model: {@literal <}QualifiedObjectName, IDeviceID, lastCache / attributes{@literal >}
   *
   * <p>In tree model: {@literal <}Pair{@literal <}node[1], tableName(translated){@literal >},
   * IDeviceID(translated), Map{@literal <}Measurement, Schema{@literal >}/templateInfo{@literal >}
   */
  private final IDualKeyCache<TableId, IDeviceID, TableDeviceCacheEntry> dualKeyCache;

  private final Map<String, String> treeModelDatabasePool = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  public TableDeviceSchemaCache() {
    dualKeyCache =
        new DualKeyCacheBuilder<TableId, IDeviceID, TableDeviceCacheEntry>()
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(TableId::estimateSize)
            .secondKeySizeComputer(deviceID -> (int) deviceID.ramBytesUsed())
            .valueSizeComputer(TableDeviceCacheEntry::estimateSize)
            .build();

    MetricService.getInstance().addMetricSet(new TableDeviceSchemaCacheMetrics(this));
  }

  /////////////////////////////// Attribute ///////////////////////////////

  // The input deviceId shall have its tailing nulls trimmed
  public Map<String, String> getDeviceAttribute(final String database, final IDeviceID deviceId) {
    readWriteLock.readLock().lock();
    try {
      final TableDeviceCacheEntry entry =
          dualKeyCache.get(new TableId(database, deviceId.getTableName()), deviceId);
      return entry == null ? null : entry.getAttributeMap();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // The input deviceId shall have its tailing nulls trimmed
  public void putAttributes(
      final String database, final IDeviceID deviceId, final Map<String, String> attributeMap) {
    readWriteLock.readLock().lock();
    try {
      dualKeyCache.update(
          new TableId(database, deviceId.getTableName()),
          deviceId,
          new TableDeviceCacheEntry(),
          entry -> entry.setAttribute(database, deviceId.getTableName(), attributeMap),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void updateAttributes(
      final String database, final IDeviceID deviceId, final Map<String, String> attributeMap) {
    dualKeyCache.update(
        new TableId(database, deviceId.getTableName()),
        deviceId,
        null,
        entry -> entry.updateAttribute(database, deviceId.getTableName(), attributeMap),
        false);
  }

  public void invalidateAttributes(final String database, final String tableName) {
    dualKeyCache.update(
        new TableId(database, tableName), deviceId -> true, entry -> -entry.invalidateAttribute());
  }

  /**
   * Invalidate the attribute cache of one device. The "deviceId" shall equal to {@code null} when
   * invalidate the cache of the whole table.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   */
  public void invalidateAttributes(final String database, final IDeviceID deviceId) {
    dualKeyCache.update(
        new TableId(database, deviceId.getTableName()),
        deviceId,
        null,
        entry -> -entry.invalidateAttribute(),
        false);
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  /**
   * Update the last cache on query or data recover. The input "TimeValuePair" shall never be or
   * contain {@code null} unless the measurement is the time measurement "". If a measurement is
   * with all {@code null}s, its timeValuePair shall be {@link
   * TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}.
   *
   * <p>If the global last time is queried or recovered, the measurement shall be an empty string
   * and time shall be in the timeValuePair's timestamp, whose value is typically {@link
   * TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE}. Or, the device's last time won't be updated because
   * we cannot guarantee the completeness of the existing measurements in cache.
   *
   * <p>The input "TimeValuePair" shall never be or contain {@code null}.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCache(
      final String database,
      final IDeviceID deviceId,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs) {
    readWriteLock.readLock().lock();
    try {
      dualKeyCache.update(
          new TableId(database, deviceId.getTableName()),
          deviceId,
          new TableDeviceCacheEntry(),
          entry ->
              entry.updateLastCache(
                  database, deviceId.getTableName(), measurements, timeValuePairs),
          true);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Update the last cache in writing. If a measurement is with all {@code null}s or is an
   * id/attribute column, its timeValuePair shall be {@code null}. For correctness, this will put
   * the cache lazily and only update the existing last caches of measurements.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void mayUpdateLastCacheWithoutLock(
      final String database,
      final IDeviceID deviceId,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs) {
    dualKeyCache.update(
        new TableId(database, deviceId.getTableName()),
        deviceId,
        null,
        entry -> entry.tryUpdateLastCache(measurements, timeValuePairs),
        false);
  }

  /**
   * Get the last entry of a measurement, the measurement shall never be "time".
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   * @param measurement the measurement to get
   * @return {@code null} iff cache miss, {@link TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR} iff
   *     cache hit but result is {@code null}, and the result value otherwise.
   */
  public TimeValuePair getLastEntry(
      final String database, final IDeviceID deviceId, final String measurement) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, deviceId.getTableName()), deviceId);
    return Objects.nonNull(entry) ? entry.getTimeValuePair(measurement) : null;
  }

  /**
   * Get the last value of measurements last by a target measurement. If the caller wants to last by
   * time or get the time last by another source measurement, the measurement shall be "" to
   * indicate the time column.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
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
      final IDeviceID deviceId,
      final String sourceMeasurement,
      final List<String> targetMeasurements) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, deviceId.getTableName()), deviceId);
    return Objects.nonNull(entry)
        ? entry.getLastRow(sourceMeasurement, targetMeasurements)
        : Optional.empty();
  }

  /**
   * Invalidate the last cache of one table.
   *
   * @param database the device's database, without "root"
   * @param table tableName
   */
  public void invalidateLastCache(final String database, final String table) {
    dualKeyCache.update(
        new TableId(database, table), deviceId -> true, entry -> -entry.invalidateLastCache());
  }

  /**
   * Invalidate the last cache of one device.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   */
  public void invalidateLastCache(final String database, final IDeviceID deviceId) {
    dualKeyCache.update(
        new TableId(database, deviceId.getTableName()),
        deviceId,
        null,
        entry -> -entry.invalidateLastCache(),
        false);
  }

  /////////////////////////////// Tree model ///////////////////////////////

  public void putMeasurementSchema(
      final String database,
      final String[] devicePath,
      final String measurement,
      final SchemaCacheEntry measurementSchema) {
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(devicePath));
    final String previousDatabase = treeModelDatabasePool.putIfAbsent(database, database);
    dualKeyCache.update(
        new TableId(devicePath[1], deviceID.getTableName()),
        deviceID,
        null,
        entry ->
            entry.setMeasurementSchema(
                Objects.nonNull(previousDatabase) ? previousDatabase : database,
                measurement,
                measurementSchema),
        false);
  }

  public IDeviceSchema getDeviceSchema(final String[] devicePath) {
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(devicePath));
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(devicePath[1], deviceID.getTableName()), deviceID);
    return Objects.nonNull(entry) ? entry.getDeviceSchema() : null;
  }

  /**
   * Update the last cache on query or data recover in tree model. The input "TimeValuePair" shall
   * never be or contain {@code null} unless the measurement is the time measurement "". If the
   * measurements are all {@code null}s, the timeValuePair shall be {@link
   * TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}.
   *
   * <p>If the global last time is queried or recovered, the measurement shall be an empty string
   * and time shall be in the timeValuePair's timestamp, whose value is typically {@link
   * TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE}. Or, the device's last time won't be updated because
   * we cannot guarantee the completeness of the existing measurements in cache.
   *
   * <p>The input "TimeValuePair" shall never be or contain {@code null}.
   *
   * @param database the device's database, without "root"
   * @param deviceId IDeviceID
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCache(
      final String database,
      final IDeviceID deviceId,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs,
      final boolean isAligned,
      final MeasurementSchema[] measurementSchemas) {
    final String previousDatabase = treeModelDatabasePool.putIfAbsent(database, database);
    dualKeyCache.update(
        new TableId(database, deviceId.getTableName()),
        deviceId,
        null,
        entry ->
            entry.tryUpdateLastCache(measurements, timeValuePairs)
                + entry.setMeasurementSchema(
                    Objects.nonNull(previousDatabase) ? previousDatabase : database,
                    isAligned,
                    measurements,
                    measurementSchemas),
        false);
  }

  public void invalidateLastCache(final PartialPath devicePath, final String measurement) {
    final ToIntFunction<TableDeviceCacheEntry> updateFunction =
        PathPatternUtil.hasWildcard(measurement)
            ? entry -> -entry.invalidateLastCache()
            : entry -> -entry.invalidateLastCache(measurement);
    final String[] nodes = devicePath.getNodes();

    if (!devicePath.hasWildcard()) {
      final IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(
              StringArrayDeviceID.splitDeviceIdString(devicePath.getNodes()));
      dualKeyCache.update(
          new TableId(nodes[1], deviceID.getTableName()), deviceID, null, updateFunction, false);
    } else {
      // This may take quite a long time to perform, yet it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      final Predicate<TableId> firstKeyChecker =
          PathPatternUtil.hasWildcard(nodes[1])
              ? tableId -> true
              : tableId -> tableId.belongTo(nodes[1]);
      dualKeyCache.update(
          firstKeyChecker,
          cachedDeviceID -> {
            try {
              return new PartialPath(cachedDeviceID).matchFullPath(devicePath);
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal deviceID {} found in cache when invalidating by path {}, invalidate it anyway",
                  cachedDeviceID,
                  devicePath);
              return true;
            }
          },
          updateFunction);
    }
  }

  public void invalidateCache(final PartialPath devicePath, final String measurement) {
    final String[] nodes = devicePath.getNodes();

    if (!devicePath.hasWildcard()) {
      final IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(
              StringArrayDeviceID.splitDeviceIdString(devicePath.getNodes()));
      dualKeyCache.invalidate(
          new TableId(nodes[1], deviceID.getTableName()), deviceID, null, updateFunction, false);
    } else {
      // This may take quite a long time to perform, yet it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      final Predicate<TableId> firstKeyChecker =
          PathPatternUtil.hasWildcard(nodes[1])
              ? tableId -> true
              : tableId -> tableId.belongTo(nodes[1]);
      dualKeyCache.invalidate(
          firstKeyChecker,
          cachedDeviceID -> {
            try {
              return new PartialPath(cachedDeviceID).matchFullPath(devicePath);
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal deviceID {} found in cache when invalidating by path {}, invalidate it anyway",
                  cachedDeviceID,
                  devicePath);
              return true;
            }
          });
    }
  }

  /////////////////////////////// Management  ///////////////////////////////

  public long getHitCount() {
    return dualKeyCache.stats().hitCount();
  }

  public long getRequestCount() {
    return dualKeyCache.stats().requestCount();
  }

  public void invalidate(final String database) {
    readWriteLock.writeLock().lock();
    try {
      if (!database.contains(".")) {
        dualKeyCache.invalidate(tableId -> tableId.belongTo(database), deviceID -> true);
      } else {
        // Multi-layered database in tree model
        final String prefix = database.substring(0, database.indexOf("."));
        dualKeyCache.invalidate(
            tableId -> tableId.belongTo(prefix), deviceID -> deviceID.matchDatabaseName(database));
      }
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

  public void invalidateAll() {
    dualKeyCache.invalidateAll();
  }
}
