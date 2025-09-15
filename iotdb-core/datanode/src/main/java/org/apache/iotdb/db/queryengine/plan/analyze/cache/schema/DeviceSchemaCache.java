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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToIntFunction;

/**
 * The {@link DeviceSchemaCache} caches some of the devices and their: attributes of tables /
 * measurement info / template info. The last value of one device is also cached here. Here are the
 * semantics of attributes, other semantics are omitted:
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
public class DeviceSchemaCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DeviceSchemaCache.class);

  /**
   * In table model: {@literal <}{@link QualifiedObjectName}, {@link IDeviceID}, lastCache /
   * attributes{@literal >}
   *
   * <p>In tree model: {@literal <}Pair{@literal <}{@code null}, tableName(translated){@literal >},
   * {@link IDeviceID}(translated), Map{@literal <}Measurement, Schema{@literal
   * >}/templateInfo{@literal >}
   */
  private final IDualKeyCache<TableId, IDeviceID, TableDeviceCacheEntry> dualKeyCache;

  private final Map<String, String> databasePool = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  DeviceSchemaCache() {
    dualKeyCache =
        new DualKeyCacheBuilder<TableId, IDeviceID, TableDeviceCacheEntry>()
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(TableId::estimateSize)
            .secondKeySizeComputer(deviceID -> (int) deviceID.ramBytesUsed())
            .valueSizeComputer(TableDeviceCacheEntry::estimateSize)
            .build();
    MetricService.getInstance().addMetricSet(new DataNodeSchemaCacheMetrics(this));
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  /**
   * Get the last {@link TimeValuePair} of a measurement, the measurement shall never be "time".
   *
   * @param database the device's database, without "root", {@code null} for tree model
   * @param deviceId {@link IDeviceID}
   * @param measurement the measurement to get
   * @return {@code null} iff cache miss, {@link DeviceLastCache#EMPTY_TIME_VALUE_PAIR} iff cache
   *     hit but result is {@code null}, and the result value otherwise.
   */
  public TimeValuePair getLastEntry(
      final @Nullable String database, final IDeviceID deviceId, final String measurement) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(database, deviceId.getTableName()), deviceId);
    return Objects.nonNull(entry) ? entry.getTimeValuePair(measurement) : null;
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

  public void putDeviceSchema(final String database, final DeviceSchemaInfo deviceSchemaInfo) {
    final PartialPath devicePath = deviceSchemaInfo.getDevicePath();
    final IDeviceID deviceID = devicePath.getIDeviceID();
    final String previousDatabase = databasePool.putIfAbsent(database, database);

    dualKeyCache.update(
        new TableId(null, deviceID.getTableName()),
        deviceID,
        new TableDeviceCacheEntry(),
        entry ->
            entry.setDeviceSchema(
                Objects.nonNull(previousDatabase) ? previousDatabase : database, deviceSchemaInfo),
        true);
  }

  public IDeviceSchema getDeviceSchema(final String[] devicePath) {
    return getDeviceSchema(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(devicePath)));
  }

  public IDeviceSchema getDeviceSchema(final IDeviceID deviceID) {
    final TableDeviceCacheEntry entry =
        dualKeyCache.get(new TableId(null, deviceID.getTableName()), deviceID);
    return Objects.nonNull(entry) ? entry.getDeviceSchema() : null;
  }

  void updateLastCache(
      final String database,
      final IDeviceID deviceID,
      final String[] measurements,
      final @Nullable TimeValuePair[] timeValuePairs,
      final boolean isAligned,
      final IMeasurementSchema[] measurementSchemas,
      final boolean initOrInvalidate) {
    final String previousDatabase = databasePool.putIfAbsent(database, database);
    final String database2Use = Objects.nonNull(previousDatabase) ? previousDatabase : database;

    dualKeyCache.update(
        new TableId(null, deviceID.getTableName()),
        deviceID,
        new TableDeviceCacheEntry(),
        initOrInvalidate
            ? entry ->
                entry.setMeasurementSchema(
                        database2Use, isAligned, measurements, measurementSchemas)
                    + entry.initOrInvalidateLastCache(
                        deviceID.getTableName(), measurements, Objects.nonNull(timeValuePairs))
            : entry ->
                entry.setMeasurementSchema(
                        database2Use, isAligned, measurements, measurementSchemas)
                    + entry.tryUpdateLastCache(measurements, timeValuePairs),
        Objects.isNull(timeValuePairs));
  }

  public boolean getLastCache(
      final Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> inputMap) {
    return dualKeyCache.batchApply(inputMap, TableDeviceCacheEntry::updateInputMap);
  }

  // WARNING: This is not guaranteed to affect table model's cache
  void invalidateLastCache(final PartialPath devicePath, final String measurement) {
    final ToIntFunction<TableDeviceCacheEntry> updateFunction =
        PathPatternUtil.hasWildcard(measurement)
            ? entry -> -entry.invalidateLastCache()
            : entry -> -entry.invalidateLastCache(measurement);

    if (!devicePath.hasWildcard()) {
      final IDeviceID deviceID = devicePath.getIDeviceID();
      dualKeyCache.update(
          new TableId(null, deviceID.getTableName()), deviceID, null, updateFunction, false);
    } else {
      // This may take quite a long time to perform, yet we assume that the "invalidateLastCache" is
      // only called by deletions, which has a low frequency; and it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      dualKeyCache.update(
          tableId -> {
            try {
              return devicePath.matchPrefixPath(new PartialPath(tableId.getTableName()));
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal tableID {} found in cache when invalidating by path {}, invalidate it anyway",
                  tableId.getTableName(),
                  devicePath);
              return true;
            }
          },
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

  // WARNING: This is not guaranteed to affect table model's cache
  void invalidateCache(
      final @Nonnull PartialPath devicePath, final boolean isMultiLevelWildcardMeasurement) {
    if (!devicePath.hasWildcard()) {
      final IDeviceID deviceID = devicePath.getIDeviceID();
      dualKeyCache.invalidate(new TableId(null, deviceID.getTableName()), deviceID);
    } else {
      // This may take quite a long time to perform, yet we assume that the "invalidateLastCache" is
      // only called by deletions, which has a low frequency; and it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      dualKeyCache.invalidate(
          tableId -> {
            try {
              return devicePath.matchPrefixPath(new PartialPath(tableId.getTableName()));
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal tableID {} found in cache when invalidating by path {}, invalidate it anyway",
                  tableId.getTableName(),
                  devicePath);
              return true;
            }
          },
          cachedDeviceID -> {
            try {
              return isMultiLevelWildcardMeasurement
                  ? devicePath.matchPrefixPath(new PartialPath(cachedDeviceID))
                  : devicePath.matchFullPath(new PartialPath(cachedDeviceID));
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

  long getHitCount() {
    return dualKeyCache.stats().hitCount();
  }

  long getRequestCount() {
    return dualKeyCache.stats().requestCount();
  }

  long getMemoryUsage() {
    return dualKeyCache.stats().memoryUsage();
  }

  long capacity() {
    return dualKeyCache.stats().capacity();
  }

  long entriesCount() {
    return dualKeyCache.stats().entriesCount();
  }

  void invalidateLastCache(final @Nonnull String database) {
    readWriteLock.writeLock().lock();

    try {
      dualKeyCache.update(
          tableId ->
              Objects.isNull(tableId.getDatabase()) && tableId.getTableName().startsWith(database),
          deviceID -> true,
          entry -> -entry.invalidateLastCache());
      dualKeyCache.update(
          tableId ->
              Objects.isNull(tableId.getDatabase()) && database.startsWith(tableId.getTableName()),
          deviceID -> deviceID.matchDatabaseName(database),
          entry -> -entry.invalidateLastCache());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidate(final @Nonnull String database) {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidate(
          tableId ->
              Objects.isNull(tableId.getDatabase()) && tableId.getTableName().startsWith(database),
          deviceID -> true);
      dualKeyCache.invalidate(
          tableId ->
              Objects.isNull(tableId.getDatabase()) && database.startsWith(tableId.getTableName()),
          deviceID -> deviceID.matchDatabaseName(database));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateLastCache() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(tableId -> true, deviceID -> true, entry -> -entry.invalidateLastCache());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateTreeSchema() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(tableId -> true, deviceID -> true, entry -> -entry.invalidateSchema());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateAll() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidateAll();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }
}
