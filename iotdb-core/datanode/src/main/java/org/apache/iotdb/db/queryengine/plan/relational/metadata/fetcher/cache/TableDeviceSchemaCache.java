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
import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * The {@link TableDeviceSchemaCache} caches some of the devices and their: attributes of tables /
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
public class TableDeviceSchemaCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TableDeviceSchemaCache.class);

  /**
   * In table model: {@literal <}{@link QualifiedObjectName}, {@link IDeviceID}, lastCache /
   * attributes{@literal >}
   *
   * <p>In tree model: {@literal <}Pair{@literal <}{@code null}, tableName(translated){@literal >},
   * {@link IDeviceID}(translated), Map{@literal <}Measurement, Schema{@literal
   * >}/templateInfo{@literal >}
   */
  private final IDualKeyCache<TableId, IDeviceID, TableDeviceCacheEntry> dualKeyCache;

  private final Map<String, String> treeModelDatabasePool = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private TableDeviceSchemaCache() {
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

  public static TableDeviceSchemaCache getInstance() {
    return TableDeviceSchemaCacheHolder.INSTANCE;
  }

  private static class TableDeviceSchemaCacheHolder {
    private static final TableDeviceSchemaCache INSTANCE = new TableDeviceSchemaCache();
  }

  /////////////////////////////// Attribute ///////////////////////////////

  // The input deviceId shall have its tailing nulls trimmed
  public Map<String, Binary> getDeviceAttribute(final String database, final IDeviceID deviceId) {
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
      final String database, final IDeviceID deviceId, final Map<String, Binary> attributeMap) {
    readWriteLock.readLock().lock();
    try {
      // Avoid stale table
      if (Objects.isNull(
          DataNodeTableCache.getInstance().getTable(database, deviceId.getTableName()))) {
        return;
      }
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
      final String database, final IDeviceID deviceId, final Map<String, Binary> attributeMap) {
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
   * @param deviceId {@link IDeviceID}
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
   * Update the last cache on query or data recover.
   *
   * <p>Note: The query shall put the cache twice:
   *
   * <p>- First time put the {@link TimeValuePair} array as {@code null} before the query accesses
   * data. It does not indicate that the measurements are all {@code null}s, just to allow the
   * writing to update the cache, then avoid that the query put a stale value to cache and break the
   * consistency. WARNING: The writing may temporarily put a stale value in cache if a stale value
   * is written, but it won't affect the eventual consistency.
   *
   * <p>- Second time put the calculated {@link TimeValuePair}s, and use {@link
   * #updateLastCacheIfExists(String, IDeviceID, String[], TimeValuePair[])}. The input {@link
   * TimeValuePair}s shall never be or contain {@code null}, if a measurement is with all {@code
   * null}s, its {@link TimeValuePair} shall be {@link TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}.
   * For time column, the input measurement shall be "", and the value shall be {@link
   * TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE}. If the time column is not explicitly specified, the
   * device's last time won't be updated because we cannot guarantee the completeness of the
   * existing measurements in cache.
   *
   * <p>If the query has ended abnormally, it shall call this to invalidate the entry it has pushed
   * in the first time, to avoid the stale writing damaging the eventual consistency. The input
   * {@link TimeValuePair}s shall be all {@code null}s in this case.
   *
   * @param database the device's database, without "root"
   * @param deviceId {@link IDeviceID}
   * @param measurements the fetched measurements
   * @param isInvalidate whether to init or invalidate the cache
   */
  public void initOrInvalidateLastCache(
      final String database,
      final IDeviceID deviceId,
      final String[] measurements,
      final boolean isInvalidate) {
    readWriteLock.readLock().lock();
    try {
      // Avoid stale table
      if (Objects.isNull(
          DataNodeTableCache.getInstance().getTable(database, deviceId.getTableName()))) {
        return;
      }
      dualKeyCache.update(
          new TableId(database, deviceId.getTableName()),
          deviceId,
          new TableDeviceCacheEntry(),
          entry ->
              entry.initOrInvalidateLastCache(
                  database, deviceId.getTableName(), measurements, isInvalidate, true),
          !isInvalidate);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Update the last cache in writing or the second push of last cache query. If a measurement is
   * with all {@code null}s or is an id/attribute column, its {@link TimeValuePair}[] shall be
   * {@code null}. For correctness, this will put the cache lazily and only update the existing last
   * caches of measurements.
   *
   * @param database the device's database, without "root"
   * @param deviceId {@link IDeviceID}
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCacheIfExists(
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
   * Get the last {@link TimeValuePair} of a measurement, the measurement shall never be "time".
   *
   * @param database the device's database, without "root", {@code null} for tree model
   * @param deviceId {@link IDeviceID}
   * @param measurement the measurement to get
   * @return {@code null} iff cache miss, {@link TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR} iff
   *     cache hit but result is {@code null}, and the result value otherwise.
   */
  public TimeValuePair getLastEntry(
      final @Nullable String database, final IDeviceID deviceId, final String measurement) {
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
   * @param deviceId {@link IDeviceID}
   * @param sourceMeasurement the measurement to get
   * @return {@code Optional.empty()} iff the last cache is miss at all; Or the optional of a pair,
   *     the {@link Pair#left} will be the source measurement's last time, (OptionalLong.empty() iff
   *     the source measurement is all {@code null}); {@link Pair#right} will be an {@link
   *     TsPrimitiveType} array, whose element will be {@code null} if cache miss, {@link
   *     TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE} iff cache hit and the measurement is without any
   *     values when last by the source measurement's time, and the result value otherwise.
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

  // Shall be accessed through "TreeDeviceSchemaCacheManager"

  void putDeviceSchema(final String database, final DeviceSchemaInfo deviceSchemaInfo) {
    final PartialPath devicePath = deviceSchemaInfo.getDevicePath();
    final IDeviceID deviceID = devicePath.getIDeviceID();
    final String previousDatabase = treeModelDatabasePool.putIfAbsent(database, database);

    dualKeyCache.update(
        new TableId(null, deviceID.getTableName()),
        deviceID,
        new TableDeviceCacheEntry(),
        entry ->
            entry.setDeviceSchema(
                Objects.nonNull(previousDatabase) ? previousDatabase : database, deviceSchemaInfo),
        true);
  }

  IDeviceSchema getDeviceSchema(final String[] devicePath) {
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(devicePath));
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
    final String previousDatabase = treeModelDatabasePool.putIfAbsent(database, database);
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
                        database,
                        deviceID.getTableName(),
                        measurements,
                        Objects.nonNull(timeValuePairs),
                        false)
            : entry ->
                entry.setMeasurementSchema(
                        database2Use, isAligned, measurements, measurementSchemas)
                    + entry.tryUpdateLastCache(measurements, timeValuePairs),
        Objects.isNull(timeValuePairs));
  }

  // WARNING: This is not guaranteed to affect table model's cache
  void invalidateLastCache(final PartialPath devicePath, final String measurement) {
    final ToIntFunction<TableDeviceCacheEntry> updateFunction =
        PathPatternUtil.hasWildcard(measurement)
            ? entry -> -entry.invalidateLastCache()
            : entry -> -entry.invalidateLastCache(measurement, false);

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

  // This database is with "root"
  void invalidateLastCache(final @Nonnull String qualifiedDatabase) {
    final String database = PathUtils.unQualifyDatabaseName(qualifiedDatabase);
    readWriteLock.writeLock().lock();

    try {
      dualKeyCache.update(
          tableId ->
              tableId.belongTo(database)
                  || Objects.isNull(tableId.getDatabase())
                      && tableId.getTableName().startsWith(qualifiedDatabase),
          deviceID -> true,
          entry -> -entry.invalidateLastCache());
      dualKeyCache.update(
          tableId ->
              Objects.isNull(tableId.getDatabase())
                  && qualifiedDatabase.startsWith(tableId.getTableName()),
          deviceID -> deviceID.matchDatabaseName(qualifiedDatabase),
          entry -> -entry.invalidateLastCache());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // This database is without "root"
  public void invalidate(final @Nonnull String database) {
    final String qualifiedDatabase = PathUtils.qualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidate(
          tableId ->
              tableId.belongTo(database)
                  || Objects.isNull(tableId.getDatabase())
                      && tableId.getTableName().startsWith(qualifiedDatabase),
          deviceID -> true);
      dualKeyCache.invalidate(
          tableId ->
              Objects.isNull(tableId.getDatabase())
                  && qualifiedDatabase.startsWith(tableId.getTableName()),
          deviceID -> deviceID.matchDatabaseName(qualifiedDatabase));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // Only used by table model
  public void invalidate(final String database, final String tableName) {
    readWriteLock.writeLock().lock();
    try {
      // Table cache's invalidate must be guarded by this lock
      DataNodeTableCache.getInstance().invalid(database, tableName);
      dualKeyCache.invalidate(new TableId(database, tableName));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // The fuzzy filters are not considered because:
  // 1. We can actually invalidate more cache entries than we need.
  // 2. Constructing the filterOperators may require some time and complication
  // 3. The fuzzy filters may contain attributes, which may not exist or be stale
  public void invalidate(
      final String database, final String tableName, final List<PartialPath> patterns) {
    readWriteLock.writeLock().lock();
    try {
      final TableId firstKey = new TableId(database, tableName);
      if (patterns.isEmpty()) {
        dualKeyCache.invalidate(firstKey);
      } else {
        final List<PartialPath> multiMatchList =
            patterns.stream()
                .filter(
                    idFilter -> {
                      if (!idFilter.hasWildcard()) {
                        final IDeviceID deviceId =
                            IDeviceID.Factory.DEFAULT_FACTORY.create(
                                Arrays.copyOfRange(
                                    idFilter.getNodes(), 2, idFilter.getNodeLength()));
                        dualKeyCache.invalidate(firstKey, deviceId);
                        return false;
                      }
                      return true;
                    })
                .collect(Collectors.toList());

        dualKeyCache.invalidate(
            firstKey,
            deviceId -> {
              final String[] segments = (String[]) deviceId.getSegments();
              for (int i = 1; i < segments.length; ++i) {
                for (final PartialPath path : multiMatchList) {
                  final int pathIndex = i + 2;
                  if (path.getNodes()[pathIndex].equals(segments[i])
                      || path.getNodes()[pathIndex].equals(ONE_LEVEL_PATH_WILDCARD)
                          && ((ExtendedPartialPath) path).match(pathIndex, segments[i])) {
                    return true;
                  }
                }
              }
              return false;
            });
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidate(
      final String database,
      final String tableName,
      final String columnName,
      final boolean isAttributeColumn) {
    readWriteLock.writeLock().lock();
    try {
      // Table cache's invalidate must be guarded by this lock
      DataNodeTableCache.getInstance().invalid(database, tableName, columnName);
      final ToIntFunction<TableDeviceCacheEntry> updateFunction =
          isAttributeColumn
              ? entry -> entry.invalidateAttributeColumn(columnName)
              : entry -> entry.invalidateLastCache(columnName, true);
      dualKeyCache.update(new TableId(null, tableName), deviceID -> true, updateFunction);
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

  public void invalidateAttributeCache() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(tableId -> true, deviceID -> true, entry -> -entry.invalidateAttribute());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateTreeSchema() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(
          tableId -> true, deviceID -> true, entry -> -entry.invalidateTreeSchema());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateAll() {
    dualKeyCache.invalidateAll();
  }
}
