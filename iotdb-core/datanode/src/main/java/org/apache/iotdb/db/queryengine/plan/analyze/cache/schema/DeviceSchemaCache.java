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
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.RamUsageEstimator;
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
 * <p>1. If a device misses cache, it does not necessarily mean that the device does not exist,
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
   * Leading_Segment, {@link IDeviceID}, Map{@literal <}Measurement, Schema{@literal
   * >}/templateInfo{@literal >}
   */
  private final IDualKeyCache<String, PartialPath, TableDeviceCacheEntry> dualKeyCache;

  private final Map<String, String> databasePool = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  DeviceSchemaCache() {
    dualKeyCache =
        new DualKeyCacheBuilder<String, PartialPath, TableDeviceCacheEntry>()
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(segment -> (int) RamUsageEstimator.sizeOf(segment))
            .secondKeySizeComputer(PartialPath::estimateSize)
            .valueSizeComputer(TableDeviceCacheEntry::estimateSize)
            .build();
    MetricService.getInstance().addMetricSet(new DataNodeSchemaCacheMetrics(this));
  }

  /////////////////////////////// Last Cache ///////////////////////////////

  /**
   * Get the last {@link TimeValuePair} of a measurement, the measurement shall never be "time".
   *
   * @param device {@link IDeviceID}
   * @param measurement the measurement to get
   * @return {@code null} iff cache miss, {@link DeviceLastCache#EMPTY_TIME_VALUE_PAIR} iff cache
   *     hit but result is {@code null}, and the result value otherwise.
   */
  public TimeValuePair getLastEntry(final PartialPath device, final String measurement) {
    final TableDeviceCacheEntry entry = dualKeyCache.get(getLeadingSegment(device), device);
    return Objects.nonNull(entry) ? entry.getTimeValuePair(measurement) : null;
  }

  /**
   * Invalidate the last cache of one device.
   *
   * @param device IDeviceID
   */
  public void invalidateDeviceLastCache(final PartialPath device) {
    dualKeyCache.update(
        getLeadingSegment(device), device, null, entry -> -entry.invalidateLastCache(), false);
  }

  /////////////////////////////// Tree model ///////////////////////////////

  public void putDeviceSchema(final String database, final DeviceSchemaInfo deviceSchemaInfo) {
    final PartialPath devicePath = deviceSchemaInfo.getDevicePath();
    final String previousDatabase = databasePool.putIfAbsent(database, database);

    dualKeyCache.update(
        getLeadingSegment(devicePath),
        devicePath,
        new TableDeviceCacheEntry(),
        entry ->
            entry.setDeviceSchema(
                Objects.nonNull(previousDatabase) ? previousDatabase : database, deviceSchemaInfo),
        true);
  }

  public IDeviceSchema getDeviceSchema(final PartialPath device) {
    final TableDeviceCacheEntry entry = dualKeyCache.get(getLeadingSegment(device), device);
    return Objects.nonNull(entry) ? entry.getDeviceSchema() : null;
  }

  void updateLastCache(
      final String database,
      final PartialPath device,
      final String[] measurements,
      final @Nullable TimeValuePair[] timeValuePairs,
      final boolean isAligned,
      final IMeasurementSchema[] measurementSchemas,
      final boolean initOrInvalidate) {
    final String previousDatabase = databasePool.putIfAbsent(database, database);
    final String database2Use = Objects.nonNull(previousDatabase) ? previousDatabase : database;

    dualKeyCache.update(
        getLeadingSegment(device),
        device,
        new TableDeviceCacheEntry(),
        initOrInvalidate
            ? entry ->
                entry.setMeasurementSchema(
                        database2Use, isAligned, measurements, measurementSchemas)
                    + entry.initOrInvalidateLastCache(measurements, Objects.nonNull(timeValuePairs))
            : entry ->
                entry.setMeasurementSchema(
                        database2Use, isAligned, measurements, measurementSchemas)
                    + entry.tryUpdateLastCache(measurements, timeValuePairs),
        Objects.isNull(timeValuePairs));
  }

  public boolean getLastCache(
      final Map<String, Map<PartialPath, Map<String, TimeValuePair>>> inputMap) {
    return dualKeyCache.batchApply(inputMap, TableDeviceCacheEntry::updateInputMap);
  }

  // WARNING: This is not guaranteed to affect table model's cache
  void invalidateLastCache(final PartialPath devicePath, final String measurement) {
    final ToIntFunction<TableDeviceCacheEntry> updateFunction =
        PathPatternUtil.hasWildcard(measurement)
            ? entry -> -entry.invalidateLastCache()
            : entry -> -entry.invalidateLastCache(measurement);

    if (!devicePath.hasWildcard()) {
      dualKeyCache.update(getLeadingSegment(devicePath), devicePath, null, updateFunction, false);
    } else {
      // This may take quite a long time to perform, yet we assume that the "invalidateLastCache" is
      // only called by deletions, which has a low frequency; and it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      dualKeyCache.update(
          segment -> {
            try {
              return devicePath.matchPrefixPath(new PartialPath(segment));
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal segmentID {} found in cache when invalidating by path {}, invalidate it anyway",
                  segment,
                  devicePath);
              return true;
            }
          },
          cachedDeviceID -> cachedDeviceID.matchFullPath(devicePath),
          updateFunction);
    }
  }

  // WARNING: This is not guaranteed to affect table model's cache
  void invalidateCache(
      final @Nonnull PartialPath devicePath, final boolean isMultiLevelWildcardMeasurement) {
    if (!devicePath.hasWildcard()) {
      dualKeyCache.invalidate(getLeadingSegment(devicePath), devicePath);
    } else {
      // This may take quite a long time to perform, yet we assume that the "invalidateLastCache" is
      // only called by deletions, which has a low frequency; and it has avoided that
      // the un-related paths being cleared, like "root.*.b.c.**" affects
      // "root.*.d.c.**", thereby lower the query performance.
      dualKeyCache.invalidate(
          segment -> {
            try {
              return devicePath.matchPrefixPath(new PartialPath(segment));
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal segmentID {} found in cache when invalidating by path {}, invalidate it anyway",
                  segment,
                  devicePath);
              return true;
            }
          },
          cachedDeviceID -> {
            try {
              return isMultiLevelWildcardMeasurement
                  ? devicePath.matchPrefixPath(cachedDeviceID)
                  : devicePath.matchFullPath(cachedDeviceID);
            } catch (final IllegalPathException e) {
              logger.warn(
                  "Illegal device {} found in cache when invalidating by path {}, invalidate it anyway",
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

  // Note: It might be very slow if the database is too long
  void invalidateLastCache(final @Nonnull String database) {
    readWriteLock.writeLock().lock();

    try {
      dualKeyCache.update(
          segment -> segment.startsWith(database),
          device -> true,
          entry -> -entry.invalidateLastCache());
      dualKeyCache.update(
          database::startsWith,
          device -> device.startsWith(database),
          entry -> -entry.invalidateLastCache());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // Note: It might be very slow if the database is too long
  public void invalidate(final @Nonnull String database) {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.invalidate(segment -> segment.startsWith(database), device -> true);
      dualKeyCache.invalidate(database::startsWith, device -> device.startsWith(database));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateLastCache() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(segment -> true, device -> true, entry -> -entry.invalidateLastCache());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateTreeSchema() {
    readWriteLock.writeLock().lock();
    try {
      dualKeyCache.update(segment -> true, device -> true, entry -> -entry.invalidateSchema());
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

  // Utils of leading segment

  private static String getLeadingSegment(final PartialPath device) {
    final String segment;
    int lastSeparatorPos = -1;
    int separatorNum = 0;

    final String deviceStr = device.getFullPath();
    for (int i = 0; i < deviceStr.length(); i++) {
      if (deviceStr.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        lastSeparatorPos = i;
        separatorNum++;
        if (separatorNum == 3) {
          break;
        }
      }
    }
    if (lastSeparatorPos == -1) {
      // not find even one separator, probably during a test, use the device as the tableName
      segment = deviceStr;
    } else {
      // use the first DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME segments or all segments but the last
      // one as the table name
      segment = deviceStr.substring(0, lastSeparatorPos);
    }

    return segment;
  }
}
