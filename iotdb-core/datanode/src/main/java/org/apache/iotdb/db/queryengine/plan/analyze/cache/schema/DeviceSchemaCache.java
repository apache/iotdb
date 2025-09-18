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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * The {@link DeviceSchemaCache} caches some of the devices and their: measurement info / template
 * info. The last value of one device is also cached here.
 */
@ThreadSafe
public class DeviceSchemaCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DeviceSchemaCache.class);

  /**
   * Leading_Segment, {@link IDeviceID}, Map{@literal <}Measurement, Schema{@literal
   * >}/templateInfo{@literal >}
   *
   * <p>The segment is used to:
   *
   * <p>1. Keep abreast of the newer versions.
   *
   * <p>2. Optimize the speed in invalidation by databases for most scenarios.
   */
  private final IDualKeyCache<String, PartialPath, DeviceCacheEntry> dualKeyCache;

  private final Map<String, String> databasePool = new ConcurrentHashMap<>();

  private final ReentrantLock lock = new ReentrantLock(false);

  DeviceSchemaCache() {
    dualKeyCache =
        new DualKeyCacheBuilder<String, PartialPath, DeviceCacheEntry>()
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(segment -> (int) RamUsageEstimator.sizeOf(segment))
            .secondKeySizeComputer(PartialPath::estimateSize)
            .valueSizeComputer(DeviceCacheEntry::estimateSize)
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
    final DeviceCacheEntry entry = dualKeyCache.get(getLeadingSegment(device), device);
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
    final String databaseToUse = databasePool.computeIfAbsent(database, k -> database);

    dualKeyCache.update(
        getLeadingSegment(devicePath),
        devicePath,
        new DeviceCacheEntry(),
        entry -> entry.setDeviceSchema(databaseToUse, deviceSchemaInfo),
        true);
  }

  public IDeviceSchema getDeviceSchema(final PartialPath device) {
    final DeviceCacheEntry entry = dualKeyCache.get(getLeadingSegment(device), device);
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
    final String database2Use = databasePool.computeIfAbsent(database, k -> database);

    dualKeyCache.update(
        getLeadingSegment(device),
        device,
        new DeviceCacheEntry(),
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
    return dualKeyCache.batchApply(inputMap, DeviceCacheEntry::updateInputMap);
  }

  void invalidateLastCache(final PartialPath devicePath, final String measurement) {
    final ToIntFunction<DeviceCacheEntry> updateFunction =
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
          cachedDeviceID ->
              isMultiLevelWildcardMeasurement
                  ? devicePath.matchPrefixPath(cachedDeviceID)
                  : devicePath.matchFullPath(cachedDeviceID));
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
    Predicate<PartialPath> secondKeyChecker;
    try {
      final PartialPath databasePath = new PartialPath(database);
      secondKeyChecker = device -> device.matchPrefixPath(databasePath);
    } catch (final Exception ignored) {
      secondKeyChecker = device -> device.startsWith(database);
    }

    lock.lock();
    try {
      dualKeyCache.update(
          segment -> segment.startsWith(database),
          device -> true,
          entry -> -entry.invalidateLastCache());
      dualKeyCache.update(
          database::startsWith, secondKeyChecker, entry -> -entry.invalidateLastCache());
    } finally {
      lock.unlock();
    }
  }

  // Note: It might be very slow if the database is too long
  public void invalidate(final @Nonnull String database) {
    lock.lock();
    try {
      dualKeyCache.invalidate(segment -> segment.startsWith(database), device -> true);
      dualKeyCache.invalidate(database::startsWith, device -> device.startsWith(database));
    } finally {
      lock.unlock();
    }
  }

  public void invalidateLastCache() {
    lock.lock();
    try {
      dualKeyCache.update(segment -> true, device -> true, entry -> -entry.invalidateLastCache());
    } finally {
      lock.unlock();
    }
  }

  public void invalidateTreeSchema() {
    lock.lock();
    try {
      dualKeyCache.update(segment -> true, device -> true, entry -> -entry.invalidateSchema());
    } finally {
      lock.unlock();
    }
  }

  public void invalidateAll() {
    lock.lock();
    try {
      dualKeyCache.invalidateAll();
    } finally {
      lock.unlock();
    }
  }

  // Utils of leading segment

  public static String getLeadingSegment(final PartialPath device) {
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
      segment = deviceStr;
    } else {
      segment = deviceStr.substring(0, lastSeparatorPos);
    }

    return segment;
  }
}
