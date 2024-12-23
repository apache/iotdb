/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileAnalyzeSchemaMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadTsFileTreeSchemaCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileTreeSchemaCache.class);

  private static final int BATCH_FLUSH_TIME_SERIES_NUMBER;
  private static final int MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX;
  private static final long ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;
  private static final long FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES;

  static {
    final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    BATCH_FLUSH_TIME_SERIES_NUMBER = CONFIG.getLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber();
    MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX =
        CONFIG.getLoadTsFileMaxDeviceCountToUseDeviceTimeIndex();
    ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES =
        CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes() <= 0
            ? ((long) BATCH_FLUSH_TIME_SERIES_NUMBER) << 10
            : CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes();
    FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES = ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES >> 1;
  }

  private final LoadTsFileAnalyzeSchemaMemoryBlock block;

  private Map<IDeviceID, Set<MeasurementSchema>> currentBatchDevice2TimeSeriesSchemas;
  private Map<IDeviceID, Boolean> tsFileDevice2IsAligned;
  private Set<PartialPath> alreadySetDatabases;

  private Collection<ModEntry> currentModifications;
  private ITimeIndex currentTimeIndex;

  private long batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes = 0;
  private long tsFileDevice2IsAlignedMemoryUsageSizeInBytes = 0;
  private long alreadySetDatabasesMemoryUsageSizeInBytes = 0;
  private long currentModificationsMemoryUsageSizeInBytes = 0;
  private long currentTimeIndexMemoryUsageSizeInBytes = 0;

  private int currentBatchTimeSeriesCount = 0;

  public LoadTsFileTreeSchemaCache() throws LoadRuntimeOutOfMemoryException {
    this.block =
        LoadTsFileMemoryManager.getInstance()
            .allocateAnalyzeSchemaMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
    this.currentBatchDevice2TimeSeriesSchemas = new HashMap<>();
    this.tsFileDevice2IsAligned = new HashMap<>();
    this.alreadySetDatabases = new HashSet<>();
    this.currentModifications = new ArrayList<>();
  }

  public Map<IDeviceID, Set<MeasurementSchema>> getDevice2TimeSeries() {
    return currentBatchDevice2TimeSeriesSchemas;
  }

  public boolean getDeviceIsAligned(IDeviceID device) {
    if (!tsFileDevice2IsAligned.containsKey(device)) {
      LOGGER.warn(
          "Device {} is not in the tsFileDevice2IsAligned cache {}.",
          device,
          tsFileDevice2IsAligned);
    }
    return tsFileDevice2IsAligned.get(device);
  }

  public Set<PartialPath> getAlreadySetDatabases() {
    return alreadySetDatabases;
  }

  public void addTimeSeries(IDeviceID device, MeasurementSchema measurementSchema) {
    long memoryUsageSizeInBytes = 0;
    if (!currentBatchDevice2TimeSeriesSchemas.containsKey(device)) {
      memoryUsageSizeInBytes += device.ramBytesUsed();
    }
    if (currentBatchDevice2TimeSeriesSchemas
        .computeIfAbsent(device, k -> new HashSet<>())
        .add(measurementSchema)) {
      memoryUsageSizeInBytes += measurementSchema.serializedSize();
      currentBatchTimeSeriesCount++;
    }

    if (memoryUsageSizeInBytes > 0) {
      batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
      block.addMemoryUsage(memoryUsageSizeInBytes);
    }
  }

  public void addIsAlignedCache(IDeviceID device, boolean isAligned, boolean addIfAbsent) {
    long memoryUsageSizeInBytes = 0;
    if (!tsFileDevice2IsAligned.containsKey(device)) {
      memoryUsageSizeInBytes += device.ramBytesUsed();
    }
    if (addIfAbsent
        ? (tsFileDevice2IsAligned.putIfAbsent(device, isAligned) == null)
        : (tsFileDevice2IsAligned.put(device, isAligned) == null)) {
      memoryUsageSizeInBytes += Byte.BYTES;
    }

    if (memoryUsageSizeInBytes > 0) {
      tsFileDevice2IsAlignedMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
      block.addMemoryUsage(memoryUsageSizeInBytes);
    }
  }

  public void setCurrentModificationsAndTimeIndex(
      TsFileResource resource, TsFileSequenceReader reader) throws IOException {
    clearModificationsAndTimeIndex();

    currentModifications = resource.getAllModEntries();
    for (final ModEntry modification : currentModifications) {
      currentModificationsMemoryUsageSizeInBytes += modification.serializedSize();
    }

    // If there are too many modifications, a larger memory block is needed to avoid frequent
    // flush.
    long newMemorySize =
        currentModificationsMemoryUsageSizeInBytes > ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES / 2
            ? currentModificationsMemoryUsageSizeInBytes + ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES
            : ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;
    block.forceResize(newMemorySize);
    block.addMemoryUsage(currentModificationsMemoryUsageSizeInBytes);

    // No need to build device time index if there are no modifications
    if (!currentModifications.isEmpty() && resource.resourceFileExists()) {
      final AtomicInteger deviceCount = new AtomicInteger();
      reader
          .getAllDevicesIteratorWithIsAligned()
          .forEachRemaining(o -> deviceCount.getAndIncrement());

      // Use device time index only if the device count is less than the threshold, avoiding too
      // much memory usage
      if (deviceCount.get() < MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX) {
        currentTimeIndex = resource.getTimeIndex();
        if (currentTimeIndex instanceof FileTimeIndex) {
          currentTimeIndex = resource.buildDeviceTimeIndex();
        }
        currentTimeIndexMemoryUsageSizeInBytes = currentTimeIndex.calculateRamSize();
        block.addMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
      }
    }
  }

  public boolean isDeviceDeletedByMods(IDeviceID device) throws IllegalPathException {
    return currentTimeIndex != null
        && ModificationUtils.isAllDeletedByMods(
            currentModifications,
            device,
            currentTimeIndex.getStartTime(device),
            currentTimeIndex.getEndTime(device));
  }

  public boolean isTimeseriesDeletedByMods(IDeviceID device, TimeseriesMetadata timeseriesMetadata)
      throws IllegalPathException {
    return ModificationUtils.isTimeseriesDeletedByMods(
        currentModifications,
        device,
        timeseriesMetadata.getMeasurementId(),
        timeseriesMetadata.getStatistics().getStartTime(),
        timeseriesMetadata.getStatistics().getEndTime());
  }

  public void addAlreadySetDatabase(PartialPath database) {
    long memoryUsageSizeInBytes = 0;
    if (alreadySetDatabases.add(database)) {
      memoryUsageSizeInBytes += PartialPath.estimateSize(database);
    }

    if (memoryUsageSizeInBytes > 0) {
      alreadySetDatabasesMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
      block.addMemoryUsage(memoryUsageSizeInBytes);
    }
  }

  public boolean shouldFlushTimeSeries() {
    return !block.hasEnoughMemory()
        || currentBatchTimeSeriesCount >= BATCH_FLUSH_TIME_SERIES_NUMBER;
  }

  public boolean shouldFlushAlignedCache() {
    return tsFileDevice2IsAlignedMemoryUsageSizeInBytes >= FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES;
  }

  public void clearTimeSeries() {
    currentBatchDevice2TimeSeriesSchemas.clear();
    block.reduceMemoryUsage(batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes);
    batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes = 0;
    currentBatchTimeSeriesCount = 0;
  }

  public void clearModificationsAndTimeIndex() {
    currentModifications.clear();
    currentTimeIndex = null;
    block.reduceMemoryUsage(currentModificationsMemoryUsageSizeInBytes);
    block.reduceMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
    currentModificationsMemoryUsageSizeInBytes = 0;
    currentTimeIndexMemoryUsageSizeInBytes = 0;
  }

  public void clearAlignedCache() {
    tsFileDevice2IsAligned.clear();
    block.reduceMemoryUsage(tsFileDevice2IsAlignedMemoryUsageSizeInBytes);
    tsFileDevice2IsAlignedMemoryUsageSizeInBytes = 0;
  }

  public void clearDeviceIsAlignedCacheIfNecessary() {
    if (!shouldFlushAlignedCache()) {
      return;
    }

    long releaseMemoryInBytes = 0;
    final Set<IDeviceID> timeSeriesCacheKeySet =
        new HashSet<>(currentBatchDevice2TimeSeriesSchemas.keySet());
    Iterator<Map.Entry<IDeviceID, Boolean>> iterator = tsFileDevice2IsAligned.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<IDeviceID, Boolean> entry = iterator.next();
      if (!timeSeriesCacheKeySet.contains(entry.getKey())) {
        releaseMemoryInBytes += entry.getKey().ramBytesUsed() + Byte.BYTES;
        iterator.remove();
      }
    }
    if (releaseMemoryInBytes > 0) {
      tsFileDevice2IsAlignedMemoryUsageSizeInBytes -= releaseMemoryInBytes;
      block.reduceMemoryUsage(releaseMemoryInBytes);
    }
  }

  private void clearDatabasesCache() {
    alreadySetDatabases.clear();
    block.reduceMemoryUsage(alreadySetDatabasesMemoryUsageSizeInBytes);
    alreadySetDatabasesMemoryUsageSizeInBytes = 0;
  }

  public void close() {
    clearTimeSeries();
    clearModificationsAndTimeIndex();
    clearAlignedCache();
    clearDatabasesCache();

    block.close();

    currentBatchDevice2TimeSeriesSchemas = null;
    tsFileDevice2IsAligned = null;
    alreadySetDatabases = null;
  }
}
