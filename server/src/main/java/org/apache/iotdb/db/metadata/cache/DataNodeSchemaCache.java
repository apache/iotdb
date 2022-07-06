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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * This class takes the responsibility of metadata cache management of all DataRegions under
 * StorageEngine
 */
public class DataNodeSchemaCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<PartialPath, SchemaCacheEntry> cache;

  private DataNodeSchemaCache() {
    cache =
        Caffeine.newBuilder()
            .maximumWeight(config.getAllocateMemoryForSchemaCache())
            .weigher(
                (PartialPath key, SchemaCacheEntry value) ->
                    PartialPath.estimateSize(key) + SchemaCacheEntry.estimateSize(value))
            .build();
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      // add metrics
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.CACHE_HIT.toString(),
              MetricLevel.IMPORTANT,
              cache,
              l -> (long) (l.stats().hitRate() * 100),
              Tag.NAME.toString(),
              "schemaCache");
    }
  }

  public static DataNodeSchemaCache getInstance() {
    return DataNodeSchemaCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeSchemaCacheHolder {
    private static final DataNodeSchemaCache INSTANCE = new DataNodeSchemaCache();
  }

  /**
   * Get SchemaEntity info without auto create schema
   *
   * @param devicePath should not be measurementPath or AlignedPath
   * @param measurements
   * @return timeseries partialPath and its SchemaEntity
   */
  public SchemaTree get(PartialPath devicePath, String[] measurements) {
    SchemaTree schemaTree = new SchemaTree();
    SchemaCacheEntry schemaCacheEntry;
    for (String measurement : measurements) {
      PartialPath path = devicePath.concatNode(measurement);
      schemaCacheEntry = cache.getIfPresent(path);
      if (schemaCacheEntry != null) {
        schemaTree.appendSingleMeasurement(
            devicePath.concatNode(
                schemaCacheEntry.getSchemaEntryId()), // the cached path may be alias path
            schemaCacheEntry.getMeasurementSchema(),
            null,
            schemaCacheEntry.isAligned());
      }
    }
    return schemaTree;
  }

  public void put(SchemaTree schemaTree) {
    for (MeasurementPath measurementPath : schemaTree.getAllMeasurement()) {
      SchemaCacheEntry schemaCacheEntry =
          new SchemaCacheEntry(
              (MeasurementSchema) measurementPath.getMeasurementSchema(),
              measurementPath.isUnderAlignedEntity());
      cache.put(new PartialPath(measurementPath.getNodes()), schemaCacheEntry);
    }
  }

  public TimeValuePair getLastCache(PartialPath seriesPath) {
    SchemaCacheEntry entry = cache.getIfPresent(seriesPath);
    if (null == entry) {
      return null;
    }

    return DataNodeLastCacheManager.getLastCache(entry);
  }

  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    SchemaCacheEntry entry = cache.getIfPresent(seriesPath);
    if (null == entry) {
      return;
    }

    DataNodeLastCacheManager.updateLastCache(
        entry, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  public void resetLastCache(PartialPath seriesPath) {
    SchemaCacheEntry entry = cache.getIfPresent(seriesPath);
    if (null == entry) {
      return;
    }

    DataNodeLastCacheManager.resetLastCache(entry);
  }

  /**
   * For delete timeseries meatadata cache operation
   *
   * @param partialPath
   * @return
   */
  public void invalidate(PartialPath partialPath) {
    resetLastCache(partialPath);
    cache.invalidate(partialPath);
  }

  public long estimatedSize() {
    return cache.estimatedSize();
  }

  public void cleanUp() {
    cache.invalidateAll();
    cache.cleanUp();
  }
}
