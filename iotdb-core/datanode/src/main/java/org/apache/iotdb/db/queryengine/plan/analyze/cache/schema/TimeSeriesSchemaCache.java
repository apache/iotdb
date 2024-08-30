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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheUpdating;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.DataNodeLastCacheManager;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

public class TimeSeriesSchemaCache {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // <device, measurement, entry>
  private final IDualKeyCache<PartialPath, String, SchemaCacheEntry> dualKeyCache;

  TimeSeriesSchemaCache() {
    DualKeyCacheBuilder<PartialPath, String, SchemaCacheEntry> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(
                DualKeyCachePolicy.valueOf(config.getDataNodeSchemaCacheEvictionPolicy()))
            .memoryCapacity(config.getAllocateMemoryForSchemaCache())
            .firstKeySizeComputer(PartialPath::estimateSize)
            .secondKeySizeComputer(s -> 32 + 2 * s.length())
            .valueSizeComputer(SchemaCacheEntry::estimateSize)
            .build();
  }

  public void putSingleMeasurementPath(String storageGroup, MeasurementPath measurementPath) {
    SchemaCacheEntry schemaCacheEntry =
        new SchemaCacheEntry(
            storageGroup,
            measurementPath.getMeasurementSchema(),
            measurementPath.getTagMap(),
            measurementPath.isUnderAlignedEntity());
    dualKeyCache.put(
        measurementPath.getDevicePath(), measurementPath.getMeasurement(), schemaCacheEntry);
  }

  /** get SchemaCacheEntry and update last cache */
  @TestOnly
  public void updateLastCache(
      PartialPath devicePath,
      String measurement,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    SchemaCacheEntry entry = dualKeyCache.get(devicePath, measurement);
    if (null == entry) {
      return;
    }

    DataNodeLastCacheManager.updateLastCache(
        entry, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /** get SchemaCacheEntry and update last cache by device */
  public void updateLastCache(
      String database,
      PartialPath devicePath,
      String[] measurements,
      MeasurementSchema[] measurementSchemas,
      boolean isAligned,
      IntFunction<TimeValuePair> timeValuePairProvider,
      IntPredicate shouldUpdateProvider,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    SchemaCacheEntry entry;
    List<Integer> missingMeasurements = new ArrayList<>();
    dualKeyCache.update(
        new IDualKeyCacheUpdating<PartialPath, String, SchemaCacheEntry>() {
          @Override
          public PartialPath getFirstKey() {
            return devicePath;
          }

          @Override
          public String[] getSecondKeyList() {
            return measurements;
          }

          @Override
          public int updateValue(int index, SchemaCacheEntry value) {
            if (!shouldUpdateProvider.test(index)) {
              return 0;
            }
            if (value == null) {
              missingMeasurements.add(index);
              return 0;
            } else {
              return DataNodeLastCacheManager.updateLastCache(
                  value, timeValuePairProvider.apply(index), highPriorityUpdate, latestFlushedTime);
            }
          }
        });

    for (int index : missingMeasurements) {
      entry = dualKeyCache.get(devicePath, measurements[index]);
      if (entry == null) {
        synchronized (dualKeyCache) {
          entry = dualKeyCache.get(devicePath, measurements[index]);
          if (null == entry) {
            entry = new SchemaCacheEntry(database, measurementSchemas[index], null, isAligned);
            dualKeyCache.put(devicePath, measurements[index], entry);
          }
        }
      }
    }
    dualKeyCache.update(
        new IDualKeyCacheUpdating<PartialPath, String, SchemaCacheEntry>() {
          @Override
          public PartialPath getFirstKey() {
            return devicePath;
          }

          @Override
          public String[] getSecondKeyList() {
            return missingMeasurements.stream().map(i -> measurements[i]).toArray(String[]::new);
          }

          @Override
          public int updateValue(int index, SchemaCacheEntry value) {
            return DataNodeLastCacheManager.updateLastCache(
                value,
                timeValuePairProvider.apply(missingMeasurements.get(index)),
                highPriorityUpdate,
                latestFlushedTime);
          }
        });
  }

  /**
   * get or create SchemaCacheEntry and update last cache, only support non-aligned sensor or
   * aligned sensor without only one sub sensor
   */
  public void updateLastCache(
      String storageGroup,
      MeasurementPath measurementPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    PartialPath seriesPath = measurementPath.transformToPartialPath();
    SchemaCacheEntry entry =
        dualKeyCache.get(seriesPath.getDevicePath(), seriesPath.getMeasurement());
    if (null == entry) {
      synchronized (dualKeyCache) {
        entry = dualKeyCache.get(seriesPath.getDevicePath(), seriesPath.getMeasurement());
        if (null == entry) {
          entry =
              new SchemaCacheEntry(
                  measurementPath.getMeasurementSchema(), measurementPath.getTagMap());
          dualKeyCache.put(seriesPath.getDevicePath(), seriesPath.getMeasurement(), entry);
        }
      }
    }
    dualKeyCache.update(
        new IDualKeyCacheUpdating<PartialPath, String, SchemaCacheEntry>() {
          @Override
          public PartialPath getFirstKey() {
            return measurementPath.getDevicePath();
          }

          @Override
          public String[] getSecondKeyList() {
            return new String[] {measurementPath.getMeasurement()};
          }

          @Override
          public int updateValue(int index, SchemaCacheEntry value) {
            return DataNodeLastCacheManager.updateLastCache(
                value, timeValuePair, highPriorityUpdate, latestFlushedTime);
          }
        });
  }

  public void invalidateAll() {
    dualKeyCache.invalidateAll();
  }

  public void invalidate(String database) {
    dualKeyCache.invalidate(database);
  }

  public void invalidateLastCache(PartialPath path) {
    if (!path.hasWildcard()) {
      SchemaCacheEntry entry = dualKeyCache.get(path.getDevicePath(), path.getMeasurement());
      if (null == entry) {
        return;
      }
      dualKeyCache.update(
          new IDualKeyCacheUpdating<PartialPath, String, SchemaCacheEntry>() {
            @Override
            public PartialPath getFirstKey() {
              return path.getDevicePath();
            }

            @Override
            public String[] getSecondKeyList() {
              return new String[] {path.getMeasurement()};
            }

            @Override
            public int updateValue(int index, SchemaCacheEntry value) {
              return -DataNodeLastCacheManager.invalidateLastCache(value);
            }
          });
    } else {
      dualKeyCache.invalidateLastCache(path);
    }
  }

  public void invalidateDataRegionLastCache(String database) {
    dualKeyCache.invalidateDataRegionLastCache(database);
  }

  public void invalidate(List<? extends PartialPath> partialPathList) {
    dualKeyCache.invalidate(partialPathList);
  }

  public void cleanUp() {
    dualKeyCache.cleanUp();
  }
}
