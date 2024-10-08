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
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.view.InsertNonWritableViewException;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheUpdating;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.DataNodeLastCacheManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

public class TimeSeriesSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaCache.class);
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

  public long getHitCount() {
    return dualKeyCache.stats().hitCount();
  }

  public long getRequestCount() {
    return dualKeyCache.stats().requestCount();
  }

  /**
   * Get SchemaEntity info without auto create schema
   *
   * @param devicePath should not be measurementPath or AlignedPath
   * @param measurements
   * @return timeseries partialPath and its SchemaEntity
   */
  public ClusterSchemaTree get(PartialPath devicePath, String[] measurements) {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    Set<String> storageGroupSet = new HashSet<>();

    dualKeyCache.compute(
        new IDualKeyCacheComputation<PartialPath, String, SchemaCacheEntry>() {
          @Override
          public PartialPath getFirstKey() {
            return devicePath;
          }

          @Override
          public String[] getSecondKeyList() {
            return measurements;
          }

          @Override
          public void computeValue(int index, SchemaCacheEntry value) {
            if (value != null) {
              schemaTree.appendSingleMeasurement(
                  devicePath.concatAsMeasurementPath(value.getSchemaEntryId()),
                  value.getIMeasurementSchema(),
                  value.getTagMap(),
                  null,
                  null,
                  value.isAligned());
              storageGroupSet.add(value.getStorageGroup());
            }
          }
        });
    schemaTree.setDatabases(storageGroupSet);
    return schemaTree;
  }

  public ClusterSchemaTree get(PartialPath fullPath) {
    SchemaCacheEntry schemaCacheEntry =
        dualKeyCache.get(fullPath.getDevicePath(), fullPath.getMeasurement());
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    if (schemaCacheEntry != null) {
      schemaTree.appendSingleMeasurement(
          fullPath,
          schemaCacheEntry.getIMeasurementSchema(),
          schemaCacheEntry.getTagMap(),
          null,
          null,
          schemaCacheEntry.isAligned());
      schemaTree.setDatabases(Collections.singleton(schemaCacheEntry.getStorageGroup()));
    }
    return schemaTree;
  }

  public List<Integer> computeAndRecordLogicalView(ISchemaComputation schemaComputation) {
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    final AtomicBoolean isFirstNonViewMeasurement = new AtomicBoolean(true);
    dualKeyCache.compute(
        new IDualKeyCacheComputation<PartialPath, String, SchemaCacheEntry>() {
          @Override
          public PartialPath getFirstKey() {
            return schemaComputation.getDevicePath();
          }

          @Override
          public String[] getSecondKeyList() {
            return schemaComputation.getMeasurements();
          }

          @Override
          public void computeValue(int index, SchemaCacheEntry value) {
            if (value == null) {
              indexOfMissingMeasurements.add(index);
            } else {
              if (isFirstNonViewMeasurement.get() && (!value.isLogicalView())) {
                schemaComputation.computeDevice(value.isAligned());
                isFirstNonViewMeasurement.getAndSet(false);
              }
              schemaComputation.computeMeasurement(index, value);
            }
          }
        });
    return indexOfMissingMeasurements;
  }

  public Pair<List<Integer>, List<String>> computeSourceOfLogicalView(
      ISchemaComputation schemaComputation) {
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    List<String> missedPathStringList = new ArrayList<>();
    Pair<Integer, Integer> beginToEnd = schemaComputation.getRangeOfLogicalViewSchemaListRecorded();
    List<LogicalViewSchema> logicalViewSchemaList = schemaComputation.getLogicalViewSchemaList();
    List<Integer> indexListOfLogicalViewPaths = schemaComputation.getIndexListOfLogicalViewPaths();
    for (int i = beginToEnd.left; i < beginToEnd.right; i++) {
      LogicalViewSchema logicalViewSchema = logicalViewSchemaList.get(i);
      final int realIndex = indexListOfLogicalViewPaths.get(i);
      final int recordMissingIndex = i;
      if (!logicalViewSchema.isWritable()) {
        PartialPath path = schemaComputation.getDevicePath();
        path = path.concatAsMeasurementPath(schemaComputation.getMeasurements()[realIndex]);
        throw new RuntimeException(new InsertNonWritableViewException(path.getFullPath()));
      }
      PartialPath fullPath = logicalViewSchema.getSourcePathIfWritable();
      dualKeyCache.compute(
          new IDualKeyCacheComputation<PartialPath, String, SchemaCacheEntry>() {
            @Override
            public PartialPath getFirstKey() {
              return fullPath.getDevicePath();
            }

            @Override
            public String[] getSecondKeyList() {
              return new String[] {fullPath.getMeasurement()};
            }

            @Override
            public void computeValue(int index, SchemaCacheEntry value) {
              if (value == null) {
                indexOfMissingMeasurements.add(recordMissingIndex);
              } else {
                // Can not call function computeDevice here, because the value is source of one
                // view, but schemaComputation is the device in this insert statement. The
                // computation between them is miss matched.
                if (value.isLogicalView()) {
                  // does not support views in views
                  throw new RuntimeException(
                      new UnsupportedOperationException(
                          String.format(
                              "The source of view [%s] is also a view! Nested view is unsupported! "
                                  + "Please check it.",
                              fullPath)));
                }
                schemaComputation.computeMeasurementOfView(realIndex, value, value.isAligned());
              }
            }
          });
    }
    for (int index : indexOfMissingMeasurements) {
      missedPathStringList.add(logicalViewSchemaList.get(index).getSourcePathStringIfWritable());
    }
    return new Pair<>(indexOfMissingMeasurements, missedPathStringList);
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

  public TimeValuePair getLastCache(PartialPath seriesPath) {
    SchemaCacheEntry entry =
        dualKeyCache.get(seriesPath.getDevicePath(), seriesPath.getMeasurement());
    if (null == entry) {
      return null;
    }

    return DataNodeLastCacheManager.getLastCache(entry);
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
                  storageGroup,
                  measurementPath.getMeasurementSchema(),
                  measurementPath.getTagMap(),
                  measurementPath.isUnderAlignedEntity());
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
