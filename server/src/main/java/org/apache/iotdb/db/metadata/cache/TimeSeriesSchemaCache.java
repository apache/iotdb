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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.metadata.cache.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.metadata.cache.dualkeycache.impl.DualKeyCachePolicy;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class TimeSeriesSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

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
                  devicePath.concatNode(value.getSchemaEntryId()),
                  value.getIMeasurementSchema(),
                  value.getTagMap(),
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

  public Pair<List<Integer>, List<String>> computeSourceOfLogicalViewInCache(
      ISchemaComputation schemaComputation) {
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    List<String> missedPathStringList = new ArrayList<>();
    final AtomicBoolean isFirstMeasurement = new AtomicBoolean(true);
    Pair<Integer, Integer> beginToEnd = schemaComputation.getSizeOfLogicalViewSchemaListRecorded();
    List<LogicalViewSchema> logicalViewSchemaList = schemaComputation.getLogicalViewSchemaList();
    List<Integer> indexListOfLogicalViewPaths = schemaComputation.getIndexListOfLogicalViewPaths();
    for (int i = beginToEnd.left; i < beginToEnd.right; i++) {
      LogicalViewSchema logicalViewSchema = logicalViewSchemaList.get(i);
      final int realIndex = indexListOfLogicalViewPaths.get(i);
      final int recordMissingIndex = i;
      if (!logicalViewSchema.isWritable()) {
        throw new RuntimeException(
            new UnsupportedOperationException(
                "You can not insert into a logical view which is not alias series!"));
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
              index = realIndex;
              if (value == null) {
                indexOfMissingMeasurements.add(recordMissingIndex);
              } else {
                if (isFirstMeasurement.get()) {
                  schemaComputation.computeDevice(value.isAligned());
                  isFirstMeasurement.getAndSet(false);
                }
                if (value.isLogicalView()) {
                  // does not support views in views
                  throw new RuntimeException(
                      new UnsupportedOperationException(
                          String.format(
                              "The source of view [%s] is also a view! Nested view is unsupported! "
                                  + "Please check it.",
                              fullPath)));
                }
                schemaComputation.computeMeasurementOfView(index, value, value.isAligned());
              }
            }
          });
    }
    for (int index : indexOfMissingMeasurements) {
      missedPathStringList.add(logicalViewSchemaList.get(index).getSourcePathStringIfWritable());
    }
    return new Pair<>(indexOfMissingMeasurements, missedPathStringList);
  }

  public void put(ClusterSchemaTree schemaTree) {
    for (MeasurementPath measurementPath : schemaTree.getAllMeasurement()) {
      putSingleMeasurementPath(schemaTree.getBelongedDatabase(measurementPath), measurementPath);
    }
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
      Function<Integer, TimeValuePair> timeValuePairProvider,
      Function<Integer, Boolean> shouldUpdateProvider,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    SchemaCacheEntry entry;
    List<Integer> missingMeasurements = new ArrayList<>();
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
            if (!shouldUpdateProvider.apply(index)) {
              return;
            }
            if (value == null) {
              missingMeasurements.add(index);
            } else {
              DataNodeLastCacheManager.updateLastCache(
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

      DataNodeLastCacheManager.updateLastCache(
          entry, timeValuePairProvider.apply(index), highPriorityUpdate, latestFlushedTime);
    }
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
                  (MeasurementSchema) measurementPath.getMeasurementSchema(),
                  measurementPath.getTagMap(),
                  measurementPath.isUnderAlignedEntity());
          dualKeyCache.put(seriesPath.getDevicePath(), seriesPath.getMeasurement(), entry);
        }
      }
    }

    DataNodeLastCacheManager.updateLastCache(
        entry, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  public void invalidateAll() {
    dualKeyCache.invalidateAll();
  }

  public void cleanUp() {
    dualKeyCache.cleanUp();
  }
}
