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
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * This class takes the responsibility of metadata cache management of all DataRegions under
 * StorageEngine
 */
public class DataNodeSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

  private final DeviceUsingTemplateSchemaCache deviceUsingTemplateSchemaCache;

  private final TimeSeriesSchemaCache timeSeriesSchemaCache;

  // cache update or clean have higher priority than cache read
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private DataNodeSchemaCache() {
    deviceUsingTemplateSchemaCache = new DeviceUsingTemplateSchemaCache(templateManager);
    timeSeriesSchemaCache = new TimeSeriesSchemaCache();

    MetricService.getInstance().addMetricSet(new DataNodeSchemaCacheMetrics(this));
  }

  public long getHitCount() {
    return deviceUsingTemplateSchemaCache.getHitCount() + timeSeriesSchemaCache.getHitCount();
  }

  public long getRequestCount() {
    return deviceUsingTemplateSchemaCache.getRequestCount()
        + timeSeriesSchemaCache.getRequestCount();
  }

  public static DataNodeSchemaCache getInstance() {
    return DataNodeSchemaCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeSchemaCacheHolder {
    private static final DataNodeSchemaCache INSTANCE = new DataNodeSchemaCache();
  }

  public void takeReadLock() {
    readWriteLock.readLock().lock();
  }

  public void releaseReadLock() {
    readWriteLock.readLock().unlock();
  }

  public void takeWriteLock() {
    readWriteLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    readWriteLock.writeLock().unlock();
  }

  /**
   * Get SchemaEntity info without auto create schema
   *
   * @param devicePath should not be measurementPath or AlignedPath
   * @param measurements
   * @return timeseries partialPath and its SchemaEntity
   */
  public ClusterSchemaTree get(PartialPath devicePath, String[] measurements) {
    return timeSeriesSchemaCache.get(devicePath, measurements);
  }

  public ClusterSchemaTree get(PartialPath fullPath) {
    ClusterSchemaTree clusterSchemaTree = deviceUsingTemplateSchemaCache.get(fullPath);
    if (clusterSchemaTree == null || clusterSchemaTree.isEmpty()) {
      return timeSeriesSchemaCache.get(fullPath);
    } else {
      return clusterSchemaTree;
    }
  }

  public ClusterSchemaTree getMatchedSchemaWithTemplate(PartialPath path) {
    return deviceUsingTemplateSchemaCache.getMatchedSchemaWithTemplate(path);
  }

  public List<Integer> computeWithoutTemplate(ISchemaComputation schemaComputation) {
    List<Integer> result = timeSeriesSchemaCache.computeAndRecordLogicalView(schemaComputation);
    schemaComputation.recordRangeOfLogicalViewSchemaListNow();
    return result;
  }

  /**
   * This function is used to process logical view schema list in statement. It will try to find the
   * source paths of those views in cache. If it found sources, measurement schemas of sources will
   * be recorded in measurement schema list; else the views will be recorded as missed. The indexes
   * of missed views and full paths of their source paths will be returned.
   *
   * @param schemaComputation the statement you want to process
   * @return The indexes of missed views and full paths of their source paths will be returned.
   */
  public Pair<List<Integer>, List<String>> computeSourceOfLogicalView(
      ISchemaComputation schemaComputation) {
    if (!schemaComputation.hasLogicalViewNeedProcess()) {
      return new Pair<>(new ArrayList<>(), new ArrayList<>());
    }
    return timeSeriesSchemaCache.computeSourceOfLogicalView(schemaComputation);
  }

  public List<Integer> computeWithTemplate(ISchemaComputation schemaComputation) {
    return deviceUsingTemplateSchemaCache.compute(schemaComputation);
  }

  /**
   * Store the fetched schema in either the schemaCache or templateSchemaCache, depending on its
   * associated device.
   */
  public void put(ClusterSchemaTree tree) {
    Optional<Pair<Template, ?>> templateInfo;
    PartialPath devicePath;
    Set<PartialPath> templateDevices = new HashSet<>();
    Set<PartialPath> commonDevices = new HashSet<>();
    for (MeasurementPath path : tree.getAllMeasurement()) {
      devicePath = path.getDevicePath();
      if (templateDevices.contains(devicePath)) {
        continue;
      }

      if (commonDevices.contains(devicePath)) {
        timeSeriesSchemaCache.putSingleMeasurementPath(tree.getBelongedDatabase(path), path);
        continue;
      }

      templateInfo = Optional.ofNullable(templateManager.checkTemplateSetInfo(devicePath));
      if (templateInfo.isPresent()) {
        deviceUsingTemplateSchemaCache.put(
            devicePath, tree.getBelongedDatabase(devicePath), templateInfo.get().left.getId());
        templateDevices.add(devicePath);
      } else {
        timeSeriesSchemaCache.putSingleMeasurementPath(tree.getBelongedDatabase(path), path);
        commonDevices.add(devicePath);
      }
    }
  }

  public TimeValuePair getLastCache(PartialPath seriesPath) {
    return timeSeriesSchemaCache.getLastCache(seriesPath);
  }

  /** get SchemaCacheEntry and update last cache */
  public void updateLastCache(
      PartialPath devicePath,
      String measurement,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    timeSeriesSchemaCache.updateLastCache(
        devicePath, measurement, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

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
    timeSeriesSchemaCache.updateLastCache(
        database,
        devicePath,
        measurements,
        measurementSchemas,
        isAligned,
        timeValuePairProvider,
        shouldUpdateProvider,
        highPriorityUpdate,
        latestFlushedTime);
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
    timeSeriesSchemaCache.updateLastCache(
        storageGroup, measurementPath, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  public void invalidateAll() {
    deviceUsingTemplateSchemaCache.invalidateCache();
    timeSeriesSchemaCache.invalidateAll();
  }

  public void cleanUp() {
    deviceUsingTemplateSchemaCache.invalidateCache();
    timeSeriesSchemaCache.invalidateAll();
  }
}
