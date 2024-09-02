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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.exception.metadata.view.InsertNonWritableViewException;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.IDeviceSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceNormalSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceTemplateSchema;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class takes the responsibility of metadata cache management of all DataRegions under
 * StorageEngine
 */
public class TreeDeviceSchemaCacheManager {

  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

  private final TableDeviceSchemaCache tableDeviceSchemaCache;

  // cache update or clean have higher priority than cache read
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private TreeDeviceSchemaCacheManager() {
    tableDeviceSchemaCache = TableDeviceSchemaFetcher.getInstance().getTableDeviceCache();
  }

  public static TreeDeviceSchemaCacheManager getInstance() {
    return TreeDeviceSchemaCacheManagerHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class TreeDeviceSchemaCacheManagerHolder {
    private static final TreeDeviceSchemaCacheManager INSTANCE = new TreeDeviceSchemaCacheManager();
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
  public ClusterSchemaTree get(final PartialPath devicePath, final String[] measurements) {
    final ClusterSchemaTree tree = new ClusterSchemaTree();
    final IDeviceSchema schema = tableDeviceSchemaCache.getDeviceSchema(devicePath.getNodes());
    if (!(schema instanceof TreeDeviceNormalSchema)) {
      return tree;
    }
    final TreeDeviceNormalSchema treeSchema = (TreeDeviceNormalSchema) schema;
    for (final String measurement : measurements) {
      final SchemaCacheEntry entry = treeSchema.getSchemaCacheEntry(measurement);
      if (Objects.nonNull(entry)) {
        tree.appendSingleMeasurement(
            devicePath.concatNode(measurement),
            entry.getSchema(),
            entry.getTagMap(),
            null,
            null,
            treeSchema.isAligned());
      }
    }
    tree.setDatabases(Collections.singleton(treeSchema.getDatabase()));
    return tree;
  }

  /**
   * Get schema info under the given device if the device path is a template activated path.
   *
   * @param devicePath full path of the device
   * @return empty if cache miss or the device path is not a template activated path
   */
  public ClusterSchemaTree getMatchedSchemaWithTemplate(final PartialPath devicePath) {
    final ClusterSchemaTree tree = new ClusterSchemaTree();
    final IDeviceSchema schema = tableDeviceSchemaCache.getDeviceSchema(devicePath.getNodes());
    if (!(schema instanceof TreeDeviceTemplateSchema)) {
      return tree;
    }
    final TreeDeviceTemplateSchema treeSchema = (TreeDeviceTemplateSchema) schema;
    Template template = templateManager.getTemplate(treeSchema.getTemplateId());
    tree.appendTemplateDevice(devicePath, template.isDirectAligned(), template.getId(), template);
    tree.setDatabases(Collections.singleton(treeSchema.getDatabase()));
    return tree;
  }

  /**
   * Get schema info under the given full path that must not be a template series.
   *
   * @param fullPath full path
   * @return empty if cache miss
   */
  public ClusterSchemaTree getMatchedSchemaWithoutTemplate(final PartialPath fullPath) {
    final ClusterSchemaTree tree = new ClusterSchemaTree();
    final IDeviceSchema schema =
        tableDeviceSchemaCache.getDeviceSchema(
            Arrays.copyOf(fullPath.getNodes(), fullPath.getNodeLength() - 1));
    if (!(schema instanceof TreeDeviceNormalSchema)) {
      return tree;
    }
    final TreeDeviceNormalSchema treeSchema = (TreeDeviceNormalSchema) schema;
    final SchemaCacheEntry entry = treeSchema.getSchemaCacheEntry(fullPath.getMeasurement());
    if (Objects.isNull(entry)) {
      return tree;
    }
    tree.appendSingleMeasurement(
        fullPath, entry.getSchema(), entry.getTagMap(), null, null, treeSchema.isAligned());
    tree.setDatabases(Collections.singleton(treeSchema.getDatabase()));
    return tree;
  }

  public List<Integer> computeWithoutTemplate(final ISchemaComputation schemaComputation) {
    final List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    final String[] measurements = schemaComputation.getMeasurements();

    final IDeviceSchema schema =
        tableDeviceSchemaCache.getDeviceSchema(schemaComputation.getDevicePath().getNodes());
    if (!(schema instanceof TreeDeviceNormalSchema)) {
      return IntStream.range(0, schemaComputation.getMeasurements().length)
          .boxed()
          .collect(Collectors.toList());
    }
    final TreeDeviceNormalSchema treeSchema = (TreeDeviceNormalSchema) schema;

    for (int i = 0; i < schemaComputation.getMeasurements().length; i++) {
      final SchemaCacheEntry value = treeSchema.getSchemaCacheEntry(measurements[i]);
      if (value == null) {
        indexOfMissingMeasurements.add(i);
      } else {
        schemaComputation.computeMeasurement(i, value);
      }
    }
    schemaComputation.computeDevice(treeSchema.isAligned());
    schemaComputation.recordRangeOfLogicalViewSchemaListNow();
    return indexOfMissingMeasurements;
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

    final List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    final Pair<Integer, Integer> beginToEnd =
        schemaComputation.getRangeOfLogicalViewSchemaListRecorded();
    final List<LogicalViewSchema> logicalViewSchemaList =
        schemaComputation.getLogicalViewSchemaList();
    final List<Integer> indexListOfLogicalViewPaths =
        schemaComputation.getIndexListOfLogicalViewPaths();
    final String[] measurements = schemaComputation.getMeasurements();

    for (int i = beginToEnd.left; i < beginToEnd.right; i++) {
      final LogicalViewSchema logicalViewSchema = logicalViewSchemaList.get(i);
      final int realIndex = indexListOfLogicalViewPaths.get(i);
      if (!logicalViewSchema.isWritable()) {
        throw new RuntimeException(
            new InsertNonWritableViewException(
                schemaComputation
                    .getDevicePath()
                    .concatAsMeasurementPath(schemaComputation.getMeasurements()[realIndex])
                    .getFullPath()));
      }

      final IDeviceSchema schema =
          tableDeviceSchemaCache.getDeviceSchema(schemaComputation.getDevicePath().getNodes());
      if (!(schema instanceof TreeDeviceNormalSchema)) {
        return new Pair<>(
            IntStream.range(0, schemaComputation.getMeasurements().length)
                .boxed()
                .collect(Collectors.toList()),
            logicalViewSchemaList.stream()
                .map(LogicalViewSchema::getSourcePathStringIfWritable)
                .collect(Collectors.toList()));
      }

      final TreeDeviceNormalSchema treeSchema = (TreeDeviceNormalSchema) schema;

      for (int index = 0; index < schemaComputation.getMeasurements().length; index++) {
        final SchemaCacheEntry value = treeSchema.getSchemaCacheEntry(measurements[index]);
        if (value == null) {
          indexOfMissingMeasurements.add(i);
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
                        logicalViewSchema.getSourcePathIfWritable())));
          }
          schemaComputation.computeMeasurementOfView(realIndex, value, treeSchema.isAligned());
        }
      }
    }
    return new Pair<>(
        indexOfMissingMeasurements,
        indexOfMissingMeasurements.stream()
            .map(index -> logicalViewSchemaList.get(index).getSourcePathStringIfWritable())
            .collect(Collectors.toList()));
  }

  public List<Integer> computeWithTemplate(final ISchemaComputation computation) {
    final List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    final String[] measurements = computation.getMeasurements();
    final IDeviceSchema deviceSchema =
        tableDeviceSchemaCache.getDeviceSchema(computation.getDevicePath().getNodes());

    if (!(deviceSchema instanceof TreeDeviceTemplateSchema)) {
      return IntStream.range(0, measurements.length).boxed().collect(Collectors.toList());
    }

    final TreeDeviceTemplateSchema deviceTemplateSchema = (TreeDeviceTemplateSchema) deviceSchema;

    computation.computeDevice(
        templateManager.getTemplate(deviceTemplateSchema.getTemplateId()).isDirectAligned());
    final Map<String, IMeasurementSchema> templateSchema =
        templateManager.getTemplate(deviceTemplateSchema.getTemplateId()).getSchemaMap();
    for (int i = 0; i < measurements.length; i++) {
      if (!templateSchema.containsKey(measurements[i])) {
        indexOfMissingMeasurements.add(i);
        continue;
      }
      final IMeasurementSchema schema = templateSchema.get(measurements[i]);
      computation.computeMeasurement(
          i,
          new IMeasurementSchemaInfo() {
            @Override
            public String getName() {
              return schema.getMeasurementId();
            }

            @Override
            public IMeasurementSchema getSchema() {
              if (isLogicalView()) {
                return new LogicalViewSchema(
                    schema.getMeasurementId(), ((LogicalViewSchema) schema).getExpression());
              } else {
                return this.getSchemaAsMeasurementSchema();
              }
            }

            @Override
            public MeasurementSchema getSchemaAsMeasurementSchema() {
              return new MeasurementSchema(
                  schema.getMeasurementId(),
                  schema.getType(),
                  schema.getEncodingType(),
                  schema.getCompressor());
            }

            @Override
            public LogicalViewSchema getSchemaAsLogicalViewSchema() {
              throw new RuntimeException(
                  new UnsupportedOperationException(
                      "Function getSchemaAsLogicalViewSchema is not supported in DeviceUsingTemplateSchemaCache."));
            }

            @Override
            public Map<String, String> getTagMap() {
              return null;
            }

            @Override
            public Map<String, String> getAttributeMap() {
              return null;
            }

            @Override
            public String getAlias() {
              return null;
            }

            @Override
            public boolean isLogicalView() {
              return schema.isLogicalView();
            }
          });
    }
    return indexOfMissingMeasurements;
  }

  /**
   * Store the fetched schema in either the schemaCache or templateSchemaCache, depending on its
   * associated device.
   */
  public void put(final ClusterSchemaTree tree) {
    tree.getAllDevices()
        .forEach(
            deviceSchemaInfo ->
                tableDeviceSchemaCache.putDeviceSchema(
                    tree.getBelongedDatabase(deviceSchemaInfo.getDevicePath()), deviceSchemaInfo));
  }

  public TimeValuePair getLastCache(final MeasurementPath seriesPath) {
    return tableDeviceSchemaCache.getLastEntry(
        seriesPath.getNodes()[1], seriesPath.getIDeviceID(), seriesPath.getMeasurement());
  }

  public void invalidateLastCache(final MeasurementPath path) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      return;
    }
    tableDeviceSchemaCache.invalidateLastCache(path.getDevicePath(), path.getMeasurement());
  }

  public void invalidateLastCacheInDataRegion(final String database) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      return;
    }
    tableDeviceSchemaCache.invalidate(database);
  }

  /**
   * Update the last cache on query or data recover in tree model. The input "TimeValuePair" shall
   * never be or contain {@code null} unless the measurement is the time measurement "". If the
   * measurements are all {@code null}s, the timeValuePair shall be {@link
   * TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}.
   *
   * <p>If the global last time is queried or recovered, the measurement shall be an empty string
   * and time shall be in the timeValuePair's timestamp, whose value is typically {@link
   * TableDeviceLastCache#EMPTY_PRIMITIVE_TYPE}. Or, the device's last time won't be updated because
   * we cannot guarantee the completeness of the existing measurements in cache.
   *
   * <p>The input "TimeValuePair" shall never be or contain {@code null}.
   *
   * @param database the device's database, without "root"
   * @param deviceID IDeviceID
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCache(
      final String database,
      final String prefix,
      final IDeviceID deviceID,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs,
      final boolean isAligned,
      final MeasurementSchema[] measurementSchemas) {
    tableDeviceSchemaCache.updateLastCache(
        database,
        prefix,
        deviceID,
        measurements,
        timeValuePairs,
        isAligned,
        measurementSchemas,
        false);
  }

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
    // Deprecated
  }

  public void updateLastCacheWithoutLock(
      String database,
      PartialPath devicePath,
      String[] measurements,
      MeasurementSchema[] measurementSchemas,
      boolean isAligned,
      IntFunction<TimeValuePair> timeValuePairProvider,
      IntPredicate shouldUpdateProvider,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    // Deprecated
  }

  /**
   * get or create SchemaCacheEntry and update last cache, only support non-aligned sensor or
   * aligned sensor without only one sub sensor
   */
  public void updateLastCache(
      final String database,
      final MeasurementPath measurementPath,
      final TimeValuePair timeValuePair) {
    tableDeviceSchemaCache.updateLastCache(
        database,
        measurementPath.getNodes()[1],
        measurementPath.getIDeviceID(),
        new String[] {measurementPath.getMeasurement()},
        new TimeValuePair[] {timeValuePair},
        measurementPath.isUnderAlignedEntity(),
        new IMeasurementSchema[] {measurementPath.getMeasurementSchema()},
        true);
  }

  public void invalidate(final List<MeasurementPath> partialPathList) {
    // Currently invalidate by device
    partialPathList.forEach(
        measurementPath -> tableDeviceSchemaCache.invalidateCache(measurementPath.getDevicePath()));
  }

  public void cleanUp() {
    tableDeviceSchemaCache.invalidateAll();
  }
}
