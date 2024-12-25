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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.exception.metadata.view.InsertNonWritableViewException;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    tableDeviceSchemaCache = TableDeviceSchemaCache.getInstance();
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
      final ISchemaComputation schemaComputation) {
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

      final PartialPath fullPath = logicalViewSchema.getSourcePathIfWritable();
      final IDeviceSchema schema =
          tableDeviceSchemaCache.getDeviceSchema(fullPath.getDevicePath().getNodes());
      if (!(schema instanceof TreeDeviceNormalSchema)) {
        indexOfMissingMeasurements.add(i);
        continue;
      }

      final TreeDeviceNormalSchema treeSchema = (TreeDeviceNormalSchema) schema;
      final SchemaCacheEntry value = treeSchema.getSchemaCacheEntry(fullPath.getMeasurement());
      if (Objects.isNull(value)) {
        indexOfMissingMeasurements.add(i);
        continue;
      }

      if (value.isLogicalView()) {
        throw new RuntimeException(
            new UnsupportedOperationException(
                String.format(
                    "The source of view [%s] is also a view! Nested view is unsupported! "
                        + "Please check it.",
                    logicalViewSchema.getSourcePathIfWritable())));
      }

      schemaComputation.computeMeasurementOfView(realIndex, value, treeSchema.isAligned());
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
              return schema.getMeasurementName();
            }

            @Override
            public IMeasurementSchema getSchema() {
              if (isLogicalView()) {
                return new LogicalViewSchema(
                    schema.getMeasurementName(), ((LogicalViewSchema) schema).getExpression());
              } else {
                return this.getSchemaAsMeasurementSchema();
              }
            }

            @Override
            public MeasurementSchema getSchemaAsMeasurementSchema() {
              return new MeasurementSchema(
                  schema.getMeasurementName(),
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
   * Store the fetched schema in either the {@link TreeDeviceNormalSchema} or {@link
   * TreeDeviceTemplateSchema}, depending on its associated device.
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
        null, seriesPath.getIDeviceID(), seriesPath.getMeasurement());
  }

  public void invalidateLastCache(final MeasurementPath path) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      return;
    }
    tableDeviceSchemaCache.invalidateLastCache(path.getDevicePath(), path.getMeasurement());
  }

  public void invalidateDatabaseLastCache(final String database) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      return;
    }
    tableDeviceSchemaCache.invalidateLastCache(database);
  }

  /**
   * Update the {@link TableDeviceLastCache} in writing for tree model. If a measurement is with all
   * {@code null}s or is an id/attribute column, its {@link TimeValuePair[]} shall be {@code null}.
   * For correctness, this will put the {@link TableDeviceCacheEntry} lazily and only update the
   * existing {@link TableDeviceLastCache}s of measurements.
   *
   * @param database the device's database, WITH "root"
   * @param deviceID {@link IDeviceID}
   * @param measurements the fetched measurements
   * @param timeValuePairs the {@link TimeValuePair}s with indexes corresponding to the measurements
   */
  public void updateLastCacheIfExists(
      final String database,
      final IDeviceID deviceID,
      final String[] measurements,
      final @Nonnull TimeValuePair[] timeValuePairs,
      final boolean isAligned,
      final IMeasurementSchema[] measurementSchemas) {
    tableDeviceSchemaCache.updateLastCache(
        database, deviceID, measurements, timeValuePairs, isAligned, measurementSchemas, false);
  }

  /**
   * Update the {@link TableDeviceLastCache} on query in tree model.
   *
   * <p>Note: The query shall put the {@link TableDeviceLastCache} twice:
   *
   * <p>- First time set the "isCommit" to {@code false} before the query accesses data. It is just
   * to allow the writing to update the cache, then avoid that the query put a stale value to cache
   * and break the consistency. WARNING: The writing may temporarily put a stale value in cache if a
   * stale value is written, but it won't affect the eventual consistency.
   *
   * <p>- Second time put the calculated {@link TimeValuePair}, and use {@link
   * #updateLastCacheIfExists(String, IDeviceID, String[], TimeValuePair[], boolean,
   * IMeasurementSchema[])}. The input {@link TimeValuePair} shall never be or contain {@code null},
   * if the measurement is with all {@code null}s, its {@link TimeValuePair} shall be {@link
   * TableDeviceLastCache#EMPTY_TIME_VALUE_PAIR}. This method is not supposed to update time column.
   *
   * <p>If the query has ended abnormally, it shall call this to invalidate the entry it has pushed
   * in the first time, to avoid the stale writing damaging the eventual consistency. In this case
   * and the "isInvalidate" shall be {@code true}.
   *
   * @param database the device's database, WITH "root"
   * @param measurementPath the fetched {@link MeasurementPath}
   * @param isInvalidate {@code true} if invalidate the first pushed cache, or {@code null} for the
   *     first fetch.
   */
  public void updateLastCache(
      final String database, final MeasurementPath measurementPath, final boolean isInvalidate) {
    tableDeviceSchemaCache.updateLastCache(
        database,
        measurementPath.getIDeviceID(),
        new String[] {measurementPath.getMeasurement()},
        isInvalidate ? new TimeValuePair[] {null} : null,
        measurementPath.isUnderAlignedEntity(),
        new IMeasurementSchema[] {measurementPath.getMeasurementSchema()},
        true);
  }

  public void invalidate(final List<MeasurementPath> partialPathList) {
    // Currently invalidate by device
    partialPathList.forEach(
        measurementPath -> {
          final boolean isMultiLevelWildcardMeasurement =
              PathPatternUtil.isMultiLevelMatchWildcard(measurementPath.getMeasurement());
          tableDeviceSchemaCache.invalidateCache(
              isMultiLevelWildcardMeasurement ? measurementPath : measurementPath.getDevicePath(),
              isMultiLevelWildcardMeasurement);
        });
  }

  public void cleanUp() {
    tableDeviceSchemaCache.invalidateAll();
  }
}
