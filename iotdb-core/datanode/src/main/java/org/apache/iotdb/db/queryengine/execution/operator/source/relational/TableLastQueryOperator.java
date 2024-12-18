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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR;

public class TableLastQueryOperator extends TableAggregationTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private boolean finished = false;
  // TODO not need all table aggregators when match last cache
  // private final List<TableAggregator> tableAggregators;
  private List<TableAggregator> unCachedTableAggregators;
  // private final List<ColumnSchema> groupingKeySchemas;

  private final QualifiedObjectName qualifiedObjectName;
  // private final List<DeviceEntry> deviceEntries;
  private int currentDeviceIndex;
  // private final List<String> measurementColumnNames;
  // private final List<IMeasurementSchema> measurementSchemas;
  // private final List<TSDataType> measurementColumnTSDataTypes;

  private QueryDataSource queryDataSource;

  // last_by(x,time) or last(time)
  private final boolean[] isLastBy;
  private final List<String> lastColumns;
  private final List<String> lastByColumns;
  private final int[] indexOfLastColumnInAggregators;
  private final int[] indexOfLastByColumnInAggregators;
  private boolean hashLastBy;
  private boolean needCacheTimeColumn;
  private boolean calcCacheForCurrentDevice;
  private Set<String> currentUnCacheMeasurements = new HashSet<>();

  private final List<TableAggregator> originalTableAggregators;
  private final List<String> originalMeasurementColumnNames;
  private final Set<String> originalAllSensors;
  private final List<IMeasurementSchema> originalMeasurementSchemas;
  // private final List<TSDataType> originalMeasurementColumnTSDataTypes;
  private final List<ColumnSchema> originalColumnSchemas;
  private final int[] originalColumnsIndexArray;
  private final SeriesScanOptions originalSeriesScanOptions;
  // stores all inputChannels of tableAggregators,
  // e.g. for aggregation `last(s1), count(s2), count(s1)`, the inputChannels should be [0, 1, 0]
  private final List<Integer> originalAggregatorInputChannels;

  public TableLastQueryOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      List<DeviceEntry> deviceEntries,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
      int measurementCount,
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      int[] groupingKeyIndex,
      ITableTimeRangeIterator tableTimeRangeIterator,
      boolean ascending,
      boolean canUseStatistics,
      List<Integer> aggregatorInputChannels,
      QualifiedObjectName qualifiedObjectName) {

    super(
        sourceId,
        context,
        null,
        null,
        deviceEntries,
        null,
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        null,
        groupingKeySchemas,
        groupingKeyIndex,
        tableTimeRangeIterator,
        ascending,
        canUseStatistics,
        null);

    this.originalTableAggregators = tableAggregators;
    this.originalAggregatorInputChannels = aggregatorInputChannels;

    this.originalColumnSchemas = columnSchemas;
    this.originalColumnsIndexArray = columnsIndexArray;

    this.originalMeasurementColumnNames = measurementColumnNames;
    this.originalMeasurementSchemas = measurementSchemas;

    this.originalSeriesScanOptions = seriesScanOptions;
    this.originalAllSensors = allSensors;

    //    this.tableAggregators = tableAggregators;
    //    this.groupingKeySchemas = groupingKeySchemas;
    //    this.deviceEntries = deviceEntries;
    //    this.measurementColumnNames = measurementColumnNames;
    //    this.measurementSchemas = measurementSchemas;
    //    this.measurementColumnTSDataTypes = measurementColumnTSDataTypes;

    this.isLastBy = new boolean[tableAggregators.size()];
    this.qualifiedObjectName = qualifiedObjectName;
    this.lastColumns = new ArrayList<>();
    this.lastByColumns = new ArrayList<>();
    this.indexOfLastColumnInAggregators = new int[lastColumns.size()];
    this.indexOfLastByColumnInAggregators = new int[lastByColumns.size()];
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return currentDeviceIndex < deviceEntries.size();
  }

  @Override
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    while (System.nanoTime() - start > maxRuntime
        && !resultTsBlockBuilder.isFull()
        && currentDeviceIndex < deviceEntries.size()) {
      processCurrentDevice();
    }

    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
  }

  /** Main process logic, calc the last aggregation results of current device. */
  private void processCurrentDevice() {
    // calc indexes...
    // consider last(time), last(id), last(attr), last(measurement), last_by(xx)

    // TODO need lock?
    DeviceEntry currentDeviceEntry = deviceEntries.get(currentDeviceIndex);

    if (!calcCacheForCurrentDevice) {

      for (int i = 0; i < originalTableAggregators.size(); i++) {
        TableAggregator tableAggregator = originalTableAggregators.get(i);
        if (isLastBy[i]) {
          Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
              TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
                  qualifiedObjectName.getDatabaseName(),
                  currentDeviceEntry.getDeviceID(),
                  "",
                  lastByColumns);
        } else {

        }
      }

      // process last(time), last_by(x,time)...
      if (!lastByColumns.isEmpty()) {
        Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
            TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
                qualifiedObjectName.getDatabaseName(),
                currentDeviceEntry.getDeviceID(),
                "",
                lastByColumns);

        if (!lastByResult.isPresent()) {
          // all missed
          currentUnCacheMeasurements.addAll(lastByColumns);
        } else {
          TsPrimitiveType[] lastByResultPair = lastByResult.get().getRight();
          for (int i = 0; i < lastByColumns.size(); i++) {
            if (lastByResultPair[i] == null) {
              currentUnCacheMeasurements.add(lastByColumns.get(i));
            } else {

            }
          }
        }
        // TODO verify id and attr columns
      }

      // process last(x), last(y), ...
      if (!lastColumns.isEmpty()) {
        for (int i = 0; i < lastColumns.size(); i++) {
          String measurement = lastByColumns.get(i);
          TimeValuePair timeValuePair =
              TABLE_DEVICE_SCHEMA_CACHE.getLastEntry(
                  qualifiedObjectName.getDatabaseName(),
                  currentDeviceEntry.getDeviceID(),
                  measurement);

          if (timeValuePair == null) {
            currentUnCacheMeasurements.add(measurement);
          } else {

            ColumnBuilder columnBuilder =
                resultTsBlockBuilder.getColumnBuilder(indexOfLastColumnInAggregators[i]);

            if (timeValuePair == EMPTY_TIME_VALUE_PAIR) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeTsPrimitiveType(timeValuePair.getValue());
            }
          }
        }
      }

      calcCacheForCurrentDevice = true;
    }

    // read last value from File or MemTable, update last cache
    if (!currentUnCacheMeasurements.isEmpty()) {

      // avoid invoke twice

      // TABLE_DEVICE_SCHEMA_CACHE.initOrInvalidateLastCache();

      // TABLE_DEVICE_SCHEMA_CACHE.updateLastCacheIfExists

      // init SeriesOptions
    }
  }

  private void buildResultTsBlock() {
    resultTsBlock =
        resultTsBlockBuilder.build(
            new RunLengthEncodedColumn(
                TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()));
    resultTsBlockBuilder.reset();
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
    int groupingKeySize = groupingKeySchemas != null ? groupingKeySchemas.size() : 0;
    List<TSDataType> resultDataTypes = new ArrayList<>(groupingKeySize + tableAggregators.size());

    if (groupingKeySchemas != null) {
      for (int i = 0; i < groupingKeySchemas.size(); i++) {
        resultDataTypes.add(TSDataType.STRING);
      }
    }

    for (TableAggregator aggregator : tableAggregators) {
      resultDataTypes.add(aggregator.getType());
    }

    return resultDataTypes;
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    this.seriesScanUtil.initQueryDataSource(queryDataSource);
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
  }

  @Override
  public long calculateMaxPeekMemory() {
    // TODO
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
