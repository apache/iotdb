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

import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR;

public class TableLastQueryOperator extends AbstractDataSourceOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private boolean finished = false;
  // TODO not need all table aggregators when match last cache
  private final List<TableAggregator> tableAggregators;
  private List<TableAggregator> unCachedTableAggregators;
  private final List<ColumnSchema> groupingKeySchemas;

  private final QualifiedObjectName qualifiedObjectName;
  private final List<DeviceEntry> deviceEntries;
  private int currentDeviceIndex;
  private final List<String> measurementColumnNames;
  private final List<IMeasurementSchema> measurementSchemas;
  private final List<TSDataType> measurementColumnTSDataTypes;

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
  private List<String> currentUnCacheMeasurements = new ArrayList<>();

  public TableLastQueryOperator(
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      QualifiedObjectName qualifiedObjectName,
      List<DeviceEntry> deviceEntries,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      List<TSDataType> measurementColumnTSDataTypes) {
    this.tableAggregators = tableAggregators;
    this.groupingKeySchemas = groupingKeySchemas;
    this.qualifiedObjectName = qualifiedObjectName;
    this.deviceEntries = deviceEntries;
    this.measurementColumnNames = measurementColumnNames;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes = measurementColumnTSDataTypes;
    this.isLastBy = new boolean[tableAggregators.size()];

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
      if (!lastByColumns.isEmpty()) {
        Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
            TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
                qualifiedObjectName.getDatabaseName(),
                currentDeviceEntry.getDeviceID(),
                "",
                lastByColumns);
        if (!lastByResult.isPresent()) {
          // all missed

        } else {

        }
        // TODO verify id and attr columns
      }

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
  protected List<TSDataType> getResultDataTypes() {
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
