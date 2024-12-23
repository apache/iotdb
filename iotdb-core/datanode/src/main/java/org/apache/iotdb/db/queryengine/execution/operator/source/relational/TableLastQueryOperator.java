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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastDescAccumulator;
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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableLastQueryOperator extends TableAggregationTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private boolean finished = false;
  private boolean fetchLastCacheForCurrentDevice;
  private boolean hasBuildAggTableScanArguments;
  private int outputDeviceIndex;
  private DeviceEntry currentDeviceEntry;
  private Map<String, Integer> aggColumnLayout;
  private int newChannelCnt = 0;
  private List<Integer> newAggColumnsIndexArray;
  private final List<Integer> initLastByInputChannels = Arrays.asList(0, -1, -1);
  private final List<Integer> initLastInputChannels = Arrays.asList(0, -1);
  // the ordinal of uncached columns in initTableAggregators
  private List<Integer> unCachedMeasurementToAggregatorIndex = new ArrayList<>();
  private final String dbName;
  // last_by(x,time) or last(time)
  private final boolean[] isLastByArray;

  private QueryDataSource queryDataSource;
  private final List<DeviceEntry> initDeviceEntries;
  private final List<TableAggregator> initTableAggregators;
  private final List<ColumnSchema> initAggColumnSchemas;
  private final int[] initAggColumnsIndexArray;
  private final List<Integer> initAggregatorInputChannels;
  private final List<String> initMeasurementColumnNames;
  private final List<IMeasurementSchema> initMeasurementSchemas;
  private SeriesScanOptions initSeriesScanOptions;
  private final int groupKeySize;

  public TableLastQueryOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> aggColumnSchemas,
      int[] aggColumnsIndexArray,
      List<DeviceEntry> deviceEntries,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
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
        new ArrayList<>(1),
        1,
        seriesScanOptions,
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

    this.initTableAggregators = tableAggregators;
    this.initAggregatorInputChannels = aggregatorInputChannels;
    this.initDeviceEntries = deviceEntries;
    this.initAggColumnSchemas = aggColumnSchemas;
    this.initAggColumnsIndexArray = aggColumnsIndexArray;
    this.initMeasurementColumnNames = measurementColumnNames;
    this.initMeasurementSchemas = measurementSchemas;
    this.initSeriesScanOptions = seriesScanOptions;
    this.dbName = qualifiedObjectName.getDatabaseName();
    this.groupKeySize = groupingKeySchemas == null ? 0 : groupingKeySchemas.size();

    this.isLastByArray = new boolean[tableAggregators.size()];
    for (int i = 0; i < tableAggregators.size(); i++) {
      if (tableAggregators.get(i).getAccumulator() instanceof LastByAccumulator) {
        isLastByArray[i] = true;
      }
    }
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

    return outputDeviceIndex < initDeviceEntries.size();
  }

  @Override
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    while (System.nanoTime() - start < maxRuntime
        && !resultTsBlockBuilder.isFull()
        && outputDeviceIndex < initDeviceEntries.size()) {
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
    currentDeviceEntry = initDeviceEntries.get(outputDeviceIndex);

    if (!fetchLastCacheForCurrentDevice) {
      resetAggArguments();

      int channelNum = 0;
      for (int aggregatorNum = 0; aggregatorNum < initTableAggregators.size(); aggregatorNum++) {
        TableAggregator aggregator = initTableAggregators.get(aggregatorNum);
        if (isLastByArray[aggregatorNum]) {
          processLastBy(aggregatorNum, channelNum);
        } else {
          processLast(aggregatorNum, channelNum);
        }
        channelNum += aggregator.getChannelCount();
      }

      fetchLastCacheForCurrentDevice = true;
    }

    if (hasUnCachedColumns()) {
      if (!hasBuildAggTableScanArguments) {
        buildAggTableScanArguments();
      }

      if (calculateAggregationResultForCurrentTimeRange()) {
        int channel = 0;
        List<String> updateMeasurementList = new ArrayList<>();
        List<TimeValuePair> updateTimeValuePairList = new ArrayList<>();
        for (TableAggregator tableAggregator : tableAggregators) {
          ColumnSchema schema = aggColumnSchemas.get(aggregatorInputChannels.get(channel));
          if (schema.getColumnCategory() != TsTableColumnCategory.TIME
              && schema.getColumnCategory() != TsTableColumnCategory.MEASUREMENT) {
            // only time and measurement column can update last cache
            channel += tableAggregator.getChannelCount();
            continue;
          }

          boolean isLastBy = tableAggregator.getAccumulator() instanceof LastByDescAccumulator;
          updateMeasurementList.add(
              schema.getColumnCategory() == TsTableColumnCategory.TIME ? "" : schema.getName());
          if (!isLastBy) {
            LastDescAccumulator lastAccumulator =
                (LastDescAccumulator) tableAggregator.getAccumulator();
            TimeValuePair tv =
                lastAccumulator.hasInitResult()
                    ? new TimeValuePair(
                        lastAccumulator.getMaxTime(),
                        cloneTsPrimitiveType(lastAccumulator.getLastValue()))
                    : EMPTY_TIME_VALUE_PAIR;
            updateTimeValuePairList.add(tv);
          } else {
            LastByDescAccumulator lastByAccumulator =
                (LastByDescAccumulator) tableAggregator.getAccumulator();
            long lastTime = lastByAccumulator.getLastTimeOfY();
            TimeValuePair tv =
                new TimeValuePair(
                    lastTime,
                    !lastByAccumulator.hasInitResult() || lastByAccumulator.isXNull()
                        ? EMPTY_PRIMITIVE_TYPE
                        : cloneTsPrimitiveType(lastByAccumulator.getXResult()));
            updateTimeValuePairList.add(tv);
          }
          channel += tableAggregator.getChannelCount();
        }

        if (!updateMeasurementList.isEmpty()) {
          String[] updateMeasurementArray = updateMeasurementList.toArray(new String[0]);
          TimeValuePair[] updateTimeValuePairArray =
              updateTimeValuePairList.toArray(new TimeValuePair[0]);

          TABLE_DEVICE_SCHEMA_CACHE.initOrInvalidateLastCache(
              dbName, currentDeviceEntry.getDeviceID(), updateMeasurementArray, false);
          TABLE_DEVICE_SCHEMA_CACHE.updateLastCacheIfExists(
              dbName,
              currentDeviceEntry.getDeviceID(),
              updateMeasurementArray,
              updateTimeValuePairArray);
        }

        outputDeviceIndex++;
        fetchLastCacheForCurrentDevice = false;
        resetTableAggregators();
      }
    } else {
      outputDeviceIndex++;
      fetchLastCacheForCurrentDevice = false;
      resultTsBlockBuilder.declarePosition();
    }
  }

  private void processLastBy(int aggregatorNum, int channelNum) {
    TableAggregator aggregator = initTableAggregators.get(aggregatorNum);
    ColumnSchema schema = initAggColumnSchemas.get(initAggregatorInputChannels.get(channelNum));
    String columnName = schema.getName();
    TsTableColumnCategory category = schema.getColumnCategory();
    if (category == TsTableColumnCategory.TIME || category == TsTableColumnCategory.MEASUREMENT) {
      if (category == TsTableColumnCategory.TIME) {
        columnName = "";
      }
      Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
          TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
              dbName, currentDeviceEntry.getDeviceID(), "", Collections.singletonList(columnName));
      if (lastByResult.isPresent() && lastByResult.get().getLeft().isPresent()) {
        ColumnBuilder columnBuilder =
            resultTsBlockBuilder.getColumnBuilder(groupKeySize + aggregatorNum);
        TsPrimitiveType timeValuePair = lastByResult.get().getRight()[0];
        if (aggregator.getStep().isOutputPartial()) {
          columnBuilder.writeBinary(
              new Binary(
                  serializeTimeValue(
                      getTSDataType(schema.getType()),
                      lastByResult.get().getLeft().getAsLong(),
                      timeValuePair == null || timeValuePair == EMPTY_PRIMITIVE_TYPE,
                      timeValuePair)));
        } else {
          if (timeValuePair == null || timeValuePair == EMPTY_PRIMITIVE_TYPE) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.writeTsPrimitiveType(timeValuePair);
          }
        }

        return;
      }
    }

    unCachedMeasurementToAggregatorIndex.add(aggregatorNum);
    TableAggregator newAggregator =
        new TableAggregator(
            aggregator.getAccumulator(),
            aggregator.getStep(),
            aggregator.getType(),
            initLastByInputChannels,
            OptionalInt.empty());
    tableAggregators.add(newAggregator);

    // last_by always has three channels
    for (int i = 0; i < 3; i++) {
      buildNewAggregators(channelNum, newAggregator, i);
    }
  }

  private void processLast(int aggregatorNum, int channelNum) {
    TableAggregator aggregator = initTableAggregators.get(aggregatorNum);
    ColumnSchema schema = initAggColumnSchemas.get(initAggregatorInputChannels.get(channelNum));
    String columnName = schema.getName();
    if (schema.getColumnCategory() == TsTableColumnCategory.TIME
        || schema.getColumnCategory() == TsTableColumnCategory.MEASUREMENT) {
      TimeValuePair timeValuePair =
          TABLE_DEVICE_SCHEMA_CACHE.getLastEntry(
              dbName, currentDeviceEntry.getDeviceID(), columnName);

      if (timeValuePair != null) {
        ColumnBuilder columnBuilder =
            resultTsBlockBuilder.getColumnBuilder(groupKeySize + aggregatorNum);

        if (timeValuePair == EMPTY_TIME_VALUE_PAIR) {
          columnBuilder.appendNull();
        } else {
          if (aggregator.getStep().isOutputPartial()) {
            columnBuilder.writeBinary(
                new Binary(
                    serializeTimeValue(
                        timeValuePair.getValue().getDataType(),
                        timeValuePair.getTimestamp(),
                        false,
                        timeValuePair.getValue())));
          } else {
            columnBuilder.writeTsPrimitiveType(timeValuePair.getValue());
          }
        }

        return;
      }
    }

    unCachedMeasurementToAggregatorIndex.add(aggregatorNum);
    TableAggregator newAggregator =
        new TableAggregator(
            aggregator.getAccumulator(),
            aggregator.getStep(),
            aggregator.getType(),
            initLastInputChannels,
            OptionalInt.empty());
    tableAggregators.add(newAggregator);

    // last_by always has two channels
    for (int i = 0; i < 2; i++) {
      buildNewAggregators(channelNum, newAggregator, i);
    }
  }

  private void buildNewAggregators(int channelNum, TableAggregator newAggregator, int i) {
    ColumnSchema schema;
    String columnName;
    int aggIdx = initAggregatorInputChannels.get(channelNum + i);
    schema = initAggColumnSchemas.get(aggIdx);
    columnName = schema.getName();

    if (!aggColumnLayout.containsKey(columnName)) {
      switch (schema.getColumnCategory()) {
        case ID:
        case ATTRIBUTE:
          newAggColumnsIndexArray.add(initAggColumnsIndexArray[aggIdx]);
          break;
        case MEASUREMENT:
          newAggColumnsIndexArray.add(measurementCount);
          measurementCount++;
          measurementColumnNames.add(schema.getName());
          measurementSchemas.add(
              new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
          break;
        case TIME:
          newAggColumnsIndexArray.add(-1);
          break;
        default:
          throw new IllegalArgumentException("Unexpected category: " + schema.getColumnCategory());
      }

      aggColumnSchemas.add(schema);
      aggregatorInputChannels.add(newChannelCnt);
      aggColumnLayout.put(columnName, newChannelCnt++);
    } else {
      aggregatorInputChannels.add(aggColumnLayout.get(columnName));
    }

    newAggregator.getInputChannels()[i] = aggColumnLayout.get(columnName);
  }

  private boolean hasUnCachedColumns() {
    return this.tableAggregators != null && !this.tableAggregators.isEmpty();
  }

  private void buildAggTableScanArguments() {
    aggColumnsIndexArray = newAggColumnsIndexArray.stream().mapToInt(Integer::intValue).toArray();
    allSensors = new HashSet<>(measurementColumnNames);
    allSensors.add("");

    //    seriesScanOptions =
    //        new SeriesScanOptions(
    //            seriesScanOptions.getGlobalTimeFilter(),
    //            seriesScanOptions.getPushDownFilter(),
    //            seriesScanOptions.getPushDownLimit(),
    //            seriesScanOptions.getPushDownOffset(),
    //            allSensors,
    //            seriesScanOptions.getPushLimitToEachDevice());
    deviceEntries.clear();
    deviceEntries.add(currentDeviceEntry);
    currentDeviceIndex = 0;

    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    constructAlignedSeriesScanUtil();
    this.seriesScanUtil.initQueryDataSource(queryDataSource);

    this.hasBuildAggTableScanArguments = true;
    // aggTableScanOperator.initQueryDataSource(this.queryDataSource);
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult();
    // after appendAggregationResult invoked, aggregators must be cleared
    // resetTableAggregators();
  }

  /** Append a row of aggregation results to the result tsBlock. */
  public void appendAggregationResult() {

    // no data in current time range, just output empty
    if (!timeIterator.hasCachedTimeRange()) {
      return;
    }

    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    if (groupingKeyIndex != null) {
      for (int i = 0; i < groupKeySize; i++) {
        if (TsTableColumnCategory.ID == groupingKeySchemas.get(i).getColumnCategory()) {
          String id =
              (String) deviceEntries.get(currentDeviceIndex).getNthSegment(groupingKeyIndex[i] + 1);
          if (id == null) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
          }
        } else {
          Binary attribute =
              deviceEntries
                  .get(currentDeviceIndex)
                  .getAttributeColumnValues()
                  .get(groupingKeyIndex[i]);
          if (attribute == null) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].writeBinary(attribute);
          }
        }
      }
    }

    for (int i = 0; i < tableAggregators.size(); i++) {
      tableAggregators
          .get(i)
          .evaluate(columnBuilders[groupKeySize + unCachedMeasurementToAggregatorIndex.get(i)]);
    }

    resultTsBlockBuilder.declarePosition();
  }

  private void resetAggArguments() {
    hasBuildAggTableScanArguments = false;

    tableAggregators = new ArrayList<>();
    newChannelCnt = 0;

    aggColumnLayout = new HashMap<>();
    aggColumnSchemas = new ArrayList<>();
    aggregatorInputChannels = new ArrayList<>();
    newAggColumnsIndexArray = new ArrayList<>();

    measurementColumnNames = new ArrayList<>();
    measurementSchemas = new ArrayList<>();
    measurementCount = 0;

    unCachedMeasurementToAggregatorIndex.clear();
  }

  private TsPrimitiveType cloneTsPrimitiveType(TsPrimitiveType originalValue) {
    switch (originalValue.getDataType()) {
      case BOOLEAN:
        return new TsPrimitiveType.TsBoolean(originalValue.getBoolean());
      case INT32:
      case DATE:
        return new TsPrimitiveType.TsInt(originalValue.getInt());
      case INT64:
      case TIMESTAMP:
        return new TsPrimitiveType.TsLong(originalValue.getLong());
      case FLOAT:
        return new TsPrimitiveType.TsFloat(originalValue.getFloat());
      case DOUBLE:
        return new TsPrimitiveType.TsDouble(originalValue.getDouble());
      case TEXT:
      case BLOB:
      case STRING:
        return new TsPrimitiveType.TsBinary(originalValue.getBinary());
      case VECTOR:
        return new TsPrimitiveType.TsVector(originalValue.getVector());
      default:
        throw new UnSupportedDataTypeException(
            "Unsupported data type:" + originalValue.getDataType());
    }
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
    List<TSDataType> resultDataTypes = new ArrayList<>(groupKeySize + initTableAggregators.size());

    if (groupingKeySchemas != null) {
      for (int i = 0; i < groupingKeySchemas.size(); i++) {
        resultDataTypes.add(TSDataType.STRING);
      }
    }

    for (TableAggregator aggregator : initTableAggregators) {
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
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(initDeviceEntries);
  }
}
