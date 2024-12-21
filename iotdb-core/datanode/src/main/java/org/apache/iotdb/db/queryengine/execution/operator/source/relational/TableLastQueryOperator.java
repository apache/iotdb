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

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
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

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableLastQueryOperator extends TableAggregationTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private boolean finished = false;
  private boolean fetchLastCacheForCurrentDevice;
  private int outputDeviceIndex;
  private DeviceEntry currentDeviceEntry;
  private TableAggregationTableScanOperator aggTableScanOperator;
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
        Collections.emptyList(),
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
  private void processCurrentDevice() throws Exception {
    currentDeviceEntry = initDeviceEntries.get(outputDeviceIndex);

    if (!fetchLastCacheForCurrentDevice) {
      resetAggArguments();

      int initChannel = 0;
      for (int i = 0; i < initTableAggregators.size(); i++) {
        TableAggregator aggregator = initTableAggregators.get(i);
        if (isLastByArray[i]) {
          processLastBy(i, initChannel);
        } else {
          processLast(i, initChannel);
        }
        initChannel += aggregator.getChannelCount();
      }

      fetchLastCacheForCurrentDevice = true;
    }

    if (hasUnCachedColumns()) {
      if (aggTableScanOperator == null) {
        buildAggTableScanArguments();
      }

      if (!aggTableScanOperator.hasNext()) {
        for (int i = 0; i < tableAggregators.size() - 1; i++) {
          resultTsBlockBuilder
              .getValueColumnBuilders()[groupKeySize + unCachedMeasurementToAggregatorIndex.get(i)]
              .appendNull();
        }
        outputDeviceIndex++;
        fetchLastCacheForCurrentDevice = false;
        appendToResultTsBlockBuilder();
        return;
      }

      TsBlock updateBlock = aggTableScanOperator.next();
      if (updateBlock != null && !updateBlock.isEmpty()) {
        int channel = 0;
        List<String> updateMeasurementList = new ArrayList<>();
        List<TimeValuePair> updateTimeValuePairList = new ArrayList<>();
        for (int i = 0; i < tableAggregators.size() - 1; i++) {
          if (updateBlock.getColumn(i).isNull(0)) {
            resultTsBlockBuilder
                .getValueColumnBuilders()[
                groupKeySize + unCachedMeasurementToAggregatorIndex.get(i)]
                .appendNull();
          } else {
            resultTsBlockBuilder
                .getValueColumnBuilders()[
                groupKeySize + unCachedMeasurementToAggregatorIndex.get(i)]
                .writeBinary(updateBlock.getColumn(i).getBinary(0));
          }

          boolean isLastBy =
              tableAggregators.get(i).getAccumulator() instanceof LastByDescAccumulator;

          if (!isLastBy || !updateBlock.getColumn(i).isNull(0)) {
            ColumnSchema schema = aggColumnSchemas.get(aggregatorInputChannels.get(channel));
            if (schema.getColumnCategory() == TsTableColumnCategory.TIME
                || schema.getColumnCategory() == TsTableColumnCategory.MEASUREMENT) {
              updateMeasurementList.add(
                  schema.getColumnCategory() == TsTableColumnCategory.TIME ? "" : schema.getName());
              TimeValuePair tv =
                  new TimeValuePair(
                      updateBlock.getColumn(tableAggregators.size() - 1).getLong(0),
                      updateBlock.getColumn(i).getTsPrimitiveType(0));
              updateTimeValuePairList.add(tv);
            }
          }
        }
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

        outputDeviceIndex++;
        fetchLastCacheForCurrentDevice = false;
        appendToResultTsBlockBuilder();
      }
    }
  }

  private void processLastBy(int aggregatorNum, int channelNum) {
    TableAggregator aggregator = initTableAggregators.get(aggregatorNum);
    ColumnSchema schema = initAggColumnSchemas.get(initAggregatorInputChannels.get(channelNum));
    String columnName = schema.getName();
    if (schema.getColumnCategory() == TsTableColumnCategory.TIME
        || schema.getColumnCategory() == TsTableColumnCategory.MEASUREMENT) {
      Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
          TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
              dbName, currentDeviceEntry.getDeviceID(), "", Collections.singletonList(columnName));

      if (lastByResult.isPresent() && lastByResult.get().getRight()[0] != null) {
        TsPrimitiveType timeValuePair = lastByResult.get().getRight()[0];
        ColumnBuilder columnBuilder =
            resultTsBlockBuilder.getColumnBuilder(groupKeySize + aggregatorNum);

        if (timeValuePair == null) {
          columnBuilder.appendNull();
        } else {
          if (aggregator.getStep().isOutputPartial()) {
            columnBuilder.writeBinary(
                new Binary(
                    serializeTimeValue(
                        timeValuePair.getDataType(),
                        lastByResult.get().getLeft().getAsLong(),
                        false,
                        timeValuePair)));
          } else {
            columnBuilder.writeTsPrimitiveType(timeValuePair);
          }
        }
        return;
      }

      unCachedMeasurementToAggregatorIndex.add(aggregatorNum);
    }

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
      schema = initAggColumnSchemas.get(initAggregatorInputChannels.get(channelNum + i));
      columnName = schema.getName();

      if (!aggColumnLayout.containsKey(columnName)) {
        switch (schema.getColumnCategory()) {
          case ID:
          case ATTRIBUTE:
            newAggColumnsIndexArray.add(initAggColumnsIndexArray[channelNum]);
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
            throw new IllegalArgumentException(
                "Unexpected category: " + schema.getColumnCategory());
        }

        aggColumnSchemas.add(schema);
        aggregatorInputChannels.add(newChannelCnt);
        aggColumnLayout.put(columnName, newChannelCnt++);
      } else {
        aggregatorInputChannels.add(aggColumnLayout.get(columnName));
      }

      newAggregator.getInputChannels()[i] = aggColumnLayout.get(columnName);
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

      unCachedMeasurementToAggregatorIndex.add(aggregatorNum);
    }

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
      schema = initAggColumnSchemas.get(initAggregatorInputChannels.get(channelNum + i));
      columnName = schema.getName();

      if (!aggColumnLayout.containsKey(columnName)) {
        switch (schema.getColumnCategory()) {
          case ID:
          case ATTRIBUTE:
            newAggColumnsIndexArray.add(initAggColumnsIndexArray[channelNum]);
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
            throw new IllegalArgumentException(
                "Unexpected category: " + schema.getColumnCategory());
        }

        aggColumnSchemas.add(schema);
        aggregatorInputChannels.add(newChannelCnt);
        aggColumnLayout.put(columnName, newChannelCnt++);
      } else {
        aggregatorInputChannels.add(aggColumnLayout.get(columnName));
      }

      newAggregator.getInputChannels()[i] = aggColumnLayout.get(columnName);
    }
  }

  private boolean hasUnCachedColumns() {
    return this.tableAggregators != null && !this.tableAggregators.isEmpty();
  }

  private void buildAggTableScanArguments() {
    addLastTimeAggregationToAggregators();

    aggColumnsIndexArray = newAggColumnsIndexArray.stream().mapToInt(Integer::intValue).toArray();
    allSensors = new HashSet<>(measurementColumnNames);
    allSensors.add("");

    aggTableScanOperator =
        new TableAggregationTableScanOperator(
            sourceId,
            operatorContext,
            aggColumnSchemas,
            aggColumnsIndexArray,
            Collections.singletonList(currentDeviceEntry),
            seriesScanOptions,
            measurementColumnNames,
            allSensors,
            measurementSchemas,
            tableAggregators,
            Collections.emptyList(),
            null,
            timeIterator,
            false,
            canUseStatistics,
            aggregatorInputChannels);

    aggTableScanOperator.initQueryDataSource(this.queryDataSource);
  }

  private void addLastTimeAggregationToAggregators() {
    // always add last(time) to tail, update last cache need the value of last(time)
    int timeColumnIdx = aggColumnLayout.get("time");
    tableAggregators.add(
        new TableAggregator(
            new LastDescAccumulator(TSDataType.TIMESTAMP),
            AggregationNode.Step.FINAL,
            TSDataType.TIMESTAMP,
            Collections.singletonList(timeColumnIdx),
            OptionalInt.empty()));

    aggregatorInputChannels.add(timeColumnIdx);
    aggregatorInputChannels.add(timeColumnIdx);
  }

  private void appendToResultTsBlockBuilder() {
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    for (int i = 0; i < groupKeySize; i++) {
      if (TsTableColumnCategory.ID == groupingKeySchemas.get(i).getColumnCategory()) {
        String id =
            (String)
                initDeviceEntries.get(outputDeviceIndex).getNthSegment(groupingKeyIndex[i] + 1);
        if (id == null) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
        }
      } else {
        Binary attribute =
            initDeviceEntries
                .get(outputDeviceIndex)
                .getAttributeColumnValues()
                .get(groupingKeyIndex[i]);
        if (attribute == null) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeBinary(attribute);
        }
      }
    }
    resultTsBlockBuilder.declarePosition();
  }

  private void resetAggArguments() throws Exception {
    if (aggTableScanOperator != null) {
      aggTableScanOperator.close();
    }
    aggTableScanOperator = null;

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

  private void buildResultTsBlock() {
    resultTsBlock =
        resultTsBlockBuilder.build(
            new RunLengthEncodedColumn(
                TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()));
    resultTsBlockBuilder.reset();
  }

  @Override
  protected void constructAlignedSeriesScanUtil() {
    // TODO?
    //    if (this.deviceEntries == null || this.deviceEntries.isEmpty()) {
    //      return;
    //    }

    DeviceEntry deviceEntry;

    if (this.deviceEntries.isEmpty() || this.deviceEntries.get(this.outputDeviceIndex) == null) {
      // for device which is not exist
      deviceEntry = new DeviceEntry(new StringArrayDeviceID(""), Collections.emptyList());
    } else {
      deviceEntry = this.deviceEntries.get(this.outputDeviceIndex);
    }

    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas, allSensors);

    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            alignedPath,
            Ordering.DESC,
            seriesScanOptions,
            operatorContext.getInstanceContext(),
            true,
            measurementColumnTSDataTypes);
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
