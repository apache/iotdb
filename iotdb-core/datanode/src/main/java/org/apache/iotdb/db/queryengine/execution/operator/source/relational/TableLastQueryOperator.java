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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableLastQueryOperator extends TableAggregationTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private boolean finished = false;

  private final QualifiedObjectName qualifiedObjectName;
  private int lastQueryDeviceIndex;

  private QueryDataSource queryDataSource;

  private SeriesScanOptions initSeriesScanOptions;
  // last_by(x,time) or last(time)
  private final boolean[] isLastBy;
  private boolean hashLastBy;
  private boolean calcCacheForCurrentDevice;

  private final List<DeviceEntry> initDeviceEntries;
  private final List<TableAggregator> initTableAggregators;
  private final List<String> initMeasurementColumnNames;
  private final Set<String> initAllSensors;
  private final List<IMeasurementSchema> initMeasurementSchemas;
  private final List<ColumnSchema> initAggColumnSchemas;
  private final int[] initAggColumnsIndexArray;

  // stores all inputChannels of tableAggregators,
  // e.g. for aggregation `last(s1), count(s2), count(s1)`, the inputChannels should be [0, 1, 0]
  private final List<Integer> initAggregatorInputChannels;

  private final int groupKeySize;

  DeviceEntry currentDeviceEntry;

  List<Integer> cacheToAggregatorIndex = new ArrayList<>();

  TableAggregationTableScanOperator aggTableScanOperator;

  Map<String, Integer> aggColumnLayout;

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
    this.initAllSensors = allSensors;

    this.isLastBy = new boolean[tableAggregators.size()];
    // change it later
    Arrays.fill(isLastBy, true);
    this.qualifiedObjectName = qualifiedObjectName;

    groupKeySize = groupingKeySchemas == null ? 0 : groupingKeySchemas.size();
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

    return lastQueryDeviceIndex < initDeviceEntries.size();
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
        && lastQueryDeviceIndex < initDeviceEntries.size()) {
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

    // TODO need lock?
    currentDeviceEntry = initDeviceEntries.get(lastQueryDeviceIndex);

    if (!calcCacheForCurrentDevice) {
      resetArguments();

      int channelCnt = 0;

      for (int i = 0; i < initTableAggregators.size(); i++) {
        TableAggregator aggregator = initTableAggregators.get(i);
        if (isLastBy[i]) {
          processLastBy(i, channelCnt);
        } else {
          processLast(channelCnt);
        }

        channelCnt += aggregator.getChannelCount();
      }

      calcCacheForCurrentDevice = true;
    }

    if (aggColumnSchemas != null && !aggColumnSchemas.isEmpty()) {
      //      if (aggTableScanOperator == null) {
      //        // always add last(time) to tail, update last cache need the value of last(time)
      //        tableAggregators.add(
      //            new TableAggregator(
      //                new LastDescAccumulator(TSDataType.TIMESTAMP),
      //                tableAggregators.get(0).getStep(),
      //                TSDataType.TIMESTAMP,
      //                Arrays.asList(aggColumnLayout.get("time"), aggColumnLayout.get("time")),
      //                OptionalInt.empty()));
      //        aggTableScanOperator =
      //            new TableAggregationTableScanOperator(
      //                sourceId,
      //                operatorContext,
      //                aggColumnSchemas,
      //                aggColumnsIndexArray,
      //                Collections.singletonList(currentDeviceEntry),
      //                seriesScanOptions,
      //                measurementColumnNames,
      //                allSensors,
      //                measurementSchemas,
      //                tableAggregators,
      //                Collections.emptyList(),
      //                null,
      //                timeIterator,
      //                false,
      //                canUseStatistics,
      //                aggregatorInputChannels);
      //      }

      //      if (calculateAggregationResultForCurrentTimeRange()) {
      //
      //      }

      if (calculateAggregationResultForCurrentTimeRange()) {
        //        TsBlock updateBlock = aggTableScanOperator.next();
        //        if (updateBlock != null && !updateBlock.isEmpty()) {
        String dbName = qualifiedObjectName.getDatabaseName();

        List<String> updateMeasurementList = new ArrayList<>();
        List<TimeValuePair> updateTimeValuePairList = new ArrayList<>();
        for (int i = 0; i < tableAggregators.size(); i++) {
          //            if (!isLastBy[i] || (isLastBy[i] && !updateBlock.getColumn(i).isNull(0))) {
          //              updateMeasurementList.add("");
          //              TimeValuePair tv =
          //                  new TimeValuePair(
          //                      updateBlock.getColumn(0).getLong(0),
          //                      updateBlock.getColumn(i).getTsPrimitiveType(0));
          //              updateTimeValuePairList.add(tv);
          //            }
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

        aggTableScanOperator.close();
        aggTableScanOperator = null;
        lastQueryDeviceIndex++;
        appendToResultTsBlockBuilder();
        resetArguments();
      }
    }
    //    }
  }

  private void processLastBy(int aggregatorNum, int initChannel) {
    TableAggregator aggregator = initTableAggregators.get(aggregatorNum);
    ColumnSchema schema = initAggColumnSchemas.get(initChannel);
    String columnName = schema.getName();
    if (schema.getColumnCategory() == TsTableColumnCategory.TIME
        || schema.getColumnCategory() == TsTableColumnCategory.MEASUREMENT) {
      Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
          TABLE_DEVICE_SCHEMA_CACHE.getLastRow(
              qualifiedObjectName.getDatabaseName(),
              currentDeviceEntry.getDeviceID(),
              "",
              Collections.singletonList(columnName));

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

      tableAggregators.add(aggregator);
      cacheToAggregatorIndex.add(aggregatorNum);
    }

    // last_by always has three channels
    for (int i = 0; i < 3; i++) {
      schema = initAggColumnSchemas.get(initChannel + i);
      columnName = schema.getName();

      if (!aggColumnLayout.containsKey(columnName)) {
        switch (schema.getColumnCategory()) {
          case ID:
          case ATTRIBUTE:
            aggColumnsIndexArray[channel] = initAggColumnsIndexArray[initChannel];
            break;
          case MEASUREMENT:
            aggColumnsIndexArray[channel] = measurementCount;
            measurementCount++;
            measurementColumnNames.add(schema.getName());
            measurementSchemas.add(
                new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
            break;
          case TIME:
            aggColumnsIndexArray[channel] = -1;
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected category: " + schema.getColumnCategory());
        }

        aggColumnSchemas.add(schema);
        aggregatorInputChannels.add(channel);
        aggColumnLayout.put(columnName, channel++);
      } else {
        aggregatorInputChannels.add(aggColumnLayout.get(columnName));
      }

      aggregator.getInputChannels()[i] = aggColumnLayout.get(columnName);
    }
  }

  private void processLast(int channelCnt) {
    ColumnSchema columnSchema = initAggColumnSchemas.get(channelCnt);
    String columnName = columnSchema.getName();

    if (columnSchema.getColumnCategory() == TsTableColumnCategory.ID
        || columnSchema.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE) {
      if (!aggColumnLayout.containsKey(columnName)) {
        newColumnsIndexArray.add(initAggColumnsIndexArray[channelCnt]);
      }
    } else if (columnSchema.getColumnCategory() == TsTableColumnCategory.TIME) {
      aggColumnsIndexArray[channel] = -1;
    } else {
      // String measurement = initMeasurementColumnNames.get(i);

      aggColumnsIndexArray[channel] = measurementCount;
      measurementCount++;
      measurementColumnNames.add(columnSchema.getName());
      measurementSchemas.add(
          new MeasurementSchema(columnSchema.getName(), getTSDataType(columnSchema.getType())));
    }
  }

  List<Integer> newColumnsIndexArray;
  int channel = 0;

  private void appendToResultTsBlockBuilder() {
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    for (int i = 0; i < groupKeySize; i++) {
      if (TsTableColumnCategory.ID == groupingKeySchemas.get(i).getColumnCategory()) {
        String id =
            (String)
                initDeviceEntries.get(lastQueryDeviceIndex).getNthSegment(groupingKeyIndex[i] + 1);
        if (id == null) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
        }
      } else {
        Binary attribute =
            initDeviceEntries
                .get(lastQueryDeviceIndex)
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

  private void resetArguments() {
    aggColumnLayout = new HashMap<>();

    aggregatorInputChannels = new ArrayList<>();
    tableAggregators = new ArrayList<>();

    measurementColumnNames = new ArrayList<>();
    measurementSchemas = new ArrayList<>();
    newColumnsIndexArray = new ArrayList<>();
    channel = 0;
    measurementCount = 0;
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

    if (this.deviceEntries.isEmpty() || this.deviceEntries.get(this.lastQueryDeviceIndex) == null) {
      // for device which is not exist
      deviceEntry = new DeviceEntry(new StringArrayDeviceID(""), Collections.emptyList());
    } else {
      deviceEntry = this.deviceEntries.get(this.lastQueryDeviceIndex);
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
