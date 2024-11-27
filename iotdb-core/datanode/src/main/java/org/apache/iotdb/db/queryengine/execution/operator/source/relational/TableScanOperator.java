/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator.appendDataIntoBuilder;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableScanOperator extends AbstractSeriesScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableScanOperator.class);

  public static final String CURRENT_DEVICE_INDEX_STRING = "CurrentDeviceIndex";

  public static final LongColumn TIME_COLUMN_TEMPLATE =
      new LongColumn(1, Optional.empty(), new long[] {0});

  private final List<ColumnSchema> columnSchemas;

  private final int[] columnsIndexArray;

  private final List<DeviceEntry> deviceEntries;

  private final int deviceCount;

  private final Ordering scanOrder;
  private final SeriesScanOptions seriesScanOptions;

  private final List<String> measurementColumnNames;

  private final Set<String> allSensors;

  private final List<IMeasurementSchema> measurementSchemas;

  private final List<TSDataType> measurementColumnTSDataTypes;

  private TsBlockBuilder measurementDataBuilder;

  private final int maxTsBlockLineNum;

  private TsBlock measurementDataBlock;

  private QueryDataSource queryDataSource;

  private int currentDeviceIndex;

  public TableScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.operatorContext.recordSpecifiedInfo(DEVICE_NUMBER, Integer.toString(deviceEntries.size()));
    this.columnSchemas = columnSchemas;
    this.columnsIndexArray = columnsIndexArray;
    this.deviceEntries = deviceEntries;
    this.deviceCount = deviceEntries.size();
    this.scanOrder = scanOrder;
    this.seriesScanOptions = seriesScanOptions;
    this.measurementColumnNames = measurementColumnNames;
    this.allSensors = allSensors;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));

    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            (1L + columnsIndexArray.length)
                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.maxTsBlockLineNum = maxTsBlockLineNum;

    constructAlignedSeriesScanUtil();
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      boolean currentDeviceNoMoreData = false;

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          currentDeviceNoMoreData = true;
          break;
        }

      } while (System.nanoTime() - start < maxRuntime
          && !measurementDataBuilder.isFull()
          && measurementDataBlock == null);

      // current device' data is consumed up
      if (measurementDataBuilder.isEmpty()
          && measurementDataBlock == null
          && currentDeviceNoMoreData) {
        currentDeviceIndex++;
        prepareForNextDevice();
      }

    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }

    // get all measurement column data and time column data
    if (!measurementDataBuilder.isEmpty()) {
      measurementDataBlock = measurementDataBuilder.build();
      measurementDataBuilder.reset();
    }

    // append id column and attribute column
    if (!isEmpty(measurementDataBlock)) {
      constructResultTsBlock();
    } else {
      return null;
    }
    measurementDataBlock = null;
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  protected void appendToBuilder(TsBlock tsBlock) {
    if (measurementDataBuilder.isEmpty()
        && tsBlock.getPositionCount() >= measurementDataBuilder.getMaxTsBlockLineNumber()) {
      measurementDataBlock = tsBlock;
      return;
    }
    appendDataIntoBuilder(tsBlock, measurementDataBuilder);
  }

  @Override
  protected void buildResult(TsBlock tsBlock) {
    throw new UnsupportedOperationException();
  }

  private void constructResultTsBlock() {
    int positionCount = measurementDataBlock.getPositionCount();
    DeviceEntry currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
    Column[] valueColumns = new Column[columnsIndexArray.length];
    for (int i = 0; i < columnsIndexArray.length; i++) {
      switch (columnSchemas.get(i).getColumnCategory()) {
        case ID:
          // +1 for skip the table name segment
          String idColumnValue =
              ((String) currentDeviceEntry.getNthSegment(columnsIndexArray[i] + 1));
          valueColumns[i] =
              getIdOrAttributeValueColumn(
                  idColumnValue == null
                      ? null
                      : new Binary(idColumnValue, TSFileConfig.STRING_CHARSET),
                  positionCount);
          break;
        case ATTRIBUTE:
          Binary attributeColumnValue =
              currentDeviceEntry.getAttributeColumnValues().get(columnsIndexArray[i]);
          valueColumns[i] = getIdOrAttributeValueColumn(attributeColumnValue, positionCount);
          break;
        case MEASUREMENT:
          valueColumns[i] = measurementDataBlock.getColumn(columnsIndexArray[i]);
          break;
        case TIME:
          valueColumns[i] = measurementDataBlock.getTimeColumn();
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + columnSchemas.get(i).getColumnCategory());
      }
    }
    this.resultTsBlock =
        new TsBlock(
            positionCount,
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount),
            valueColumns);
  }

  private RunLengthEncodedColumn getIdOrAttributeValueColumn(Binary value, int positionCount) {
    if (value == null) {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.of(new boolean[] {true}), new Binary[] {null}),
          positionCount);
    } else {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.empty(), new Binary[] {value}), positionCount);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public boolean isFinished() throws Exception {
    return (retainedTsBlock == null)
        && (currentDeviceIndex >= deviceCount || seriesScanOptions.limitConsumedUp());
  }

  @Override
  public long calculateMaxPeekMemory() {
    return (1L + columnsIndexArray.length)
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
    List<TSDataType> resultDataTypes = new ArrayList<>(columnSchemas.size());
    for (ColumnSchema columnSchema : columnSchemas) {
      resultDataTypes.add(getTSDataType(columnSchema.getType()));
    }
    return resultDataTypes;
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    if (this.seriesScanUtil != null) {
      this.seriesScanUtil.initQueryDataSource(queryDataSource);
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    this.resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
    this.measurementDataBuilder = new TsBlockBuilder(this.measurementColumnTSDataTypes);
    this.measurementDataBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }

  private void prepareForNextDevice() {
    if (currentDeviceIndex < deviceCount) {
      // construct AlignedSeriesScanUtil for next device
      constructAlignedSeriesScanUtil();

      // reset QueryDataSource
      queryDataSource.reset();
      this.seriesScanUtil.initQueryDataSource(queryDataSource);
      this.operatorContext.recordSpecifiedInfo(
          CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
    }
  }

  private void constructAlignedSeriesScanUtil() {
    if (this.deviceEntries.isEmpty()) {
      // no need to construct SeriesScanUtil, hasNext will return false
      return;
    }

    if (this.deviceEntries.get(this.currentDeviceIndex) == null) {
      throw new IllegalStateException(
          "Device entries of index " + this.currentDeviceIndex + " in TableScanOperator is empty");
    }

    DeviceEntry deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);
    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas, allSensors);
    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            alignedPath,
            scanOrder,
            seriesScanOptions,
            operatorContext.getInstanceContext(),
            true,
            measurementColumnTSDataTypes);
  }

  public static AlignedFullPath constructAlignedPath(
      DeviceEntry deviceEntry,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors) {
    return new AlignedFullPath(
        deviceEntry.getDeviceID(), measurementColumnNames, measurementSchemas, allSensors);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
  }
}
