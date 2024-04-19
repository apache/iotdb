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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableScanOperator extends AbstractDataSourceOperator {

  private final List<ColumnSchema> columnSchemas;

  private final int[] columnsIndexArray;

  private final int measurementColumnCount;

  private final List<DeviceEntry> deviceEntries;

  private final int deviceCount;

  private final Ordering scanOrder;
  private final SeriesScanOptions seriesScanOptions;

  private final List<String> measurementColumnNames;

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
      int measurementColumnCount,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.columnSchemas = columnSchemas;
    this.columnsIndexArray = columnsIndexArray;
    this.measurementColumnCount = measurementColumnCount;
    this.deviceEntries = deviceEntries;
    this.deviceCount = deviceEntries.size();
    this.scanOrder = scanOrder;
    this.seriesScanOptions = seriesScanOptions;
    this.measurementColumnNames = measurementColumnNames;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    this.currentDeviceIndex = 0;

    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            (1L + columnsIndexArray.length)
                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.maxTsBlockLineNum = maxTsBlockLineNum;

    this.seriesScanUtil = constructAlignedSeriesScanUtil(deviceEntries.get(currentDeviceIndex));
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

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          break;
        }

      } while (System.nanoTime() - start < maxRuntime
          && !measurementDataBuilder.isFull()
          && measurementDataBlock == null);

      // current device' data is consumed up
      if (measurementDataBuilder.isEmpty() && measurementDataBlock == null) {
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

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();
      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendToBuilder(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    if (measurementDataBuilder.isEmpty()
        && tsBlock.getPositionCount() >= measurementDataBuilder.getMaxTsBlockLineNumber()) {
      measurementDataBlock = tsBlock;
      return;
    }
    TimeColumnBuilder timeColumnBuilder = measurementDataBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    for (int i = 0; i < size; i++) {
      timeColumnBuilder.writeLong(timeColumn.getLong(i));
      measurementDataBuilder.declarePosition();
    }
    for (int columnIndex = 0, columnSize = tsBlock.getValueColumnCount();
        columnIndex < columnSize;
        columnIndex++) {
      appendOneColumn(columnIndex, tsBlock, size);
    }
  }

  private void appendOneColumn(int columnIndex, TsBlock tsBlock, int size) {
    ColumnBuilder columnBuilder = measurementDataBuilder.getColumnBuilder(columnIndex);
    Column column = tsBlock.getColumn(columnIndex);
    if (column.mayHaveNull()) {
      for (int i = 0; i < size; i++) {
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
      }
    } else {
      for (int i = 0; i < size; i++) {
        columnBuilder.write(column, i);
      }
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
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
              (String) currentDeviceEntry.getDeviceID().segment(columnsIndexArray[i] + 1);
          valueColumns[i] = getIdOrAttributeValueColumn(idColumnValue, positionCount);
          break;
        case ATTRIBUTE:
          String attributeColumnValue =
              currentDeviceEntry.getAttributeColumnValues().get(columnsIndexArray[i]);
          valueColumns[i] = getIdOrAttributeValueColumn(attributeColumnValue, positionCount);
          break;
        case MEASUREMENT:
          valueColumns[i] = measurementDataBlock.getColumn(columnsIndexArray[i]);
          break;
        case TIME:
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + columnSchemas.get(i).getColumnCategory());
      }
    }
    this.resultTsBlock =
        new TsBlock(positionCount, measurementDataBlock.getTimeColumn(), valueColumns);
  }

  private RunLengthEncodedColumn getIdOrAttributeValueColumn(String value, int positionCount) {
    if (value == null) {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.of(new boolean[] {true}), new Binary[] {null}),
          positionCount);
    } else {
      return new RunLengthEncodedColumn(
          new BinaryColumn(
              1, Optional.empty(), new Binary[] {new Binary(value, TSFileConfig.STRING_CHARSET)}),
          positionCount);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return currentDeviceIndex < deviceCount;
  }

  @Override
  public boolean isFinished() throws Exception {
    return currentDeviceIndex >= deviceCount;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return (1L + columnsIndexArray.length)
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()
        * 3L;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter();
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
    List<TSDataType> resultDataTypes = new ArrayList<>(columnSchemas.size());
    for (ColumnSchema columnSchema : columnSchemas) {
      if (columnSchema.getColumnCategory() != TsTableColumnCategory.TIME) {
        resultDataTypes.add(getTSDataType(columnSchema.getType()));
      } else {
        throw new IllegalArgumentException("Should not have TimeColumnSchema");
      }
    }
    return resultDataTypes;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    this.queryDataSource = dataSource;
    this.seriesScanUtil.initQueryDataSource(dataSource);
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    this.resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
    this.measurementDataBuilder = new TsBlockBuilder(this.measurementColumnTSDataTypes);
    this.measurementDataBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }

  private void prepareForNextDevice() {
    if (currentDeviceIndex < deviceCount) {
      // construct AlignedSeriesScanUtil for next device
      this.seriesScanUtil = constructAlignedSeriesScanUtil(deviceEntries.get(currentDeviceIndex));

      // reset QueryDataSource
      queryDataSource.reset();
      this.seriesScanUtil.initQueryDataSource(queryDataSource);
    }
  }

  private AlignedSeriesScanUtil constructAlignedSeriesScanUtil(DeviceEntry deviceEntry) {
    AlignedPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas);

    return new AlignedSeriesScanUtil(
        alignedPath,
        scanOrder,
        seriesScanOptions,
        operatorContext.getInstanceContext(),
        true,
        measurementColumnTSDataTypes);
  }

  public static AlignedPath constructAlignedPath(
      DeviceEntry deviceEntry,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas) {
    String[] devicePath = new String[1 + deviceEntry.getDeviceID().segmentNum()];
    devicePath[0] = "root";
    for (int i = 1; i < devicePath.length; i++) {
      devicePath[i] = (String) deviceEntry.getDeviceID().segment(i - 1);
    }
    AlignedPath alignedPath = new AlignedPath(new PartialPath(devicePath));

    alignedPath.setMeasurementList(measurementColumnNames);
    alignedPath.setSchemaList(measurementSchemas);
    return alignedPath;
  }
}
