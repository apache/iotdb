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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MeasurementToTableViewAdaptorUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
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

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.MeasurementToTableViewAdaptorUtils.toTableBlock;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;

/**
 * ChangePointOperator performs "first of run" change-point detection pushed down to the table scan
 * level, leveraging TsFile statistics (min == max) at file/chunk/page level to skip uniform
 * segments and avoid unnecessary I/O.
 *
 * <p>For each consecutive run of identical values in the monitored column, only the first row is
 * emitted, along with the next different value (or NULL at end of partition). This replaces a
 * Filter(Window(LEAD(...))) pattern.
 *
 * <p>The output includes all table columns plus a "next" column.
 */
@SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
public class ChangePointOperator extends AbstractDataSourceOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ChangePointOperator.class);

  private final List<ColumnSchema> columnSchemas;
  private final int[] columnsIndexArray;
  private final List<DeviceEntry> deviceEntries;
  private final int deviceCount;
  private final Ordering scanOrder;
  private final SeriesScanOptions seriesScanOptions;
  private final List<String> measurementColumnNames;
  private final Set<String> allSensors;
  private final List<IMeasurementSchema> measurementSchemas;
  private final List<TSDataType> measurementColumnTsDataTypes;

  private final int monitoredMeasurementIndex;
  private final TSDataType monitoredDataType;
  private final boolean canUseStatistics;

  private int currentDeviceIndex;
  private boolean finished = false;

  private boolean hasBufferedRow = false;
  private long bufferedTime;
  private Object[] bufferedMeasurementValues;
  private Object cachedMonitoredValue;

  private TsBlockBuilder changePointBuilder;
  private List<Object> nextValues;

  private QueryDataSource queryDataSource;

  public ChangePointOperator(ChangePointOperatorParameter parameter) {
    this.sourceId = parameter.sourceId;
    this.operatorContext = parameter.context;
    this.columnSchemas = parameter.columnSchemas;
    this.columnsIndexArray = parameter.columnsIndexArray;
    this.deviceEntries = parameter.deviceEntries;
    this.deviceCount = parameter.deviceEntries.size();
    this.scanOrder = parameter.scanOrder;
    this.seriesScanOptions = parameter.seriesScanOptions;
    this.measurementColumnNames = parameter.measurementColumnNames;
    this.allSensors = parameter.allSensors;
    this.measurementSchemas = parameter.measurementSchemas;
    this.measurementColumnTsDataTypes =
        parameter.measurementSchemas.stream()
            .map(IMeasurementSchema::getType)
            .collect(Collectors.toList());
    this.monitoredMeasurementIndex = parameter.monitoredMeasurementIndex;
    this.monitoredDataType = measurementColumnTsDataTypes.get(monitoredMeasurementIndex);
    this.canUseStatistics = measurementColumnNames.size() == 1;
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(DEVICE_NUMBER, Integer.toString(deviceCount));
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.nextValues = new ArrayList<>();

    constructAlignedSeriesScanUtil();
  }

  // ======================== Operator interface ========================

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    if (this.seriesScanUtil != null) {
      this.seriesScanUtil.initQueryDataSource(queryDataSource);
    }
    List<TSDataType> changePointOutputTypes = new ArrayList<>(measurementColumnTsDataTypes);
    changePointOutputTypes.add(monitoredDataType);
    this.changePointBuilder = new TsBlockBuilder(changePointOutputTypes);
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    try {
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();
      boolean currentDeviceNoMoreData = false;

      do {
        if (readPageData()) {
          continue;
        }
        Optional<Boolean> b = readChunkData();
        if (!b.isPresent() || b.get()) {
          continue;
        }
        b = readFileData();
        if (!b.isPresent() || b.get()) {
          continue;
        }
        currentDeviceNoMoreData = true;
        break;
      } while (System.nanoTime() - start < maxRuntime && !changePointBuilder.isFull());

      if (currentDeviceNoMoreData) {
        flushBufferedRow();
        currentDeviceIndex++;
        prepareForNextDevice();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }

    if (changePointBuilder.isEmpty()) {
      return null;
    }

    TsBlock measurementBlock = changePointBuilder.build();
    changePointBuilder.reset();

    int rowCount = measurementBlock.getPositionCount();
    int measurementCount = measurementColumnTsDataTypes.size();
    Column nextColumn = measurementBlock.getColumn(measurementCount);

    Column[] measurementCols = new Column[measurementCount];
    for (int i = 0; i < measurementCount; i++) {
      measurementCols[i] = measurementBlock.getColumn(i);
    }
    TsBlock pureMeasurementBlock =
        new TsBlock(rowCount, measurementBlock.getTimeColumn(), measurementCols);

    DeviceEntry currentDeviceEntry = deviceEntries.get(Math.max(0, currentDeviceIndex - 1));
    TsBlock tableBlock =
        toTableBlock(
            pureMeasurementBlock,
            columnsIndexArray,
            columnSchemas,
            currentDeviceEntry,
            idColumnIndex -> getNthIdColumnValue(currentDeviceEntry, idColumnIndex));

    Column[] finalColumns = new Column[tableBlock.getValueColumnCount() + 1];
    for (int i = 0; i < tableBlock.getValueColumnCount(); i++) {
      finalColumns[i] = tableBlock.getColumn(i);
    }
    finalColumns[tableBlock.getValueColumnCount()] = nextColumn;

    resultTsBlock =
        new TsBlock(
            rowCount, new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, rowCount), finalColumns);
    nextValues.clear();
    return checkTsBlockSizeAndGetResult();
  }

  private String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return ((String) deviceEntry.getNthSegment(idColumnIndex + 1));
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished =
          (retainedTsBlock == null)
              && currentDeviceIndex >= deviceCount
              && changePointBuilder.isEmpty();
    }
    return finished;
  }

  @Override
  public void close() throws Exception {
    // no additional resources
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (changePointBuilder == null ? 0 : changePointBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    List<TSDataType> result = new ArrayList<>();
    for (ColumnSchema schema : columnSchemas) {
      result.add(
          org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType(
              schema.getType()));
    }
    result.add(monitoredDataType);
    return result;
  }

  // ======================== Device iteration ========================

  private void constructAlignedSeriesScanUtil() {
    DeviceEntry deviceEntry;
    if (this.deviceEntries.isEmpty() || this.deviceEntries.get(this.currentDeviceIndex) == null) {
      deviceEntry = new AlignedDeviceEntry(SeriesScanUtil.EMPTY_DEVICE_ID, new Binary[0]);
    } else {
      deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);
    }

    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas, allSensors);
    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            alignedPath,
            scanOrder,
            seriesScanOptions,
            operatorContext.getInstanceContext(),
            true,
            measurementColumnTsDataTypes);
  }

  private void prepareForNextDevice() {
    hasBufferedRow = false;
    cachedMonitoredValue = null;
    if (currentDeviceIndex < deviceCount) {
      constructAlignedSeriesScanUtil();
      if (queryDataSource != null) {
        seriesScanUtil.initQueryDataSource(queryDataSource);
      }
    }
  }

  // ======================== Hierarchical scanning with statistics ========================

  private Optional<Boolean> readFileData() throws IOException {
    Optional<Boolean> b = seriesScanUtil.hasNextFile();
    if (!b.isPresent() || !b.get()) {
      return b;
    }

    if (canUseStatistics && seriesScanUtil.canUseCurrentFileStatistics()) {
      Statistics fileStats = seriesScanUtil.currentFileStatistics(monitoredMeasurementIndex);
      if (fileStats != null && isUniformSegment(fileStats)) {
        handleUniformSegment(
            seriesScanUtil.currentFileTimeStatistics().getStartTime(),
            fileStats);
        seriesScanUtil.skipCurrentFile();
        return Optional.of(true);
      }
    }

    b = readChunkData();
    if (!b.isPresent() || b.get()) {
      return b;
    }
    return Optional.empty();
  }

  private Optional<Boolean> readChunkData() throws IOException {
    Optional<Boolean> b = seriesScanUtil.hasNextChunk();
    if (!b.isPresent() || !b.get()) {
      return b;
    }

    if (canUseStatistics && seriesScanUtil.canUseCurrentChunkStatistics()) {
      Statistics chunkStats = seriesScanUtil.currentChunkStatistics(monitoredMeasurementIndex);
      if (chunkStats != null && isUniformSegment(chunkStats)) {
        handleUniformSegment(
            seriesScanUtil.currentChunkTimeStatistics().getStartTime(),
            chunkStats);
        seriesScanUtil.skipCurrentChunk();
        return Optional.of(true);
      }
    }

    if (readPageData()) {
      return Optional.of(true);
    }
    return Optional.empty();
  }

  private boolean readPageData() throws IOException {
    if (!seriesScanUtil.hasNextPage()) {
      return false;
    }

    if (canUseStatistics && seriesScanUtil.canUseCurrentPageStatistics()) {
      Statistics pageStats = seriesScanUtil.currentPageStatistics(monitoredMeasurementIndex);
      if (pageStats != null && isUniformSegment(pageStats)) {
        handleUniformSegment(
            seriesScanUtil.currentPageTimeStatistics().getStartTime(),
            pageStats);
        seriesScanUtil.skipCurrentPage();
        return true;
      }
    }

    TsBlock tsBlock = seriesScanUtil.nextPage();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return true;
    }
    processRawData(tsBlock);
    return true;
  }

  // ======================== Statistics-based optimization ========================

  private boolean isUniformSegment(Statistics statistics) {
    return statistics.getMinValue().equals(statistics.getMaxValue());
  }

  /**
   * Handles a uniform segment. For "first of run": if the uniform value differs from cached, emit
   * the buffered row, then buffer the first row from this segment (reconstructed from statistics).
   * If same as cached, do nothing — the first row of the run is already buffered.
   */
  private void handleUniformSegment(long startTime, Statistics statistics) {
    Object uniformValue = statistics.getMinValue();

    if (!hasBufferedRow) {
      hasBufferedRow = true;
      bufferedTime = startTime;
      bufferedMeasurementValues = new Object[] {uniformValue};
      cachedMonitoredValue = uniformValue;
    } else if (!valuesEqual(cachedMonitoredValue, uniformValue)) {
      emitChangePointFromStatistics(uniformValue);
      bufferedTime = startTime;
      bufferedMeasurementValues = new Object[] {uniformValue};
      cachedMonitoredValue = uniformValue;
    }
  }

  private void emitChangePointFromStatistics(Object nextValue) {
    int measurementCount = measurementColumnTsDataTypes.size();
    changePointBuilder.getTimeColumnBuilder().writeLong(bufferedTime);
    writeValueToBuilder(
        changePointBuilder.getColumnBuilder(monitoredMeasurementIndex),
        monitoredDataType,
        bufferedMeasurementValues[0]);
    writeValueToBuilder(
        changePointBuilder.getColumnBuilder(measurementCount), monitoredDataType, nextValue);
    changePointBuilder.declarePosition();
  }

  // ======================== Raw data processing ========================

  private void processRawData(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    Column monitoredColumn = tsBlock.getColumn(monitoredMeasurementIndex);

    for (int i = 0; i < size; i++) {
      if (monitoredColumn.isNull(i)) {
        continue;
      }

      Object currentValue = getColumnValue(monitoredColumn, monitoredDataType, i);

      if (!hasBufferedRow) {
        bufferRow(tsBlock, i, currentValue);
        continue;
      }

      if (!valuesEqual(cachedMonitoredValue, currentValue)) {
        emitChangePointRow(currentValue);
        bufferRow(tsBlock, i, currentValue);
      }
    }
  }

  private void bufferRow(TsBlock tsBlock, int position, Object monitoredValue) {
    hasBufferedRow = true;
    bufferedTime = tsBlock.getTimeColumn().getLong(position);
    bufferedMeasurementValues = new Object[measurementColumnTsDataTypes.size()];
    for (int col = 0; col < measurementColumnTsDataTypes.size(); col++) {
      Column c = tsBlock.getColumn(col);
      if (c.isNull(position)) {
        bufferedMeasurementValues[col] = null;
      } else {
        bufferedMeasurementValues[col] =
            getColumnValue(c, measurementColumnTsDataTypes.get(col), position);
      }
    }
    cachedMonitoredValue = monitoredValue;
  }

  private void emitChangePointRow(Object nextValue) {
    int measurementCount = measurementColumnTsDataTypes.size();
    changePointBuilder.getTimeColumnBuilder().writeLong(bufferedTime);

    for (int col = 0; col < measurementCount; col++) {
      ColumnBuilder builder = changePointBuilder.getColumnBuilder(col);
      Object val = bufferedMeasurementValues[col];
      if (val == null) {
        builder.appendNull();
      } else {
        writeValueToBuilder(builder, measurementColumnTsDataTypes.get(col), val);
      }
    }

    ColumnBuilder nextBuilder = changePointBuilder.getColumnBuilder(measurementCount);
    writeValueToBuilder(nextBuilder, monitoredDataType, nextValue);
    changePointBuilder.declarePosition();
  }

  private void flushBufferedRow() {
    if (!hasBufferedRow) {
      return;
    }
    int measurementCount = measurementColumnTsDataTypes.size();
    changePointBuilder.getTimeColumnBuilder().writeLong(bufferedTime);

    for (int col = 0; col < measurementCount; col++) {
      ColumnBuilder builder = changePointBuilder.getColumnBuilder(col);
      Object val = bufferedMeasurementValues[col];
      if (val == null) {
        builder.appendNull();
      } else {
        writeValueToBuilder(builder, measurementColumnTsDataTypes.get(col), val);
      }
    }

    changePointBuilder.getColumnBuilder(measurementCount).appendNull();
    changePointBuilder.declarePosition();
    hasBufferedRow = false;
  }

  // ======================== Value utilities ========================

  private Object getColumnValue(Column column, TSDataType dataType, int position) {
    switch (dataType) {
      case BOOLEAN:
        return column.getBoolean(position);
      case INT32:
        return column.getInt(position);
      case INT64:
      case TIMESTAMP:
        return column.getLong(position);
      case FLOAT:
        return column.getFloat(position);
      case DOUBLE:
        return column.getDouble(position);
      case TEXT:
      case STRING:
      case BLOB:
        return column.getBinary(position);
      default:
        return null;
    }
  }

  private boolean valuesEqual(Object a, Object b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a instanceof Float && b instanceof Float) {
      return Float.compare((Float) a, (Float) b) == 0;
    }
    if (a instanceof Double && b instanceof Double) {
      return Double.compare((Double) a, (Double) b) == 0;
    }
    return a.equals(b);
  }

  private void writeValueToBuilder(ColumnBuilder builder, TSDataType dataType, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    switch (dataType) {
      case BOOLEAN:
        builder.writeBoolean((Boolean) value);
        break;
      case INT32:
        builder.writeInt(((Number) value).intValue());
        break;
      case INT64:
      case TIMESTAMP:
        builder.writeLong(((Number) value).longValue());
        break;
      case FLOAT:
        builder.writeFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        builder.writeDouble(((Number) value).doubleValue());
        break;
      case TEXT:
      case STRING:
      case BLOB:
        builder.writeBinary((Binary) value);
        break;
      default:
        builder.appendNull();
        break;
    }
  }

  // ======================== Parameter class ========================

  public static class ChangePointOperatorParameter {
    public final OperatorContext context;
    public final PlanNodeId sourceId;
    public final List<ColumnSchema> columnSchemas;
    public final int[] columnsIndexArray;
    public final List<DeviceEntry> deviceEntries;
    public final Ordering scanOrder;
    public final SeriesScanOptions seriesScanOptions;
    public final List<String> measurementColumnNames;
    public final Set<String> allSensors;
    public final List<IMeasurementSchema> measurementSchemas;
    public final int monitoredMeasurementIndex;

    public ChangePointOperatorParameter(
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
        int monitoredMeasurementIndex) {
      this.context = context;
      this.sourceId = sourceId;
      this.columnSchemas = columnSchemas;
      this.columnsIndexArray = columnsIndexArray;
      this.deviceEntries = deviceEntries;
      this.scanOrder = scanOrder;
      this.seriesScanOptions = seriesScanOptions;
      this.measurementColumnNames = measurementColumnNames;
      this.allSensors = allSensors;
      this.measurementSchemas = measurementSchemas;
      this.monitoredMeasurementIndex = monitoredMeasurementIndex;
    }
  }
}
