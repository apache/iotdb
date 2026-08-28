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

import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryDataSourceLease;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.BatchDeviceEntrySource;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.InMemoryDeviceEntrySource;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator.appendDataIntoBuilder;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;

public abstract class AbstractTableScanOperator extends AbstractSeriesScanOperator {
  protected static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableScanOperator.class);

  private final List<ColumnSchema> columnSchemas;

  private final int[] columnsIndexArray;

  protected List<DeviceEntry> deviceEntries;

  protected int deviceCount;

  private final BatchDeviceEntrySource deviceEntrySource;
  private final boolean batchQueryDataSource;
  private QueryDataSourceLease currentLease;
  private boolean currentBatchInitialized;
  private boolean batchLeasePending;

  protected final Ordering scanOrder;
  protected final SeriesScanOptions seriesScanOptions;

  protected final List<String> measurementColumnNames;

  protected final Set<String> allSensors;

  protected final List<IMeasurementSchema> measurementSchemas;

  protected final List<TSDataType> measurementColumnTSDataTypes;

  private TsBlockBuilder measurementDataBuilder;

  private final int maxTsBlockLineNum;

  private TsBlock measurementDataBlock;

  protected QueryDataSource queryDataSource;

  protected int currentDeviceIndex;

  public AbstractTableScanOperator(AbstractTableScanOperatorParameter parameter) {
    this.sourceId = parameter.sourceId;
    this.operatorContext = parameter.context;
    this.operatorContext.recordSpecifiedInfo(
        DEVICE_NUMBER, Integer.toString(parameter.deviceCount));
    this.columnSchemas = parameter.columnSchemas;
    this.columnsIndexArray = parameter.columnsIndexArray;
    this.deviceEntrySource = parameter.deviceEntrySource;
    this.batchQueryDataSource = parameter.batchQueryDataSource;
    this.deviceEntries =
        parameter.batchQueryDataSource ? new ArrayList<>() : parameter.deviceEntries;
    this.deviceCount = parameter.batchQueryDataSource ? 0 : parameter.deviceCount;
    this.scanOrder = parameter.scanOrder;
    this.seriesScanOptions = parameter.seriesScanOptions;
    this.measurementColumnNames = parameter.measurementColumnNames;
    this.allSensors = parameter.allSensors;
    this.measurementSchemas = parameter.measurementSchemas;
    this.measurementColumnTSDataTypes =
        parameter.measurementSchemas.stream()
            .map(IMeasurementSchema::getType)
            .collect(Collectors.toList());
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(
        CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));

    // allSensors include time and all field columns
    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            allSensors.size() * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.maxTsBlockLineNum = parameter.maxTsBlockLineNum;
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    if (!prepareNextDeviceBatch()) {
      return null;
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
      } while (System.nanoTime() - start < maxRuntime
          && !measurementDataBuilder.isFull()
          && measurementDataBlock == null);

      // current device' data is consumed up
      if (measurementDataBuilder.isEmpty()
          && measurementDataBlock == null
          && currentDeviceNoMoreData) {
        moveToNextDevice();
      }

    } catch (IOException e) {
      throw new RuntimeException(DataNodeQueryMessages.ERROR_HAPPENED_WHILE_SCANNING_THE_FILE, e);
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
    DeviceEntry currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
    this.resultTsBlock =
        MeasurementToTableViewAdaptorUtils.toTableBlock(
            measurementDataBlock,
            columnsIndexArray,
            columnSchemas,
            deviceEntries.get(currentDeviceIndex),
            idColumnIndex -> getNthIdColumnValue(currentDeviceEntry, idColumnIndex));
  }

  abstract String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex);

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public boolean isFinished() throws Exception {
    if (retainedTsBlock != null) {
      return false;
    }
    if (seriesScanOptions.limitConsumedUp()) {
      return true;
    }
    if (batchLeasePending) {
      return false;
    }
    if (!batchQueryDataSource) {
      if (currentDeviceIndex >= deviceCount) {
        return true;
      }
      return shouldStopScanByRuntimeFilter();
    }
    return currentDeviceIndex >= deviceCount && !deviceEntrySource.hasNextBatch();
  }

  @Override
  public long calculateMaxPeekMemory() {
    // allSensors have included time column and all field columns
    return Math.max(
        maxReturnSize,
        allSensors.size() * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
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
    if (!batchQueryDataSource) {
      this.queryDataSource = (QueryDataSource) dataSource;
      if (this.seriesScanUtil != null) {
        this.seriesScanUtil.initQueryDataSource(this.queryDataSource);
      }
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    this.resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
    this.measurementDataBuilder = new TsBlockBuilder(this.measurementColumnTSDataTypes);
    this.measurementDataBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }

  protected void moveToNextDevice() {
    if (shouldStopScanByRuntimeFilter()) {
      currentDeviceIndex = deviceCount;
    } else {
      currentDeviceIndex++;
    }
    if (currentDeviceIndex < deviceCount && queryDataSource != null) {
      // construct AlignedSeriesScanUtil for next device
      constructAlignedSeriesScanUtil();

      // reset QueryDataSource
      queryDataSource.reset();
      this.seriesScanUtil.initQueryDataSource(queryDataSource);
      this.operatorContext.recordSpecifiedInfo(
          CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
    } else {
      releaseCurrentBatch();
    }
  }

  /** Returns true when file-level RF has pruned all seq/unseq files — scan can stop globally. */
  protected boolean shouldStopScanByRuntimeFilter() {
    return queryDataSource != null
        && seriesScanOptions.getTopKRuntimeFilter() != null
        && !queryDataSource.hasValidResource();
  }

  private boolean prepareNextDeviceBatch() throws Exception {
    if (currentBatchInitialized) {
      return currentDeviceIndex < deviceCount;
    }
    if (!batchQueryDataSource) {
      if (currentDeviceIndex >= deviceCount) {
        return false;
      }
      constructAlignedSeriesScanUtil();
      seriesScanUtil.initQueryDataSource(queryDataSource);
      currentBatchInitialized = true;
      return true;
    }
    while (deviceEntries.isEmpty()) {
      if (!deviceEntrySource.hasNextBatch()) {
        return false;
      }
      deviceEntries = deviceEntrySource.nextBatch();
    }
    deviceCount = deviceEntries.size();
    currentDeviceIndex = 0;
    if (batchQueryDataSource) {
      List<org.apache.iotdb.commons.path.IFullPath> paths = new ArrayList<>(deviceCount);
      for (DeviceEntry deviceEntry : deviceEntries) {
        paths.add(
            constructAlignedPath(
                deviceEntry, measurementColumnNames, measurementSchemas, allSensors));
      }
      currentLease =
          ((OperatorContext) operatorContext).getInstanceContext().initBatchQueryDataSource(paths);
      if (currentLease == null) {
        batchLeasePending = true;
        return false;
      }
      batchLeasePending = false;
      queryDataSource = currentLease.getDataSource();
    }
    constructAlignedSeriesScanUtil();
    seriesScanUtil.initQueryDataSource(queryDataSource);
    currentBatchInitialized = true;
    return true;
  }

  private void releaseCurrentBatch() {
    currentBatchInitialized = false;
    if (!batchQueryDataSource) {
      return;
    }
    deviceEntries = new ArrayList<>();
    deviceCount = 0;
    currentDeviceIndex = 0;
    queryDataSource = null;
    if (currentLease != null) {
      currentLease.close();
      currentLease = null;
    }
  }

  @Override
  public void close() throws Exception {
    releaseCurrentBatch();
    deviceEntrySource.close();
    super.close();
  }

  protected void constructAlignedSeriesScanUtil() {
    if (this.deviceEntries.isEmpty() || currentDeviceIndex >= deviceCount) {
      // no need to construct SeriesScanUtil, hasNext will return false
      return;
    }

    if (this.deviceEntries.get(this.currentDeviceIndex) == null) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_TABLESCANOPERATOR_IS_EMPTY_FDEB574F,
              this.currentDeviceIndex));
    }

    DeviceEntry deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);
    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas, allSensors);
    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            alignedPath,
            scanOrder,
            seriesScanOptions,
            ((OperatorContext) operatorContext).getInstanceContext(),
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

  public static class AbstractTableScanOperatorParameter {
    public final OperatorContext context;
    public final PlanNodeId sourceId;
    public final List<ColumnSchema> columnSchemas;
    public final int[] columnsIndexArray;
    public final List<DeviceEntry> deviceEntries;
    public final int deviceCount;
    public final BatchDeviceEntrySource deviceEntrySource;
    public final boolean batchQueryDataSource;
    public final Ordering scanOrder;
    public final SeriesScanOptions seriesScanOptions;
    public final List<String> measurementColumnNames;
    public final Set<String> allSensors;
    public final List<IMeasurementSchema> measurementSchemas;
    public final int maxTsBlockLineNum;

    public AbstractTableScanOperatorParameter(
        Set<String> allSensors,
        OperatorContext context,
        PlanNodeId sourceId,
        List<ColumnSchema> columnSchemas,
        int[] columnsIndexArray,
        List<DeviceEntry> deviceEntries,
        int deviceCount,
        BatchDeviceEntrySource deviceEntrySource,
        boolean batchQueryDataSource,
        Ordering scanOrder,
        SeriesScanOptions seriesScanOptions,
        List<String> measurementColumnNames,
        List<IMeasurementSchema> measurementSchemas,
        int maxTsBlockLineNum) {
      this.allSensors = allSensors;
      this.context = context;
      this.sourceId = sourceId;
      this.columnSchemas = columnSchemas;
      this.columnsIndexArray = columnsIndexArray;
      this.deviceEntries = deviceEntries;
      this.deviceCount = deviceCount;
      this.deviceEntrySource = deviceEntrySource;
      this.batchQueryDataSource = batchQueryDataSource;
      this.scanOrder = scanOrder;
      this.seriesScanOptions = seriesScanOptions;
      this.measurementColumnNames = measurementColumnNames;
      this.measurementSchemas = measurementSchemas;
      this.maxTsBlockLineNum = maxTsBlockLineNum;
    }

    public AbstractTableScanOperatorParameter(
        Set<String> allSensors,
        OperatorContext context,
        PlanNodeId sourceId,
        List<ColumnSchema> columnSchemas,
        int[] columnsIndexArray,
        List<DeviceEntry> deviceEntries,
        Ordering scanOrder,
        SeriesScanOptions seriesScanOptions,
        List<String> measurementColumnNames,
        List<IMeasurementSchema> measurementSchemas,
        int maxTsBlockLineNum) {
      this(
          allSensors,
          context,
          sourceId,
          columnSchemas,
          columnsIndexArray,
          deviceEntries,
          deviceEntries.size(),
          new InMemoryDeviceEntrySource(deviceEntries),
          false,
          scanOrder,
          seriesScanOptions,
          measurementColumnNames,
          measurementSchemas,
          maxTsBlockLineNum);
    }
  }
}
