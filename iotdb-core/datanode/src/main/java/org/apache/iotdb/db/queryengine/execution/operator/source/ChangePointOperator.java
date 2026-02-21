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

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * ChangePointOperator removes consecutive identical values from a time series, keeping only the
 * first occurrence when a value changes. It leverages TsFile statistics (min == max) at
 * file/chunk/page level to skip segments where all values are the same as the previously seen
 * value, avoiding unnecessary I/O.
 */
@SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
public class ChangePointOperator extends AbstractDataSourceOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ChangePointOperator.class);

  private final TSDataType dataType;
  private final boolean canUseStatistics;

  private boolean finished = false;
  private boolean isFirstPoint = true;

  private boolean cacheBoolean;
  private int cacheInt;
  private long cacheLong;
  private float cacheFloat;
  private double cacheDouble;
  private Binary cacheBinary;

  public ChangePointOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      IFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      boolean canUseStatistics) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.dataType = seriesPath.getSeriesType();
    this.canUseStatistics = canUseStatistics;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  // ======================== Operator interface ========================

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();
      boolean noMoreData = false;

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
        noMoreData = true;
        break;
      } while (System.nanoTime() - start < maxRuntime
          && !resultTsBlockBuilder.isFull()
          && retainedTsBlock == null);

      finished = (resultTsBlockBuilder.isEmpty() && retainedTsBlock == null && noMoreData);
      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public void close() throws Exception {
    // no resources to release
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
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes());
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return seriesScanUtil.getTsDataTypeList();
  }

  // ======================== Hierarchical scanning with statistics ========================

  private Optional<Boolean> readFileData() throws IOException {
    Optional<Boolean> b = seriesScanUtil.hasNextFile();
    if (!b.isPresent() || !b.get()) {
      return b;
    }

    if (canUseStatistics && seriesScanUtil.canUseCurrentFileStatistics()) {
      Statistics fileStatistics = seriesScanUtil.currentFileStatistics(0);
      if (fileStatistics != null && isUniformSegment(fileStatistics)) {
        handleUniformSegment(
            seriesScanUtil.currentFileTimeStatistics().getStartTime(), fileStatistics);
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
      Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics(0);
      if (chunkStatistics != null && isUniformSegment(chunkStatistics)) {
        handleUniformSegment(
            seriesScanUtil.currentChunkTimeStatistics().getStartTime(), chunkStatistics);
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
      Statistics pageStatistics = seriesScanUtil.currentPageStatistics(0);
      if (pageStatistics != null && isUniformSegment(pageStatistics)) {
        handleUniformSegment(
            seriesScanUtil.currentPageTimeStatistics().getStartTime(), pageStatistics);
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

  /** Returns true if all values in the segment are identical (min == max). */
  private boolean isUniformSegment(Statistics statistics) {
    return statistics.getMinValue().equals(statistics.getMaxValue());
  }

  /**
   * Handles a uniform segment (all values identical). If this is the first point ever seen, or if
   * the uniform value differs from the cached value, emit a single change point. Otherwise skip.
   */
  private void handleUniformSegment(long startTime, Statistics statistics) {
    Object uniformValue = statistics.getMinValue();

    if (isFirstPoint) {
      isFirstPoint = false;
      updateCacheFromStatistics(uniformValue);
      emitPoint(startTime, uniformValue);
    } else if (!valueEqualsCached(uniformValue)) {
      updateCacheFromStatistics(uniformValue);
      emitPoint(startTime, uniformValue);
    } else {
      updateCacheFromStatistics(uniformValue);
    }
  }

  /** Emits a single (timestamp, value) pair into the result builder. */
  private void emitPoint(long timestamp, Object value) {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);
    timeColumnBuilder.writeLong(timestamp);

    switch (dataType) {
      case BOOLEAN:
        columnBuilder.writeBoolean((Boolean) value);
        break;
      case INT32:
        columnBuilder.writeInt(((Number) value).intValue());
        break;
      case INT64:
        columnBuilder.writeLong(((Number) value).longValue());
        break;
      case FLOAT:
        columnBuilder.writeFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(((Number) value).doubleValue());
        break;
      case TEXT:
        columnBuilder.writeBinary((Binary) value);
        break;
      default:
        break;
    }
    resultTsBlockBuilder.declarePosition();
  }

  /** Updates the typed cache fields from a statistics uniform value. */
  private void updateCacheFromStatistics(Object value) {
    switch (dataType) {
      case BOOLEAN:
        cacheBoolean = (Boolean) value;
        break;
      case INT32:
        cacheInt = ((Number) value).intValue();
        break;
      case INT64:
        cacheLong = ((Number) value).longValue();
        break;
      case FLOAT:
        cacheFloat = ((Number) value).floatValue();
        break;
      case DOUBLE:
        cacheDouble = ((Number) value).doubleValue();
        break;
      case TEXT:
        cacheBinary = (Binary) value;
        break;
      default:
        break;
    }
  }

  /** Checks whether the given statistics uniform value equals the cached value. */
  private boolean valueEqualsCached(Object value) {
    switch (dataType) {
      case BOOLEAN:
        return cacheBoolean == (Boolean) value;
      case INT32:
        return cacheInt == ((Number) value).intValue();
      case INT64:
        return cacheLong == ((Number) value).longValue();
      case FLOAT:
        return Float.compare(cacheFloat, ((Number) value).floatValue()) == 0;
      case DOUBLE:
        return Double.compare(cacheDouble, ((Number) value).doubleValue()) == 0;
      case TEXT:
        return cacheBinary != null && cacheBinary.equals(value);
      default:
        return false;
    }
  }

  // ======================== Raw data processing ========================

  /** Row-by-row change point detection on a TsBlock. */
  private void processRawData(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    Column timeColumn = tsBlock.getTimeColumn();
    Column valueColumn = tsBlock.getColumn(0);
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(0);

    switch (dataType) {
      case BOOLEAN:
        processBoolean(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      case INT32:
        processInt(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      case INT64:
        processLong(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      case FLOAT:
        processFloat(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      case DOUBLE:
        processDouble(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      case TEXT:
        processText(size, timeColumn, valueColumn, timeColumnBuilder, columnBuilder);
        break;
      default:
        break;
    }
  }

  private void processBoolean(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      boolean val = valueColumn.getBoolean(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheBoolean = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeBoolean(val);
        resultTsBlockBuilder.declarePosition();
      } else if (val != cacheBoolean) {
        cacheBoolean = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeBoolean(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }

  private void processInt(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      int val = valueColumn.getInt(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheInt = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeInt(val);
        resultTsBlockBuilder.declarePosition();
      } else if (val != cacheInt) {
        cacheInt = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeInt(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }

  private void processLong(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      long val = valueColumn.getLong(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheLong = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeLong(val);
        resultTsBlockBuilder.declarePosition();
      } else if (val != cacheLong) {
        cacheLong = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeLong(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }

  private void processFloat(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      float val = valueColumn.getFloat(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheFloat = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeFloat(val);
        resultTsBlockBuilder.declarePosition();
      } else if (Float.compare(val, cacheFloat) != 0) {
        cacheFloat = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeFloat(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }

  private void processDouble(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      double val = valueColumn.getDouble(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheDouble = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeDouble(val);
        resultTsBlockBuilder.declarePosition();
      } else if (Double.compare(val, cacheDouble) != 0) {
        cacheDouble = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeDouble(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }

  private void processText(
      int size,
      Column timeColumn,
      Column valueColumn,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder columnBuilder) {
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        continue;
      }
      Binary val = valueColumn.getBinary(i);
      if (isFirstPoint) {
        isFirstPoint = false;
        cacheBinary = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeBinary(val);
        resultTsBlockBuilder.declarePosition();
      } else if (!val.equals(cacheBinary)) {
        cacheBinary = val;
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.writeBinary(val);
        resultTsBlockBuilder.declarePosition();
      }
    }
  }
}
