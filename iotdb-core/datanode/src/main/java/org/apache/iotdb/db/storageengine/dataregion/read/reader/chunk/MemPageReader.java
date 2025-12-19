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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.PageMetadata;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemPageReader implements IPageReader {

  private TsBlock tsBlock;
  private Filter recordFilter;

  private final int pageIndex;
  private final Supplier<TsBlock> tsBlockSupplier;
  private final TSDataType tsDataType;
  private final PageMetadata pageMetadata;

  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  public MemPageReader(
      Supplier<TsBlock> tsBlockSupplier,
      int pageIndex,
      TSDataType tsDataType,
      String measurementUid,
      Statistics statistics,
      Filter recordFilter) {
    this.tsBlockSupplier = tsBlockSupplier;
    this.pageIndex = pageIndex;
    this.recordFilter = recordFilter;
    this.tsDataType = tsDataType;
    this.pageMetadata = new PageMetadata(measurementUid, tsDataType, statistics);
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    getTsBlock();

    BatchData batchData = BatchDataFactory.createBatchData(tsDataType, ascending, false);

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      if (satisfyInfo[i]) {
        switch (tsDataType) {
          case BOOLEAN:
            batchData.putBoolean(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getBoolean(i));
            break;
          case INT32:
          case DATE:
            batchData.putInt(tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getInt(i));
            break;
          case INT64:
          case TIMESTAMP:
            batchData.putLong(tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getLong(i));
            break;
          case DOUBLE:
            batchData.putDouble(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getDouble(i));
            break;
          case FLOAT:
            batchData.putFloat(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getFloat(i));
            break;
          case TEXT:
          case STRING:
          case BLOB:
          case OBJECT:
            batchData.putBinary(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getBinary(i));
            break;
          default:
            throw new UnSupportedDataTypeException(String.valueOf(tsDataType));
        }
      }
    }
    return batchData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    getTsBlock();

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    // build time column
    int readEndIndex = buildTimeColumn(builder, satisfyInfo);

    // build value column
    buildValueColumn(builder, satisfyInfo, readEndIndex);

    return builder.build();
  }

  private boolean[] buildSatisfyInfoArray() {
    if (recordFilter == null || recordFilter.allSatisfy(this)) {
      boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];
      Arrays.fill(satisfyInfo, true);
      return satisfyInfo;
    }
    return recordFilter.satisfyTsBlock(tsBlock);
  }

  private int buildTimeColumn(TsBlockBuilder builder, boolean[] satisfyInfo) {
    int readEndIndex = tsBlock.getPositionCount();
    for (int rowIndex = 0; rowIndex < readEndIndex; rowIndex++) {

      if (needSkipCurrentRow(satisfyInfo, rowIndex)) {
        continue;
      }

      if (paginationController.hasCurLimit()) {
        builder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(rowIndex));
        builder.declarePosition();
        paginationController.consumeLimit();
      } else {
        readEndIndex = rowIndex;
      }
    }
    return readEndIndex;
  }

  private boolean needSkipCurrentRow(boolean[] satisfyInfo, int rowIndex) {
    if (!satisfyInfo[rowIndex]) {
      return true;
    }
    if (paginationController.hasCurOffset()) {
      paginationController.consumeOffset();
      satisfyInfo[rowIndex] = false;
      return true;
    }
    return false;
  }

  private void buildValueColumn(TsBlockBuilder builder, boolean[] satisfyInfo, int readEndIndex) {
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      Column valueColumn = tsBlock.getColumn(column);
      ColumnBuilder valueBuilder = builder.getColumnBuilder(column);
      for (int row = 0; row < readEndIndex; row++) {
        if (satisfyInfo[row]) {
          if (!valueColumn.isNull(row)) {
            valueBuilder.write(valueColumn, row);
          } else {
            valueBuilder.appendNull();
          }
        }
      }
    }
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return pageMetadata.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return pageMetadata.getTimeStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    return pageMetadata.getMeasurementStatistics(measurementIndex);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return pageMetadata.hasNullValue(measurementIndex);
  }

  @Override
  public void addRecordFilter(Filter filter) {
    this.recordFilter = FilterFactory.and(recordFilter, filter);
  }

  @Override
  public void setLimitOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public void setModified(boolean modified) {}

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    // non-aligned page reader don't need to init TsBlockBuilder at the very beginning
  }

  private void getTsBlock() {
    if (tsBlock == null) {
      initializeTsBlockIndex();
      tsBlock = tsBlockSupplier.get();
      if (pageMetadata.getStatistics() == null) {
        initPageStatistics();
      }
    }
  }

  private void initializeTsBlockIndex() {
    if (tsBlockSupplier instanceof MemChunkReader.TsBlockSupplier) {
      ((MemChunkReader.TsBlockSupplier) tsBlockSupplier).setTsBlockIndex(pageIndex);
    }
  }

  // memory page statistics should be initialized when constructing ReadOnlyMemChunk object.
  // We do the initialization if it is not set, especially in test cases.
  private void initPageStatistics() {
    Statistics statistics = Statistics.getStatsByType(tsDataType);
    updatePageStatisticsFromTsBlock(statistics);
    statistics.setEmpty(tsBlock.isEmpty());
    pageMetadata.setStatistics(statistics);
  }

  private void updatePageStatisticsFromTsBlock(Statistics statistics) {
    if (!tsBlock.isEmpty()) {
      switch (tsDataType) {
        case BOOLEAN:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getBoolean(i));
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getBinary(i));
          }
          break;
        case FLOAT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getFloat(i));
          }
          break;
        case INT32:
        case DATE:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getInt(i));
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getLong(i));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statistics.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getDouble(i));
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
      }
    }
  }
}
