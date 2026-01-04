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

import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.AlignedPageMetadata;
import org.apache.iotdb.db.utils.CommonUtils;

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
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemAlignedPageReader implements IPageReader {

  private TsBlock tsBlock;
  private final AlignedPageMetadata pageMetadata;

  private final int pageIndex;
  private final Supplier<TsBlock> tsBlockSupplier;
  private final List<TSDataType> tsDataTypes;

  private Filter recordFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private TsBlockBuilder builder;

  public MemAlignedPageReader(
      Supplier<TsBlock> tsBlockSupplier,
      int pageIndex,
      List<TSDataType> tsDataTypes,
      Statistics<? extends Serializable> timeStatistics,
      Statistics<? extends Serializable>[] valueStatistics,
      Filter recordFilter) {
    this.tsBlockSupplier = tsBlockSupplier;
    this.pageIndex = pageIndex;
    this.recordFilter = recordFilter;
    this.tsDataTypes = tsDataTypes;
    this.pageMetadata = new AlignedPageMetadata(timeStatistics, valueStatistics);
  }

  @Override
  public BatchData getAllSatisfiedPageData() throws IOException {
    return IPageReader.super.getAllSatisfiedPageData();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    getTsBlock();

    BatchData batchData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    for (int rowIndex = 0; rowIndex < tsBlock.getPositionCount(); rowIndex++) {
      if (satisfyInfo[rowIndex]) {
        long time = tsBlock.getTimeByIndex(rowIndex);
        TsPrimitiveType[] values = new TsPrimitiveType[tsBlock.getValueColumnCount()];
        for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
          if (tsBlock.getColumn(column) != null && !tsBlock.getColumn(column).isNull(rowIndex)) {
            values[column] = tsBlock.getColumn(column).getTsPrimitiveType(rowIndex);
          }
        }
        batchData.putVector(time, values);
      }
    }
    return batchData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    getTsBlock();

    builder.reset();

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    // build time column
    int readEndIndex = buildTimeColumn(satisfyInfo);

    // build value column
    buildValueColumns(satisfyInfo, readEndIndex);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[memAlignedPageReader] TsBlock:{}", CommonUtils.toString(tsBlock));
    }
    return builder.build();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MemAlignedPageReader.class);

  private boolean[] buildSatisfyInfoArray() {
    if (recordFilter == null || recordFilter.allSatisfy(this)) {
      boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];
      Arrays.fill(satisfyInfo, true);
      return satisfyInfo;
    }
    return recordFilter.satisfyTsBlock(tsBlock);
  }

  private int buildTimeColumn(boolean[] satisfyInfo) {
    int readEndIndex = tsBlock.getPositionCount();
    for (int row = 0; row < readEndIndex; row++) {

      if (needSkipCurrentRow(satisfyInfo, row)) {
        continue;
      }

      if (paginationController.hasCurLimit()) {
        builder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(row));
        builder.declarePosition();
        paginationController.consumeLimit();
      } else {
        readEndIndex = row;
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

  private void buildValueColumns(boolean[] satisfyInfo, int readEndIndex) {
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
    builder = new TsBlockBuilder(dataTypes);
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
    if (tsBlockSupplier instanceof MemAlignedChunkReader.TsBlockSupplier) {
      ((MemAlignedChunkReader.TsBlockSupplier) tsBlockSupplier).setTsBlockIndex(pageIndex);
    }
  }

  private void initPageStatistics() {
    Statistics<? extends Serializable> pageTimeStatistics =
        Statistics.getStatsByType(TSDataType.VECTOR);
    Statistics<? extends Serializable>[] pageValueStatistics = new Statistics[tsDataTypes.size()];
    for (int column = 0; column < tsDataTypes.size(); column++) {
      Statistics<? extends Serializable> valueStatistics =
          Statistics.getStatsByType(tsDataTypes.get(column));
      pageValueStatistics[column] = valueStatistics;
    }
    updatePageStatisticsFromTsBlock(pageTimeStatistics, pageValueStatistics);
    pageMetadata.setStatistics(pageTimeStatistics, pageValueStatistics);
  }

  private void updatePageStatisticsFromTsBlock(
      Statistics<? extends Serializable> timeStatistics,
      Statistics<? extends Serializable>[] valueStatistics) {
    if (!tsBlock.isEmpty()) {
      // update time statistics
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        timeStatistics.update(tsBlock.getTimeByIndex(i));
      }
      //      timeStatistics.setEmpty(false);

      for (int column = 0; column < tsDataTypes.size(); column++) {
        switch (tsDataTypes.get(column)) {
          case BOOLEAN:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBoolean(i));
            }
            break;
          case INT32:
          case DATE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getInt(i));
            }
            break;
          case INT64:
          case TIMESTAMP:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getLong(i));
            }
            break;
          case FLOAT:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getFloat(i));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getDouble(i));
            }
            break;
          case TEXT:
          case BLOB:
          case OBJECT:
          case STRING:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              valueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBinary(i));
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", tsDataTypes.get(column)));
        }
      }
    }
  }
}
