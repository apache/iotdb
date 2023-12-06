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

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemAlignedPageReader implements IPageReader {

  private final TsBlock tsBlock;
  private final AlignedChunkMetadata chunkMetadata;

  private Filter globalTimeFilter;
  private Filter pushDownFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private TsBlockBuilder builder;

  public MemAlignedPageReader(
      TsBlock tsBlock, AlignedChunkMetadata chunkMetadata, Filter globalTimeFilter) {
    this.tsBlock = tsBlock;
    this.chunkMetadata = chunkMetadata;
    this.globalTimeFilter = globalTimeFilter;
  }

  @Override
  public BatchData getAllSatisfiedPageData() throws IOException {
    return IPageReader.super.getAllSatisfiedPageData();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData batchData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      // save the first not null value of each row
      Object firstNotNullObject = getFirstNotNullObject(row);
      // if all the sub sensors' value are null in current time
      // or current row is not satisfied with the filter, just discard it
      if (firstNotNullObject != null) {
        doFilter(firstNotNullObject, row, batchData);
      }
    }
    return batchData.flip();
  }

  private Object getFirstNotNullObject(int rowIndex) {
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      if (!tsBlock.getColumn(column).isNull(rowIndex)) {
        return tsBlock.getColumn(column).getObject(rowIndex);
      }
    }
    return null;
  }

  private void doFilter(Object row, int rowIndex, BatchData batchData) {
    if (globalTimeFilter == null
        || globalTimeFilter.satisfy(tsBlock.getTimeByIndex(rowIndex), row)) {
      TsPrimitiveType[] values = new TsPrimitiveType[tsBlock.getValueColumnCount()];
      for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
        if (tsBlock.getColumn(column) != null && !tsBlock.getColumn(column).isNull(rowIndex)) {
          values[column] = tsBlock.getColumn(column).getTsPrimitiveType(rowIndex);
        }
      }
      batchData.putVector(tsBlock.getTimeByIndex(rowIndex), values);
    }
  }

  private boolean pageCanSkip() {
    return globalTimeFilter != null && globalTimeFilter.canSkip(this);
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    builder.reset();
    if (pageCanSkip()) {
      return builder.build();
    }

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    boolean[] hasValue = buildHasValueArray();

    // build time column
    int readEndIndex = buildTimeColumn(satisfyInfo, hasValue);

    // build value column
    buildValueColumns(satisfyInfo, hasValue, readEndIndex);

    return builder.build();
  }

  private boolean[] buildSatisfyInfoArray() {
    boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      long time = tsBlock.getTimeByIndex(row);
      Object[] values = new Object[tsBlock.getValueColumnCount()];
      for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
        if (!tsBlock.getColumn(column).isNull(row)) {
          values[column] = tsBlock.getColumn(column).getObject(row);
        }
      }
      if ((globalTimeFilter == null || globalTimeFilter.satisfy(time, null))
          && (pushDownFilter == null || pushDownFilter.satisfy(time, values))) {
        satisfyInfo[row] = true;
      }
    }
    return satisfyInfo;
  }

  private boolean[] buildHasValueArray() {
    boolean[] hasValue = new boolean[tsBlock.getPositionCount()];
    // other value column
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      Column valueColumn = tsBlock.getColumn(column);
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        hasValue[row] = hasValue[row] || !valueColumn.isNull(row);
      }
    }
    return hasValue;
  }

  private int buildTimeColumn(boolean[] satisfyInfo, boolean[] hasValue) {
    int readEndIndex = tsBlock.getPositionCount();
    for (int row = 0; row < readEndIndex; row++) {

      if (needSkipCurrentRow(satisfyInfo, hasValue, row)) {
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

  private boolean needSkipCurrentRow(boolean[] satisfyInfo, boolean[] hasValue, int rowIndex) {
    if (!satisfyInfo[rowIndex] || !hasValue[rowIndex]) {
      return true;
    }
    if (paginationController.hasCurOffset()) {
      paginationController.consumeOffset();
      satisfyInfo[rowIndex] = false;
      return true;
    }
    return false;
  }

  private void buildValueColumns(boolean[] satisfyInfo, boolean[] hasValue, int readEndIndex) {
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      Column valueColumn = tsBlock.getColumn(column);
      ColumnBuilder valueBuilder = builder.getColumnBuilder(column);
      for (int row = 0; row < readEndIndex; row++) {
        if (satisfyInfo[row] && hasValue[row]) {
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
    return chunkMetadata.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return chunkMetadata.getTimeStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    return chunkMetadata.getMeasurementStatistics(measurementIndex);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return chunkMetadata.hasNullValue(measurementIndex);
  }

  @Override
  public void setFilter(Filter filter) {
    if (globalTimeFilter == null) {
      this.globalTimeFilter = filter;
    } else {
      globalTimeFilter = FilterFactory.and(this.globalTimeFilter, filter);
    }
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
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    builder = new TsBlockBuilder(dataTypes);
  }
}
