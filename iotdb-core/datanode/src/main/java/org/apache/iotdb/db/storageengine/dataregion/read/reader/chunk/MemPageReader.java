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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemPageReader implements IPageReader {

  private final TsBlock tsBlock;
  private final IChunkMetadata chunkMetadata;

  private Filter recordFilter;

  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  public MemPageReader(TsBlock tsBlock, IChunkMetadata chunkMetadata, Filter recordFilter) {
    this.tsBlock = tsBlock;
    this.chunkMetadata = chunkMetadata;
    this.recordFilter = recordFilter;
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    TSDataType dataType = chunkMetadata.getDataType();
    BatchData batchData = BatchDataFactory.createBatchData(dataType, ascending, false);

    boolean[] satisfyInfo = buildSatisfyInfoArray();

    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      if (satisfyInfo[i]) {
        switch (dataType) {
          case BOOLEAN:
            batchData.putBoolean(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getBoolean(i));
            break;
          case INT32:
            batchData.putInt(tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getInt(i));
            break;
          case INT64:
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
            batchData.putBinary(
                tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getBinary(i));
            break;
          default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
      }
    }
    return batchData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    TsBlockBuilder builder =
        new TsBlockBuilder(Collections.singletonList(chunkMetadata.getDataType()));

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
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    // non-aligned page reader don't need to init TsBlockBuilder at the very beginning
  }
}
