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
package org.apache.iotdb.db.query.reader.chunk;

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
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemAlignedPageReader implements IPageReader, IAlignedPageReader {

  private final TsBlock tsBlock;
  private final AlignedChunkMetadata chunkMetadata;

  private Filter valueFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private TsBlockBuilder builder;

  public MemAlignedPageReader(TsBlock tsBlock, AlignedChunkMetadata chunkMetadata, Filter filter) {
    this.tsBlock = tsBlock;
    this.chunkMetadata = chunkMetadata;
    this.valueFilter = filter;
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
      Object firstNotNullObject = null;
      for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
        if (!tsBlock.getColumn(column).isNull(row)) {
          firstNotNullObject = tsBlock.getColumn(column).getObject(row);
          break;
        }
      }
      // if all the sub sensors' value are null in current time
      // or current row is not satisfied with the filter, just discard it
      // TODO fix value filter firstNotNullObject, currently, if it's a value filter, it will only
      // accept AlignedPath with only one sub sensor
      if (firstNotNullObject != null
          && (valueFilter == null
              || valueFilter.satisfy(tsBlock.getTimeByIndex(row), firstNotNullObject))) {
        TsPrimitiveType[] values = new TsPrimitiveType[tsBlock.getValueColumnCount()];
        for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
          if (tsBlock.getColumn(column) != null && !tsBlock.getColumn(column).isNull(row)) {
            values[column] = tsBlock.getColumn(column).getTsPrimitiveType(row);
          }
        }
        batchData.putVector(tsBlock.getTimeByIndex(row), values);
      }
    }
    return batchData.flip();
  }

  private boolean pageSatisfy() {
    if (valueFilter != null) {
      // TODO accept valueStatisticsList to filter
      return valueFilter.satisfy(getStatistics());
    } else {
      // For aligned series, When we only query some measurements under an aligned device, if the
      // values of these queried measurements at a timestamp are all null, the timestamp will not be
      // selected.
      // NOTE: if we change the query semantic in the future for aligned series, we need to remove
      // this check here.
      long rowCount = getTimeStatistics().getCount();
      for (Statistics statistics : getValueStatisticsList()) {
        if (statistics == null || statistics.hasNullValue(rowCount)) {
          return true;
        }
      }
      // When the number of points in all value pages is the same as that in the time page, it means
      // that there is no null value, and all timestamps will be selected.
      if (paginationController.hasCurOffset(rowCount)) {
        paginationController.consumeOffset(rowCount);
        return false;
      }
    }
    return true;
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    builder.reset();
    if (!pageSatisfy()) {
      return builder.build();
    }

    boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];

    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      long time = tsBlock.getTimeByIndex(row);
      // ValueFilter in MPP will only contain time filter now.
      if ((valueFilter == null || valueFilter.satisfy(time, null))) {
        satisfyInfo[row] = true;
      }
    }

    boolean[] hasValue = new boolean[tsBlock.getPositionCount()];
    // other value column
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      Column valueColumn = tsBlock.getColumn(column);
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        hasValue[row] = hasValue[row] || !valueColumn.isNull(row);
      }
    }

    // build time column
    int readEndIndex = tsBlock.getPositionCount();
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      if (!satisfyInfo[row] || !hasValue[row]) {
        continue;
      }
      if (paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
        satisfyInfo[row] = false;
        continue;
      }
      if (paginationController.hasCurLimit()) {
        builder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(row));
        builder.declarePosition();
        paginationController.consumeLimit();
      } else {
        readEndIndex = row;
        break;
      }
    }

    // build value column
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

    return builder.build();
  }

  @Override
  public Statistics getStatistics() {
    return chunkMetadata.getStatistics();
  }

  @Override
  public Statistics getStatistics(int index) {
    return chunkMetadata.getStatistics(index);
  }

  @Override
  public Statistics getTimeStatistics() {
    return chunkMetadata.getTimeStatistics();
  }

  private List<Statistics> getValueStatisticsList() {
    return chunkMetadata.getValueStatisticsList();
  }

  @Override
  public void setFilter(Filter filter) {
    if (valueFilter == null) {
      this.valueFilter = filter;
    } else {
      valueFilter = new AndFilter(this.valueFilter, filter);
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
