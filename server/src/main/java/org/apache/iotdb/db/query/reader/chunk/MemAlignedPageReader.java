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
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

public class MemAlignedPageReader implements IPageReader, IAlignedPageReader {

  private final TsBlock tsBlock;
  private final AlignedChunkMetadata chunkMetadata;
  private Filter valueFilter;
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

  @Override
  public TsBlock getAllSatisfiedData() {
    builder.reset();

    boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];

    // Time column and first value column
    // if all the sub sensors' value are null in current time
    // or current row is not satisfied with the filter, just discard it
    // currently, if it's a value filter, it will only
    // accept AlignedPath with only one sub sensor
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    Column valueColumn = tsBlock.getColumn(0);
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      long time = tsBlock.getTimeByIndex(row);
      Object value = tsBlock.getColumn(0).getObject(row);
      boolean valueIsNull = tsBlock.getColumn(0).isNull(row);
      if ((valueFilter == null || !valueIsNull && valueFilter.satisfy(time, value))) {
        builder.getTimeColumnBuilder().write(timeColumn, row);
        if (!valueIsNull) {
          builder.getColumnBuilder(0).write(valueColumn, row);
        } else {
          builder.getColumnBuilder(0).appendNull();
        }
        satisfyInfo[row] = true;
        builder.declarePosition();
      }
    }

    // other value column
    for (int column = 1; column < tsBlock.getValueColumnCount(); column++) {
      valueColumn = tsBlock.getColumn(column);
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        if (satisfyInfo[row]) {
          if (!tsBlock.getColumn(column).isNull(row)) {
            builder.getColumnBuilder(column).write(valueColumn, row);
          } else {
            builder.getColumnBuilder(column).appendNull();
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

  @Override
  public void setFilter(Filter filter) {
    if (valueFilter == null) {
      this.valueFilter = filter;
    } else {
      valueFilter = new AndFilter(this.valueFilter, filter);
    }
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
