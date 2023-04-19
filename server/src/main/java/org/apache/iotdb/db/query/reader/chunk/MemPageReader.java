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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemPageReader implements IPageReader {

  private final TsBlock tsBlock;
  private final IChunkMetadata chunkMetadata;

  private Filter valueFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  public MemPageReader(TsBlock tsBlock, IChunkMetadata chunkMetadata, Filter filter) {
    this.tsBlock = tsBlock;
    this.chunkMetadata = chunkMetadata;
    this.valueFilter = filter;
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    TSDataType dataType = chunkMetadata.getDataType();
    BatchData batchData = BatchDataFactory.createBatchData(dataType, ascending, false);
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      if (valueFilter == null
          || valueFilter.satisfy(
              tsBlock.getTimeColumn().getLong(i), tsBlock.getColumn(0).getObject(i))) {
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

  private boolean pageSatisfy() {
    if (valueFilter != null) {
      return valueFilter.satisfy(getStatistics());
    } else {
      long rowCount = getStatistics().getCount();
      if (paginationController.hasCurOffset(rowCount)) {
        paginationController.consumeOffset(rowCount);
        return false;
      }
    }
    return true;
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    TSDataType dataType = chunkMetadata.getDataType();
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder valueBuilder = builder.getColumnBuilder(0);
    if (pageSatisfy()) {
      switch (dataType) {
        case BOOLEAN:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            boolean value = tsBlock.getColumn(0).getBoolean(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeBoolean(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case INT32:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            int value = tsBlock.getColumn(0).getInt(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeInt(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case INT64:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            long value = tsBlock.getColumn(0).getLong(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeLong(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case FLOAT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            float value = tsBlock.getColumn(0).getFloat(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeFloat(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case DOUBLE:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            double value = tsBlock.getColumn(0).getDouble(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeDouble(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case TEXT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            long time = tsBlock.getTimeColumn().getLong(i);
            Binary value = tsBlock.getColumn(0).getBinary(i);
            if (valueFilter != null && !valueFilter.satisfy(time, value)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(time);
              valueBuilder.writeBinary(value);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return builder.build();
  }

  @Override
  public Statistics getStatistics() {
    return chunkMetadata.getStatistics();
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
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {}
}
