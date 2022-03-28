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
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Collections;

public class MemPageReader implements IPageReader {

  private final IPointReader timeValuePairIterator;
  private final IChunkMetadata chunkMetadata;
  private Filter valueFilter;

  public MemPageReader(
      IPointReader timeValuePairIterator, IChunkMetadata chunkMetadata, Filter filter) {
    this.timeValuePairIterator = timeValuePairIterator;
    this.chunkMetadata = chunkMetadata;
    this.valueFilter = filter;
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    TSDataType dataType = chunkMetadata.getDataType();
    BatchData batchData = BatchDataFactory.createBatchData(dataType, ascending, false);
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
      if (valueFilter == null
          || valueFilter.satisfy(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
      }
    }
    return batchData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData(boolean ascending) throws IOException {
    TSDataType dataType = chunkMetadata.getDataType();
    // TODO we still need to consider data type, ascending and descending here

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder valueBuilder = builder.getColumnBuilder(0);
    switch (dataType) {
      case BOOLEAN:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeBoolean(timeValuePair.getValue().getBoolean());
            builder.declarePosition();
          }
        }
        break;
      case INT32:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeInt(timeValuePair.getValue().getInt());
            builder.declarePosition();
          }
        }
        break;
      case INT64:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeLong(timeValuePair.getValue().getLong());
            builder.declarePosition();
          }
        }
        break;
      case FLOAT:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeFloat(timeValuePair.getValue().getFloat());
            builder.declarePosition();
          }
        }
        break;
      case DOUBLE:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeDouble(timeValuePair.getValue().getDouble());
            builder.declarePosition();
          }
        }
        break;
      case TEXT:
        while (timeValuePairIterator.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
          if (valueFilter == null
              || valueFilter.satisfy(
                  timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            timeBuilder.writeLong(timeValuePair.getTimestamp());
            valueBuilder.writeBinary(timeValuePair.getValue().getBinary());
            builder.declarePosition();
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
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
  public boolean isModified() {
    return false;
  }
}
