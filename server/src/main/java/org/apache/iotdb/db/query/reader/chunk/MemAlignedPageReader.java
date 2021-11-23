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
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;

public class MemAlignedPageReader implements IPageReader, IAlignedPageReader {

  private final IPointReader timeValuePairIterator;
  private final AlignedChunkMetadata chunkMetadata;
  private Filter valueFilter;

  public MemAlignedPageReader(
      IPointReader timeValuePairIterator, AlignedChunkMetadata chunkMetadata, Filter filter) {
    this.timeValuePairIterator = timeValuePairIterator;
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
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
      TsPrimitiveType[] values = timeValuePair.getValue().getVector();
      // save the first not null value of each row
      Object firstNotNullObject = null;
      for (TsPrimitiveType value : values) {
        if (value != null) {
          firstNotNullObject = value.getValue();
          break;
        }
      }
      // if all the sub sensors' value are null in current time
      // or current row is not satisfied with the filter, just discard it
      // TODO fix value filter firstNotNullObject, currently, if it's a value filter, it will only
      // accept AlignedPath with only one sub sensor
      if (firstNotNullObject != null
          && (valueFilter == null
              || valueFilter.satisfy(timeValuePair.getTimestamp(), firstNotNullObject))) {
        batchData.putVector(timeValuePair.getTimestamp(), values);
      }
    }
    return batchData.flip();
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
}
