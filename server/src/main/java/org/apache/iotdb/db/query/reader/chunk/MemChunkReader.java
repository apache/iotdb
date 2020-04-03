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

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * To read chunk data in memory
 */
public class MemChunkReader implements IChunkReader, IPointReader {

  private ReadOnlyMemChunk readOnlyMemChunk;
  private IPointReader timeValuePairIterator;
  private Filter filter;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  private TSDataType dataType;

  public MemChunkReader(ReadOnlyMemChunk readableChunk, Filter filter) {
    this.readOnlyMemChunk = readableChunk;
    timeValuePairIterator = readableChunk.getPointReader();
    this.filter = filter;
    this.dataType = readableChunk.getDataType();
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
      if (filter == null || filter
          .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return hasCachedTimeValuePair;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.nextTimeValuePair();
    }
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    if (!hasCachedTimeValuePair) {
      cachedTimeValuePair = timeValuePairIterator.nextTimeValuePair();
      hasCachedTimeValuePair = true;
    }
    return cachedTimeValuePair;
  }

  @Override
  public boolean hasNextSatisfiedPage() throws IOException {
    return hasNextTimeValuePair();
  }

  @Override
  public BatchData nextPageData() throws IOException {
    BatchData batchData = new BatchData(dataType);
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      batchData.putAnObject(cachedTimeValuePair.getTimestamp(),
          cachedTimeValuePair.getValue().getValue());
    }
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
      if (filter == null || filter
          .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
      }
    }
    return batchData;
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public List<IPageReader> loadPageReaderList() throws IOException {
    // we treat one ReadOnlyMemChunk as one Page
    return Collections.singletonList(
        new MemPageReader(nextPageData(), readOnlyMemChunk.getChunkMetaData().getStatistics()));
  }
}