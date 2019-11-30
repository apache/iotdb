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
package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import java.util.Iterator;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.query.reader.IAggregateChunkReader;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.fileRelated.UnSealedTsFileIterateReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * To read chunk data in memory, this class implements two interfaces {@link IPointReader} and
 * {@link IAggregateReader} based on the data source {@link ReadOnlyMemChunk}.
 * <p>
 * This class is used in {@link UnSealedTsFileIterateReader} and {@link
 * org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader}.
 */
public class MemChunkReader implements IPointReader, IAggregateReader, IAggregateChunkReader {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private Filter filter;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  private TSDataType dataType;

  public MemChunkReader(ReadOnlyMemChunk readableChunk, Filter filter) {
    timeValuePairIterator = readableChunk.getIterator();
    this.filter = filter;
    this.dataType = readableChunk.getDataType();
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    while (timeValuePairIterator.hasNext()) {
      TimeValuePair timeValuePair = timeValuePairIterator.next();
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
  public TimeValuePair next() {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  @Override
  public TimeValuePair current() {
    if (!hasCachedTimeValuePair) {
      cachedTimeValuePair = timeValuePairIterator.next();
      hasCachedTimeValuePair = true;
    }
    return cachedTimeValuePair;
  }

  @Override
  public BatchData nextBatch() {
    BatchData batchData = new BatchData(dataType, true);
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      batchData.putTime(cachedTimeValuePair.getTimestamp());
      batchData.putAnObject(cachedTimeValuePair.getValue().getValue());
    }
    while (timeValuePairIterator.hasNext()) {
      TimeValuePair timeValuePair = timeValuePairIterator.next();
      if (filter == null || filter
          .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        batchData.putTime(timeValuePair.getTimestamp());
        batchData.putAnObject(timeValuePair.getValue().getValue());
      }
    }
    return batchData;
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public PageHeader nextPageHeader() {
    return null;
  }

  @Override
  public void skipPageData() {
    nextBatch();
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return false;
  }

  @Override
  public ChunkMetaData nextChunkMeta() {
    return null;
  }

  @Override
  public ChunkReader readChunk() {
    return null;
  }
}