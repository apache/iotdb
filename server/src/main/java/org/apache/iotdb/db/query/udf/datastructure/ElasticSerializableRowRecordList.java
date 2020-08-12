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

package org.apache.iotdb.db.query.udf.datastructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.iterator.OverallRowRecordIterator;
import org.apache.iotdb.db.query.udf.iterator.RowRecordBatchIterator;
import org.apache.iotdb.db.query.udf.iterator.RowRecordIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class ElasticSerializableRowRecordList implements OverallRowRecordIterator {

  protected TSDataType[] dataTypes;
  protected long queryId;
  protected String dataId;
  protected int internalRowRecordListCapacity;
  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowRecordLists;
  protected List<Long> minTimestamps;
  protected int size;

  public ElasticSerializableRowRecordList(TSDataType[] dataTypes, long queryId, String dataId,
      float memoryLimitInMB, int cacheSize) throws QueryProcessException {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.dataId = dataId;
    int allocatableCapacity = SerializableRowRecordList
        .calculateCapacity(dataTypes, memoryLimitInMB);
    internalRowRecordListCapacity = allocatableCapacity / cacheSize;
    if (internalRowRecordListCapacity == 0) {
      cacheSize = 1;
      internalRowRecordListCapacity = allocatableCapacity;
    }
    cache = new ElasticSerializableRowRecordList.LRUCache(cacheSize);
    rowRecordLists = new ArrayList<>();
    minTimestamps = new ArrayList<>();
    size = 0;
  }

  @Override
  public RowRecordIterator getRowRecordIterator() {

    return new RowRecordIterator() {

      private int currentRowRecordIndex = -1;

      @Override
      public boolean hasNextRowRecord() {
        return currentRowRecordIndex < size - 1;
      }

      @Override
      public void next() {
        ++currentRowRecordIndex;
      }

      @Override
      public int currentRowRecordIndex() {
        return currentRowRecordIndex;
      }

      @Override
      public RowRecord currentRowRecord() throws IOException {
        return getRowRecord(currentRowRecordIndex);
      }

      @Override
      public long currentTime() throws IOException {
        return getRowRecord(currentRowRecordIndex).getTimestamp();
      }
    };
  }

  @Override
  public RowRecordBatchIterator getSizeLimitedBatchDataIterator(final int batchSize)
      throws QueryProcessException {
    if (batchSize <= 0) {
      throw new QueryProcessException("Batch size should be larger than 0.");
    }
    return new SizeLimitedBatchDataIterator(batchSize);
  }

  @Override
  public RowRecordBatchIterator getSizeLimitedBatchDataIterator(final int batchSize,
      final long displayWindowBegin) throws QueryProcessException {
    if (batchSize <= 0) {
      throw new QueryProcessException("Batch size should be larger than 0.");
    }
    try {
      return new SizeLimitedBatchDataIterator(batchSize, displayWindowBegin);
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  private class SizeLimitedBatchDataIterator implements RowRecordBatchIterator {

    private final int batchSize;
    private final int initialIndex;
    private final int totalBatch;
    private int currentBatchIndex;
    private int currentBatchSize;
    private int minIndexInCurrentBatch;

    SizeLimitedBatchDataIterator(int batchSize) {
      this.batchSize = batchSize;
      initialIndex = 0;
      totalBatch = (int) Math.ceil(((double) size) / batchSize);
      currentBatchIndex = -1;
      currentBatchSize = 0;
      minIndexInCurrentBatch = -batchSize;
    }

    SizeLimitedBatchDataIterator(int batchSize, long displayWindowBegin) throws IOException {
      this.batchSize = batchSize;
      initialIndex = findIndexByTimestamp(displayWindowBegin);
      totalBatch = (int) Math.ceil(((double) size - initialIndex) / batchSize);
      currentBatchIndex = -1;
      currentBatchSize = 0;
      minIndexInCurrentBatch = initialIndex - batchSize;
    }

    @Override
    public boolean hasNextBatch() {
      return currentBatchIndex < totalBatch - 1;
    }

    @Override
    public void next() {
      ++currentBatchIndex;
      minIndexInCurrentBatch += batchSize;
      int maxIndexInCurrentBatch = Math.min(size, minIndexInCurrentBatch + batchSize);
      currentBatchSize = maxIndexInCurrentBatch - minIndexInCurrentBatch;
    }

    @Override
    public int currentBatchIndex() {
      return currentBatchIndex;
    }

    @Override
    public RowRecordIterator currentBatch() {
      return getRowRecordIteratorInBatch(batchSize, minIndexInCurrentBatch);
    }

    @Override
    public int currentBatchSize() {
      return currentBatchSize;
    }

    @Override
    public long getTimeInCurrentBatch(int index) throws IOException {
      return getTime(minIndexInCurrentBatch + index);
    }

    @Override
    public RowRecord getRowRecordInCurrentBatch(int index) throws IOException {
      return getRowRecord(minIndexInCurrentBatch + index);
    }
  }

  private RowRecordIterator getRowRecordIteratorInBatch(int currentBatchSize,
      int minIndexInCurrentBatch) {

    return new RowRecordIterator() {

      private int currentRowRecordIndex = -1;

      @Override
      public boolean hasNextRowRecord() {
        return currentRowRecordIndex < currentBatchSize - 1;
      }

      @Override
      public void next() {
        ++currentRowRecordIndex;
      }

      @Override
      public int currentRowRecordIndex() {
        return currentRowRecordIndex;
      }

      @Override
      public RowRecord currentRowRecord() throws IOException {
        return getRowRecord(minIndexInCurrentBatch + currentRowRecordIndex);
      }

      @Override
      public long currentTime() throws IOException {
        return getTime(minIndexInCurrentBatch + currentRowRecordIndex);
      }
    };
  }

  @Override
  public RowRecordBatchIterator getTimeWindowBatchDataIterator(long timeInterval,
      long slidingStep) throws QueryProcessException {
    long displayWindowBegin = 0;
    long displayWindowEnd = 0;
    try {
      if (0 < size) {
        displayWindowBegin = getTime(0);
        displayWindowEnd = getTime(size - 1);
      }
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
    return getTimeWindowBatchDataIterator(displayWindowBegin, displayWindowEnd, timeInterval,
        slidingStep);
  }

  @Override
  public RowRecordBatchIterator getTimeWindowBatchDataIterator(long displayWindowBegin,
      long displayWindowEnd, long timeInterval, long slidingStep) throws QueryProcessException {
    if (displayWindowEnd < displayWindowBegin) {
      throw new QueryProcessException(
          "The beginning time of the display window should not be larger than the ending time.");
    }
    if (timeInterval <= 0) {
      throw new QueryProcessException("Time interval should be larger than 0.");
    }
    if (slidingStep <= 0) {
      throw new QueryProcessException("Time sliding step should be larger than 0.");
    }

    try {
      return new RowRecordBatchIterator() {

        private int currentBatchIndex = -1;
        private int minIndexInCurrentBatch = -1;
        private int currentBatchSize = 0;

        private long minTimeInNextBatch = displayWindowBegin;
        private int minIndexInNextBatch = findIndexByTimestamp(displayWindowBegin);

        @Override
        public boolean hasNextBatch() {
          return minTimeInNextBatch < displayWindowEnd;
        }

        @Override
        public void next() throws IOException {
          ++currentBatchIndex;
          long minTimeInCurrentBatch = minTimeInNextBatch;
          minIndexInCurrentBatch = minIndexInNextBatch;
          long maxTimeInCurrentBatch = minTimeInCurrentBatch + timeInterval;
          int maxIndexInCurrentBatch = findIndexByTimestamp(maxTimeInCurrentBatch);
          currentBatchSize = maxIndexInCurrentBatch - minIndexInCurrentBatch;

          minTimeInNextBatch = minTimeInCurrentBatch + slidingStep;
          minIndexInNextBatch = findIndexByTimestamp(minTimeInNextBatch, minIndexInCurrentBatch);
        }

        @Override
        public int currentBatchIndex() {
          return currentBatchIndex;
        }

        @Override
        public RowRecordIterator currentBatch() {
          return getRowRecordIteratorInBatch(currentBatchSize, minIndexInCurrentBatch);
        }

        @Override
        public int currentBatchSize() {
          return currentBatchSize;
        }

        @Override
        public long getTimeInCurrentBatch(int index) throws IOException {
          return getTime(minIndexInCurrentBatch + index);
        }

        @Override
        public RowRecord getRowRecordInCurrentBatch(int index) throws IOException {
          return getRowRecord(minIndexInCurrentBatch + index);
        }
      };
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  public OverallRowRecordIterator getOverallRowRecordIterator() {
    return this;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getTime(int index) throws IOException {
    return cache.get(index / internalRowRecordListCapacity)
        .getTime(index % internalRowRecordListCapacity);
  }

  @Override
  public RowRecord getRowRecord(int index) throws IOException {
    return cache.get(index / internalRowRecordListCapacity)
        .getRowRecord(index % internalRowRecordListCapacity);
  }

  public void put(RowRecord rowRecord) throws IOException {
    checkExpansion(rowRecord.getTimestamp());
    cache.get(size / internalRowRecordListCapacity).put(rowRecord);
    ++size;
  }

  private void checkExpansion(long timestamp) {
    if (size % internalRowRecordListCapacity == 0) {
      int index = rowRecordLists.size();
      rowRecordLists.add(SerializableRowRecordList
          .newSerializableRowRecordList(dataTypes, queryId, dataId, index));
      minTimestamps.add(timestamp);
    }
  }

  /**
   * @return Index value, whose corresponding timestamp is the first timestamp greater than or equal
   * to the parameter timestamp
   */
  private int findIndexByTimestamp(long timestamp) throws IOException {
    if (rowRecordLists.size() == 0 || timestamp < minTimestamps.get(0)) {
      return 0;
    }
    int outerIndex = rowRecordLists.size() - 1;
    for (; 0 <= outerIndex; --outerIndex) {
      if (minTimestamps.get(outerIndex) <= timestamp) {
        break;
      }
    }
    int begin = outerIndex * internalRowRecordListCapacity;
    int end = Math.min(size, begin + internalRowRecordListCapacity);
    int index = begin;
    for (; index < end; ++index) {
      if (timestamp <= getTime(index)) {
        break;
      }
    }
    return index;
  }

  private int findIndexByTimestamp(long timestamp, int indexBegin) throws IOException {
    assert indexBegin <= size;
    int outerIndex = indexBegin / internalRowRecordListCapacity;
    for (; outerIndex < rowRecordLists.size(); ++outerIndex) {
      if (timestamp <= minTimestamps.get(outerIndex)) {
        break;
      }
    }
    outerIndex = outerIndex == 0 ? 0 : outerIndex - 1;
    int index = Math.max(indexBegin, outerIndex * internalRowRecordListCapacity);
    for (; index < size; ++index) {
      if (timestamp <= getTime(index)) {
        break;
      }
    }
    return index;
  }

  /**
   * <b>Note: It's not thread safe.</b>
   */
  private class LRUCache {

    private final int capacity;
    private final LinkedList<Integer> cache;

    LRUCache(int capacity) {
      this.capacity = capacity;
      cache = new LinkedList<>();
    }

    public SerializableRowRecordList get(int targetIndex) throws IOException {
      if (rowRecordLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          rowRecordLists.get(lastIndex).serialize();
        }
        rowRecordLists.get(targetIndex).deserialize();
      }
      cache.addFirst(targetIndex);
      return rowRecordLists.get(targetIndex);
    }
  }
}
