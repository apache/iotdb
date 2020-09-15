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
import org.apache.iotdb.db.query.udf.api.iterator.OverallRowRecordIterator;
import org.apache.iotdb.db.query.udf.api.iterator.RowRecordWindowIterator;
import org.apache.iotdb.db.query.udf.api.iterator.RowRecordIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class ElasticSerializableRowRecordList implements OverallRowRecordIterator {

  public static final float MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN = 100;
  public static final int CACHE_SIZE_FOR_SINGLE_COLUMN = 3;

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
      public long currentTime() throws IOException {
        return getTime(currentRowRecordIndex);
      }

      @Override
      public RowRecord currentRowRecord() throws IOException {
        return getRowRecord(currentRowRecordIndex);
      }

      @Override
      public long nextTime() throws IOException {
        return getTime(currentRowRecordIndex + 1);
      }

      @Override
      public RowRecord nextRowRecord() throws IOException {
        return getRowRecord(currentRowRecordIndex + 1);
      }

      @Override
      public void reset() {
        currentRowRecordIndex = -1;
      }
    };
  }

  @Override
  public RowRecordWindowIterator getTumblingTimeWindowIterator(final int windowSize)
      throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException("Row record count should be larger than 0.");
    }
    return new TumblingTimeWindowIterator(windowSize);
  }

  @Override
  public RowRecordWindowIterator getTumblingTimeWindowIterator(final int windowSize,
      final long displayWindowBegin) throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException("Row record count should be larger than 0.");
    }
    try {
      return new TumblingTimeWindowIterator(windowSize, displayWindowBegin);
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  private class TumblingTimeWindowIterator implements RowRecordWindowIterator {

    private final int windowSize;
    private final int initialIndex;
    private final int totalWindow;
    private int currentWindowIndex;
    private int currentWindowSize;
    private int minIndexInCurrentWindow;

    TumblingTimeWindowIterator(int windowSize) {
      this.windowSize = windowSize;
      initialIndex = 0;
      totalWindow = (int) Math.ceil(((double) size) / windowSize);
      currentWindowIndex = -1;
      currentWindowSize = 0;
      minIndexInCurrentWindow = -windowSize;
    }

    TumblingTimeWindowIterator(int windowSize, long displayWindowBegin) throws IOException {
      this.windowSize = windowSize;
      initialIndex = findIndexByTimestamp(displayWindowBegin);
      totalWindow = (int) Math.ceil(((double) size - initialIndex) / windowSize);
      currentWindowIndex = -1;
      currentWindowSize = 0;
      minIndexInCurrentWindow = initialIndex - windowSize;
    }

    @Override
    public boolean hasNextWindow() {
      return currentWindowIndex < totalWindow - 1;
    }

    @Override
    public void next() {
      ++currentWindowIndex;
      minIndexInCurrentWindow += windowSize;
      int maxIndexInCurrentWindow = Math.min(size, minIndexInCurrentWindow + windowSize);
      currentWindowSize = maxIndexInCurrentWindow - minIndexInCurrentWindow;
    }

    @Override
    public int currentWindowIndex() {
      return currentWindowIndex;
    }

    @Override
    public RowRecordIterator currentWindow() {
      return getRowRecordIteratorInWindow(currentWindowSize, minIndexInCurrentWindow);
    }

    @Override
    public int currentWindowSize() {
      return currentWindowSize;
    }

    @Override
    public long getTimeInCurrentWindow(int index) throws IOException {
      return getTime(minIndexInCurrentWindow + index);
    }

    @Override
    public RowRecord getRowRecordInCurrentWindow(int index) throws IOException {
      return getRowRecord(minIndexInCurrentWindow + index);
    }

    @Override
    public void reset() {
      currentWindowIndex = -1;
      currentWindowSize = 0;
      minIndexInCurrentWindow = initialIndex - windowSize;
    }
  }

  private RowRecordIterator getRowRecordIteratorInWindow(int currentWindowSize,
      int minIndexInCurrentWindow) {

    return new RowRecordIterator() {

      private int currentRowRecordIndex = -1;

      @Override
      public boolean hasNextRowRecord() {
        return currentRowRecordIndex < currentWindowSize - 1;
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
        return getRowRecord(minIndexInCurrentWindow + currentRowRecordIndex);
      }

      @Override
      public long currentTime() throws IOException {
        return getTime(minIndexInCurrentWindow + currentRowRecordIndex);
      }

      @Override
      public RowRecord nextRowRecord() throws IOException {
        return getRowRecord(minIndexInCurrentWindow + currentRowRecordIndex + 1);
      }

      @Override
      public long nextTime() throws IOException {
        return getTime(minIndexInCurrentWindow + currentRowRecordIndex + 1);
      }

      @Override
      public void reset() {
        currentRowRecordIndex = -1;
      }
    };
  }

  @Override
  public RowRecordWindowIterator getSlidingTimeWindowIterator(long timeInterval, long slidingStep)
      throws QueryProcessException {
    long displayWindowBegin = 0;
    long displayWindowEnd = 0;
    try {
      if (0 < size) {
        displayWindowBegin = getTime(0);
        displayWindowEnd = getTime(size - 1) + 1; // +1 for right open interval
      }
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
    return getSlidingTimeWindowIterator(timeInterval, slidingStep, displayWindowBegin,
        displayWindowEnd);
  }

  @Override
  public RowRecordWindowIterator getSlidingTimeWindowIterator(long timeInterval, long slidingStep,
      long displayWindowBegin, long displayWindowEnd) throws QueryProcessException {
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

    int initialIndex;
    try {
      initialIndex = findIndexByTimestamp(displayWindowBegin);
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
    return new RowRecordWindowIterator() {

      private int currentWindowIndex = -1;
      private int minIndexInCurrentWindow = -1;
      private int currentWindowSize = 0;

      private long minTimeInNextWindow = displayWindowBegin;
      private int minIndexInNextWindow = initialIndex;

      @Override
      public boolean hasNextWindow() {
        return minTimeInNextWindow < displayWindowEnd;
      }

      @Override
      public void next() throws IOException {
        ++currentWindowIndex;
        long minTimeInCurrentWindow = minTimeInNextWindow;
        minIndexInCurrentWindow = minIndexInNextWindow;
        long maxTimeInCurrentWindow = Math
            .min(minTimeInCurrentWindow + timeInterval, displayWindowEnd);
        int maxIndexInCurrentWindow = findIndexByTimestamp(maxTimeInCurrentWindow);
        currentWindowSize = maxIndexInCurrentWindow - minIndexInCurrentWindow;

        minTimeInNextWindow = minTimeInCurrentWindow + slidingStep;
        minIndexInNextWindow = findIndexByTimestamp(minTimeInNextWindow, minIndexInCurrentWindow);
      }

      @Override
      public int currentWindowIndex() {
        return currentWindowIndex;
      }

      @Override
      public RowRecordIterator currentWindow() {
        return getRowRecordIteratorInWindow(currentWindowSize, minIndexInCurrentWindow);
      }

      @Override
      public int currentWindowSize() {
        return currentWindowSize;
      }

      @Override
      public long getTimeInCurrentWindow(int index) throws IOException {
        return getTime(minIndexInCurrentWindow + index);
      }

      @Override
      public RowRecord getRowRecordInCurrentWindow(int index) throws IOException {
        return getRowRecord(minIndexInCurrentWindow + index);
      }

      @Override
      public void reset() {
        currentWindowIndex = -1;
        minIndexInCurrentWindow = -1;
        currentWindowSize = 0;

        minTimeInNextWindow = displayWindowBegin;
        minIndexInNextWindow = initialIndex;
      }
    };
  }

  public OverallRowRecordIterator asOverallRowRecordIterator() {
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
