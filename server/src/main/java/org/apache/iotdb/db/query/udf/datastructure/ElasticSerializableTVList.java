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
import org.apache.iotdb.db.query.udf.api.iterator.DataPointBatchIterator;
import org.apache.iotdb.db.query.udf.api.collector.DataPointCollector;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.query.udf.api.iterator.OverallDataPointIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;

public class ElasticSerializableTVList implements OverallDataPointIterator, DataPointCollector {

  protected TSDataType dataType;
  protected long queryId;
  protected Path path;
  protected int internalTVListCapacity;
  protected LRUCache cache;
  protected List<BatchData> tvLists;
  protected List<Long> minTimestamps;
  protected int size;

  public ElasticSerializableTVList(TSDataType dataType, long queryId, Path path,
      float memoryLimitInMB, int cacheSize) throws QueryProcessException {
    this.dataType = dataType;
    this.queryId = queryId;
    this.path = path;
    int allocatableCapacity = SerializableTVList.calculateCapacity(dataType, memoryLimitInMB);
    internalTVListCapacity = allocatableCapacity / cacheSize;
    if (internalTVListCapacity == 0) {
      cacheSize = 1;
      internalTVListCapacity = allocatableCapacity;
    }
    cache = new LRUCache(cacheSize);
    tvLists = new ArrayList<>();
    minTimestamps = new ArrayList<>();
    size = 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getTime(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getTimeByIndex(index % internalTVListCapacity);
  }

  @Override
  public int getInt(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getIntByIndex(index % internalTVListCapacity);
  }

  @Override
  public long getLong(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getLongByIndex(index % internalTVListCapacity);
  }

  @Override
  public float getFloat(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getFloatByIndex(index % internalTVListCapacity);
  }

  @Override
  public double getDouble(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getDoubleByIndex(index % internalTVListCapacity);
  }

  @Override
  public boolean getBoolean(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getBooleanByIndex(index % internalTVListCapacity);
  }

  @Override
  public Binary getBinary(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getBinaryByIndex(index % internalTVListCapacity);
  }

  @Override
  public String getString(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getBinaryByIndex(index % internalTVListCapacity).getStringValue();
  }

  @Override
  public void putInt(long timestamp, int value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putInt(timestamp, value);
    ++size;
  }

  @Override
  public void putLong(long timestamp, long value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putLong(timestamp, value);
    ++size;
  }

  @Override
  public void putFloat(long timestamp, float value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putFloat(timestamp, value);
    ++size;
  }

  @Override
  public void putDouble(long timestamp, double value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putDouble(timestamp, value);
    ++size;
  }

  @Override
  public void putBoolean(long timestamp, boolean value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putBoolean(timestamp, value);
    ++size;
  }

  @Override
  public void putBinary(long timestamp, Binary value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putBinary(timestamp, value);
    ++size;
  }

  @Override
  public void putString(long timestamp, String value) throws IOException {
    checkExpansion(timestamp);
    cache.get(size / internalTVListCapacity).putBinary(timestamp, Binary.valueOf(value));
    ++size;
  }

  private void checkExpansion(long timestamp) {
    if (size % internalTVListCapacity == 0) {
      int index = tvLists.size();
      tvLists.add(SerializableTVList.newSerializableTVList(dataType, queryId, path, index));
      minTimestamps.add(timestamp);
    }
  }

  /**
   * @return Index value, whose corresponding timestamp is the first timestamp greater than or equal
   * to the parameter timestamp
   */
  private int findIndexByTimestamp(long timestamp) throws IOException {
    if (tvLists.size() == 0 || timestamp < minTimestamps.get(0)) {
      return 0;
    }
    int outerIndex = tvLists.size() - 1;
    for (; 0 <= outerIndex; --outerIndex) {
      if (minTimestamps.get(outerIndex) <= timestamp) {
        break;
      }
    }
    int begin = outerIndex * internalTVListCapacity;
    int end = Math.min(size, begin + internalTVListCapacity);
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
    int outerIndex = indexBegin / internalTVListCapacity;
    for (; outerIndex < tvLists.size(); ++outerIndex) {
      if (timestamp <= minTimestamps.get(outerIndex)) {
        break;
      }
    }
    outerIndex = outerIndex == 0 ? 0 : outerIndex - 1;
    int index = Math.max(indexBegin, outerIndex * internalTVListCapacity);
    for (; index < size; ++index) {
      if (timestamp <= getTime(index)) {
        break;
      }
    }
    return index;
  }

  @Override
  public DataPointIterator getDataPointIterator() {

    return new DataPointIterator() {

      private int currentPointIndex = -1;

      @Override
      public boolean hasNextPoint() {
        return currentPointIndex < size - 1;
      }

      @Override
      public void next() {
        ++currentPointIndex;
      }

      @Override
      public int currentPointIndex() {
        return currentPointIndex;
      }

      @Override
      public long currentTime() throws IOException {
        return getTime(currentPointIndex);
      }

      @Override
      public int currentInt() throws IOException {
        return getInt(currentPointIndex);
      }

      @Override
      public long currentLong() throws IOException {
        return getLong(currentPointIndex);
      }

      @Override
      public float currentFloat() throws IOException {
        return getFloat(currentPointIndex);
      }

      @Override
      public double currentDouble() throws IOException {
        return getDouble(currentPointIndex);
      }

      @Override
      public boolean currentBoolean() throws IOException {
        return getBoolean(currentPointIndex);
      }

      @Override
      public Binary currentBinary() throws IOException {
        return getBinary(currentPointIndex);
      }

      @Override
      public String currentString() throws IOException {
        return getString(currentPointIndex);
      }

      @Override
      public void reset() {
        currentPointIndex = -1;
      }
    };
  }

  @Override
  public DataPointBatchIterator getSizeLimitedBatchIterator(final int batchSize)
      throws QueryProcessException {
    if (batchSize <= 0) {
      throw new QueryProcessException("Batch size should be larger than 0.");
    }
    return new SizeLimitedDataPointBatchIterator(batchSize);
  }

  @Override
  public DataPointBatchIterator getSizeLimitedBatchIterator(final int batchSize,
      final long displayWindowBegin) throws QueryProcessException {
    if (batchSize <= 0) {
      throw new QueryProcessException("Batch size should be larger than 0.");
    }
    try {
      return new SizeLimitedDataPointBatchIterator(batchSize, displayWindowBegin);
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  private class SizeLimitedDataPointBatchIterator implements DataPointBatchIterator {

    private final int batchSize;
    private final int initialIndex;
    private final int totalBatch;
    private int currentBatchIndex;
    private int currentBatchSize;
    private int minIndexInCurrentBatch;

    SizeLimitedDataPointBatchIterator(int batchSize) {
      this.batchSize = batchSize;
      initialIndex = 0;
      totalBatch = (int) Math.ceil(((double) size) / batchSize);
      currentBatchIndex = -1;
      currentBatchSize = 0;
      minIndexInCurrentBatch = -batchSize;
    }

    SizeLimitedDataPointBatchIterator(int batchSize, long displayWindowBegin)
        throws IOException {
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
    public DataPointIterator currentBatch() {
      return getDataPointIteratorInBatch(batchSize, minIndexInCurrentBatch);
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
    public int getIntInCurrentBatch(int index) throws IOException {
      return getInt(minIndexInCurrentBatch + index);
    }

    @Override
    public long getLongInCurrentBatch(int index) throws IOException {
      return getLong(minIndexInCurrentBatch + index);
    }

    @Override
    public boolean getBooleanInCurrentBatch(int index) throws IOException {
      return getBoolean(minIndexInCurrentBatch + index);
    }

    @Override
    public float getFloatInCurrentBatch(int index) throws IOException {
      return getFloat(minIndexInCurrentBatch + index);
    }

    @Override
    public double getDoubleInCurrentBatch(int index) throws IOException {
      return getDouble(minIndexInCurrentBatch + index);
    }

    @Override
    public Binary getBinaryInCurrentBatch(int index) throws IOException {
      return getBinary(minIndexInCurrentBatch + index);
    }

    @Override
    public String getStringInCurrentBatch(int index) throws IOException {
      return getString(minIndexInCurrentBatch + index);
    }

    @Override
    public void reset() {
      currentBatchIndex = -1;
      currentBatchSize = 0;
      minIndexInCurrentBatch = initialIndex - batchSize;
    }
  }

  @Override
  public DataPointBatchIterator getTimeWindowBatchIterator(final long timeInterval,
      final long slidingStep) throws QueryProcessException {
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
    return getTimeWindowBatchIterator(displayWindowBegin, displayWindowEnd, timeInterval,
        slidingStep);
  }

  @Override
  public DataPointBatchIterator getTimeWindowBatchIterator(final long displayWindowBegin,
      final long displayWindowEnd, final long timeInterval, final long slidingStep)
      throws QueryProcessException {
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
    return new DataPointBatchIterator() {

      private int currentBatchIndex = -1;
      private int minIndexInCurrentBatch = -1;
      private int currentBatchSize = 0;

      private long minTimeInNextBatch = displayWindowBegin;
      private int minIndexInNextBatch = initialIndex;

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
      public DataPointIterator currentBatch() {
        return getDataPointIteratorInBatch(currentBatchSize, minIndexInCurrentBatch);
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
      public int getIntInCurrentBatch(int index) throws IOException {
        return getInt(minIndexInCurrentBatch + index);
      }

      @Override
      public long getLongInCurrentBatch(int index) throws IOException {
        return getLong(minIndexInCurrentBatch + index);
      }

      @Override
      public boolean getBooleanInCurrentBatch(int index) throws IOException {
        return getBoolean(minIndexInCurrentBatch + index);
      }

      @Override
      public float getFloatInCurrentBatch(int index) throws IOException {
        return getFloat(minIndexInCurrentBatch + index);
      }

      @Override
      public double getDoubleInCurrentBatch(int index) throws IOException {
        return getDouble(minIndexInCurrentBatch + index);
      }

      @Override
      public Binary getBinaryInCurrentBatch(int index) throws IOException {
        return getBinary(minIndexInCurrentBatch + index);
      }

      @Override
      public String getStringInCurrentBatch(int index) throws IOException {
        return getString(minIndexInCurrentBatch + index);
      }

      @Override
      public void reset() {
        currentBatchIndex = -1;
        minIndexInCurrentBatch = -1;
        currentBatchSize = 0;

        minTimeInNextBatch = displayWindowBegin;
        minIndexInNextBatch = initialIndex;
      }
    };
  }

  private DataPointIterator getDataPointIteratorInBatch(int currentBatchSize,
      int minIndexInCurrentBatch) {

    return new DataPointIterator() {

      private int currentPointIndex = -1;

      @Override
      public boolean hasNextPoint() {
        return currentPointIndex < currentBatchSize - 1;
      }

      @Override
      public void next() {
        ++currentPointIndex;
      }

      @Override
      public int currentPointIndex() {
        return currentPointIndex;
      }

      @Override
      public long currentTime() throws IOException {
        return getTime(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public int currentInt() throws IOException {
        return getInt(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public long currentLong() throws IOException {
        return getLong(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public float currentFloat() throws IOException {
        return getFloat(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public double currentDouble() throws IOException {
        return getDouble(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public boolean currentBoolean() throws IOException {
        return getBoolean(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public Binary currentBinary() throws IOException {
        return getBinary(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public String currentString() throws IOException {
        return getString(minIndexInCurrentBatch + currentPointIndex);
      }

      @Override
      public void reset() {
        currentPointIndex = -1;
      }
    };
  }

  public OverallDataPointIterator asOverallDataPointIterator() {
    return this;
  }

  public DataPointCollector asDataPointCollector() {
    return this;
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

    public BatchData get(int targetIndex) throws IOException {
      if (tvLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          ((SerializableList) tvLists.get(lastIndex)).serialize();
        }
        ((SerializableList) tvLists.get(targetIndex)).deserialize();
      }
      cache.addFirst(targetIndex);
      return tvLists.get(targetIndex);
    }
  }
}
