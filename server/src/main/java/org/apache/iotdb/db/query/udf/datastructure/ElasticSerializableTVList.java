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
import org.apache.iotdb.db.query.udf.api.collector.DataPointCollector;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointWindowIterator;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.query.udf.api.iterator.OverallDataPointIterator;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

public class ElasticSerializableTVList implements OverallDataPointIterator, DataPointCollector {

  public static final float MEMORY_USAGE_LIMIT_FOR_SINGLE_COLUMN = 100;
  public static final int CACHE_SIZE_FOR_SINGLE_COLUMN = 3;

  protected TSDataType dataType;
  protected long queryId;
  protected String uniqueId;
  protected int internalTVListCapacity;
  protected LRUCache cache;
  protected List<BatchData> tvLists;
  protected List<Long> minTimestamps;
  protected int size;

  public ElasticSerializableTVList(TSDataType dataType, long queryId, String uniqueId,
      float memoryLimitInMB, int cacheSize) throws QueryProcessException {
    this.dataType = dataType;
    this.queryId = queryId;
    this.uniqueId = uniqueId;
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

  public TSDataType getDataType() {
    return dataType;
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

  public void put(long timestamp, Object value) throws IOException {
    switch (dataType) {
      case INT32:
        putInt(timestamp, (Integer) value);
        break;
      case INT64:
        putLong(timestamp, (Long) value);
        break;
      case FLOAT:
        putFloat(timestamp, (Float) value);
        break;
      case DOUBLE:
        putDouble(timestamp, (Double) value);
        break;
      case BOOLEAN:
        putBoolean(timestamp, (Boolean) value);
        break;
      case TEXT:
        putBinary(timestamp, (Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
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
      tvLists.add(SerializableTVList.newSerializableTVList(dataType, queryId, uniqueId, index));
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
      public long nextTime() throws IOException {
        return getTime(currentPointIndex + 1);
      }

      @Override
      public int nextInt() throws IOException {
        return getInt(currentPointIndex + 1);
      }

      @Override
      public long nextLong() throws IOException {
        return getLong(currentPointIndex + 1);
      }

      @Override
      public float nextFloat() throws IOException {
        return getFloat(currentPointIndex + 1);
      }

      @Override
      public double nextDouble() throws IOException {
        return getDouble(currentPointIndex + 1);
      }

      @Override
      public boolean nextBoolean() throws IOException {
        return getBoolean(currentPointIndex + 1);
      }

      @Override
      public Binary nextBinary() throws IOException {
        return getBinary(currentPointIndex + 1);
      }

      @Override
      public String nextString() throws IOException {
        return getString(currentPointIndex + 1);
      }

      @Override
      public void reset() {
        currentPointIndex = -1;
      }
    };
  }

  @Override
  public DataPointWindowIterator getTumblingTimeWindowIterator(final int windowSize)
      throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException("Data point count be larger than 0.");
    }
    return new TumblingTimeWindowIterator(windowSize);
  }

  @Override
  public DataPointWindowIterator getTumblingTimeWindowIterator(final int windowSize,
      final long displayWindowBegin) throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException("Data point count should be larger than 0.");
    }
    try {
      return new TumblingTimeWindowIterator(windowSize, displayWindowBegin);
    } catch (IOException e) {
      throw new QueryProcessException(e.toString());
    }
  }

  private class TumblingTimeWindowIterator implements DataPointWindowIterator {

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

    TumblingTimeWindowIterator(int windowSize, long displayWindowBegin)
        throws IOException {
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
    public DataPointIterator currentWindow() {
      return getDataPointIteratorInWindow(currentWindowSize, minIndexInCurrentWindow);
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
    public int getIntInCurrentWindow(int index) throws IOException {
      return getInt(minIndexInCurrentWindow + index);
    }

    @Override
    public long getLongInCurrentWindow(int index) throws IOException {
      return getLong(minIndexInCurrentWindow + index);
    }

    @Override
    public boolean getBooleanInCurrentWindow(int index) throws IOException {
      return getBoolean(minIndexInCurrentWindow + index);
    }

    @Override
    public float getFloatInCurrentWindow(int index) throws IOException {
      return getFloat(minIndexInCurrentWindow + index);
    }

    @Override
    public double getDoubleInCurrentWindow(int index) throws IOException {
      return getDouble(minIndexInCurrentWindow + index);
    }

    @Override
    public Binary getBinaryInCurrentWindow(int index) throws IOException {
      return getBinary(minIndexInCurrentWindow + index);
    }

    @Override
    public String getStringInCurrentWindow(int index) throws IOException {
      return getString(minIndexInCurrentWindow + index);
    }

    @Override
    public void reset() {
      currentWindowIndex = -1;
      currentWindowSize = 0;
      minIndexInCurrentWindow = initialIndex - windowSize;
    }
  }

  @Override
  public DataPointWindowIterator getSlidingTimeWindowIterator(final long timeInterval,
      final long slidingStep) throws QueryProcessException {
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
  public DataPointWindowIterator getSlidingTimeWindowIterator(final long timeInterval,
      final long slidingStep, final long displayWindowBegin, final long displayWindowEnd)
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
    return new DataPointWindowIterator() {

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
      public DataPointIterator currentWindow() {
        return getDataPointIteratorInWindow(currentWindowSize, minIndexInCurrentWindow);
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
      public int getIntInCurrentWindow(int index) throws IOException {
        return getInt(minIndexInCurrentWindow + index);
      }

      @Override
      public long getLongInCurrentWindow(int index) throws IOException {
        return getLong(minIndexInCurrentWindow + index);
      }

      @Override
      public boolean getBooleanInCurrentWindow(int index) throws IOException {
        return getBoolean(minIndexInCurrentWindow + index);
      }

      @Override
      public float getFloatInCurrentWindow(int index) throws IOException {
        return getFloat(minIndexInCurrentWindow + index);
      }

      @Override
      public double getDoubleInCurrentWindow(int index) throws IOException {
        return getDouble(minIndexInCurrentWindow + index);
      }

      @Override
      public Binary getBinaryInCurrentWindow(int index) throws IOException {
        return getBinary(minIndexInCurrentWindow + index);
      }

      @Override
      public String getStringInCurrentWindow(int index) throws IOException {
        return getString(minIndexInCurrentWindow + index);
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

  private DataPointIterator getDataPointIteratorInWindow(int currentWindowSize,
      int minIndexInCurrentWindow) {

    return new DataPointIterator() {

      private int currentPointIndex = -1;

      @Override
      public boolean hasNextPoint() {
        return currentPointIndex < currentWindowSize - 1;
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
        return getTime(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public int currentInt() throws IOException {
        return getInt(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public long currentLong() throws IOException {
        return getLong(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public float currentFloat() throws IOException {
        return getFloat(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public double currentDouble() throws IOException {
        return getDouble(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public boolean currentBoolean() throws IOException {
        return getBoolean(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public Binary currentBinary() throws IOException {
        return getBinary(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public String currentString() throws IOException {
        return getString(minIndexInCurrentWindow + currentPointIndex);
      }

      @Override
      public long nextTime() throws IOException {
        return getTime(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public int nextInt() throws IOException {
        return getInt(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public long nextLong() throws IOException {
        return getLong(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public float nextFloat() throws IOException {
        return getFloat(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public double nextDouble() throws IOException {
        return getDouble(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public boolean nextBoolean() throws IOException {
        return getBoolean(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public Binary nextBinary() throws IOException {
        return getBinary(minIndexInCurrentWindow + currentPointIndex + 1);
      }

      @Override
      public String nextString() throws IOException {
        return getString(minIndexInCurrentWindow + currentPointIndex + 1);
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
