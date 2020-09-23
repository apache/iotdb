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

package org.apache.iotdb.db.query.udf.datastructure.tv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.datastructure.SerializableList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

public class ElasticSerializableTVList implements PointCollector {

  public static final float DEFAULT_MEMORY_USAGE_LIMIT = 100;
  public static final int DEFAULT_CACHE_SIZE = 3;

  protected TSDataType dataType;
  protected long queryId;
  protected String uniqueId;
  protected int internalTVListCapacity;
  protected LRUCache cache;
  protected List<BatchData> tvLists;
  protected List<Long> minTimestamps;
  protected int size;
  protected int evictionUpperBound;

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
    evictionUpperBound = 0;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public int size() {
    return size;
  }

  public long getTime(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getTimeByIndex(index % internalTVListCapacity);
  }

  public int getInt(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getIntByIndex(index % internalTVListCapacity);
  }

  public long getLong(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getLongByIndex(index % internalTVListCapacity);
  }

  public float getFloat(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getFloatByIndex(index % internalTVListCapacity);
  }

  public double getDouble(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getDoubleByIndex(index % internalTVListCapacity);
  }

  public boolean getBoolean(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getBooleanByIndex(index % internalTVListCapacity);
  }

  public Binary getBinary(int index) throws IOException {
    return cache.get(index / internalTVListCapacity)
        .getBinaryByIndex(index % internalTVListCapacity);
  }

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

  public LayerPointReader getPointReaderUsingEvictionStrategy() {

    return new LayerPointReader() {

      private int currentPointIndex = -1;

      @Override
      public boolean next() {
        if (size - 1 <= currentPointIndex) {
          return false;
        }
        ++currentPointIndex;
        return true;
      }

      @Override
      public void readyForNext() {
        setEvictionUpperBound(currentPointIndex);
      }

      @Override
      public TSDataType getDataType() {
        return dataType;
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
    };
  }

  private void setEvictionUpperBound(int evictionUpperBound) {
    this.evictionUpperBound = evictionUpperBound;
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

    BatchData get(int targetIndex) throws IOException {
      if (tvLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          if (lastIndex < evictionUpperBound / internalTVListCapacity) {
            tvLists.set(lastIndex, null);
          } else {
            ((SerializableList) tvLists.get(lastIndex)).serialize();
          }
        }
        ((SerializableList) tvLists.get(targetIndex)).deserialize();
      }
      cache.addFirst(targetIndex);
      return tvLists.get(targetIndex);
    }
  }
}
