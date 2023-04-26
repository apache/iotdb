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

package org.apache.iotdb.db.mpp.transformation.datastructure.tv;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.datastructure.Cache;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.type.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSerializableTVList implements PointCollector {

  public static ElasticSerializableTVList newElasticSerializableTVList(
      TSDataType dataType, String queryId, float memoryLimitInMB, int cacheSize) {
    return dataType.equals(TSDataType.TEXT)
        ? new ElasticSerializableBinaryTVList(queryId, memoryLimitInMB, cacheSize)
        : new ElasticSerializableTVList(dataType, queryId, memoryLimitInMB, cacheSize);
  }

  protected TSDataType dataType;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalTVListCapacity;
  protected int cacheSize;

  protected LRUCache cache;
  protected List<SerializableTVList> tvLists;
  /**
   * the bitmap used to indicate whether one value is null in the tvLists. The size of bitMap is the
   * same as tvLists and the length of whole bits is the same as tvLists' length.
   */
  protected List<BitMap> bitMaps;

  protected int size;
  protected int evictionUpperBound;

  protected ElasticSerializableTVList(
      TSDataType dataType, String queryId, float memoryLimitInMB, int cacheSize) {
    this.dataType = dataType;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    int allocatableCapacity = SerializableTVList.calculateCapacity(dataType, memoryLimitInMB);
    internalTVListCapacity = allocatableCapacity / cacheSize;
    if (internalTVListCapacity == 0) {
      cacheSize = 1;
      internalTVListCapacity = allocatableCapacity;
    }
    this.cacheSize = cacheSize;

    cache = new LRUCache(cacheSize);
    bitMaps = new ArrayList<>();
    tvLists = new ArrayList<>();
    size = 0;
    evictionUpperBound = 0;
  }

  protected ElasticSerializableTVList(
      TSDataType dataType,
      String queryId,
      float memoryLimitInMB,
      int internalTVListCapacity,
      int cacheSize) {
    this.dataType = dataType;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    this.internalTVListCapacity = internalTVListCapacity;
    this.cacheSize = cacheSize;

    cache = new LRUCache(cacheSize);
    bitMaps = new ArrayList<>();
    tvLists = new ArrayList<>();
    size = 0;
    evictionUpperBound = 0;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public int size() {
    return size;
  }

  public boolean isNull(int index) throws IOException {
    return bitMaps.get(index / internalTVListCapacity).isMarked(index % internalTVListCapacity);
  }

  public long getTime(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getTimeByIndex(index % internalTVListCapacity);
  }

  public int getInt(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getIntByIndex(index % internalTVListCapacity);
  }

  public long getLong(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getLongByIndex(index % internalTVListCapacity);
  }

  public float getFloat(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getFloatByIndex(index % internalTVListCapacity);
  }

  public double getDouble(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getDoubleByIndex(index % internalTVListCapacity);
  }

  public boolean getBoolean(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBooleanByIndex(index % internalTVListCapacity);
  }

  public org.apache.iotdb.tsfile.utils.Binary getBinary(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinaryByIndex(index % internalTVListCapacity);
  }

  public String getString(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinaryByIndex(index % internalTVListCapacity)
        .getStringValue();
  }

  public void put(long timestamp, Object value) throws IOException, QueryProcessException {
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
        putBinary(
            timestamp,
            UDFBinaryTransformer.transformToUDFBinary(
                (org.apache.iotdb.tsfile.utils.Binary) value));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  @Override
  public void putInt(long timestamp, int value) throws IOException {
    checkExpansion();
    cache.get(size / internalTVListCapacity).putInt(timestamp, value);
    ++size;
  }

  @Override
  public void putLong(long timestamp, long value) throws IOException {
    checkExpansion();
    cache.get(size / internalTVListCapacity).putLong(timestamp, value);
    ++size;
  }

  @Override
  public void putFloat(long timestamp, float value) throws IOException {
    checkExpansion();
    cache.get(size / internalTVListCapacity).putFloat(timestamp, value);
    ++size;
  }

  @Override
  public void putDouble(long timestamp, double value) throws IOException {
    checkExpansion();
    cache.get(size / internalTVListCapacity).putDouble(timestamp, value);
    ++size;
  }

  @Override
  public void putBoolean(long timestamp, boolean value) throws IOException {
    checkExpansion();
    cache.get(size / internalTVListCapacity).putBoolean(timestamp, value);
    ++size;
  }

  @Override
  public void putBinary(long timestamp, Binary value) throws IOException {
    checkExpansion();
    cache
        .get(size / internalTVListCapacity)
        .putBinary(timestamp, UDFBinaryTransformer.transformToBinary(value));
    ++size;
  }

  @Override
  public void putString(long timestamp, String value) throws IOException {
    checkExpansion();
    cache
        .get(size / internalTVListCapacity)
        .putBinary(timestamp, org.apache.iotdb.tsfile.utils.Binary.valueOf(value));
    ++size;
  }

  public void putNull(long timestamp) throws IOException {
    switch (dataType) {
      case INT32:
        putInt(timestamp, 0);
        break;
      case INT64:
        putLong(timestamp, 0L);
        break;
      case FLOAT:
        putFloat(timestamp, 0.0F);
        break;
      case DOUBLE:
        putDouble(timestamp, 0.0D);
        break;
      case BOOLEAN:
        putBoolean(timestamp, false);
        break;
      case TEXT:
        putBinary(
            timestamp,
            UDFBinaryTransformer.transformToUDFBinary(
                org.apache.iotdb.tsfile.utils.Binary.EMPTY_VALUE));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    bitMaps.get((size - 1) / internalTVListCapacity).mark((size - 1) % internalTVListCapacity);
  }

  private void checkExpansion() {
    if (size % internalTVListCapacity == 0) {
      tvLists.add(SerializableTVList.newSerializableTVList(dataType, queryId));
      bitMaps.add(new BitMap(internalTVListCapacity));
    }
  }

  public LayerPointReader constructPointReaderUsingTrivialEvictionStrategy() {

    return new LayerPointReader() {

      private int currentPointIndex = -1;

      @Override
      public boolean isConstantPointReader() {
        return false;
      }

      @Override
      public YieldableState yield() {
        throw new UnsupportedOperationException();
      }

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
        setEvictionUpperBound(currentPointIndex + 1);
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
      public org.apache.iotdb.tsfile.utils.Binary currentBinary() throws IOException {
        return getBinary(currentPointIndex);
      }

      @Override
      public boolean isCurrentNull() throws IOException {
        return isNull(currentPointIndex);
      }
    };
  }

  /**
   * @param evictionUpperBound the index of the first element that cannot be evicted. in other
   *     words, elements whose index are <b>less than</b> the evictionUpperBound can be evicted.
   */
  public void setEvictionUpperBound(int evictionUpperBound) {
    this.evictionUpperBound = evictionUpperBound;
  }

  private class LRUCache extends Cache {

    LRUCache(int capacity) {
      super(capacity);
    }

    BatchData get(int targetIndex) throws IOException {
      if (!containsKey(targetIndex)) {
        if (cacheCapacity <= size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalTVListCapacity) {
            tvLists.set(lastIndex, null);
            bitMaps.set(lastIndex, null);
          } else {
            tvLists.get(lastIndex).serialize();
          }
        }
        tvLists.get(targetIndex).deserialize();
      }
      putKey(targetIndex);
      return tvLists.get(targetIndex);
    }
  }
}
