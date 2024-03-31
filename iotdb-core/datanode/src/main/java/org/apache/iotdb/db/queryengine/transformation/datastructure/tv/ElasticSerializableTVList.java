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

package org.apache.iotdb.db.queryengine.transformation.datastructure.tv;

import org.apache.iotdb.db.queryengine.transformation.api.LayerPointReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.datastructure.Cache;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSerializableTVList {

  public static ElasticSerializableTVList newElasticSerializableTVList(
      TSDataType dataType, String queryId, float memoryLimitInMB, int cacheSize) {
//    return dataType.equals(TSDataType.TEXT)
//        ? new ElasticSerializableBinaryTVList(queryId, memoryLimitInMB, cacheSize)
//        : new ElasticSerializableTVList(dataType, queryId, memoryLimitInMB, cacheSize);
    return new ElasticSerializableTVList(dataType, queryId, memoryLimitInMB, cacheSize);
  }

  protected TSDataType dataType;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalTVListCapacity;
  protected int cacheSize;

  protected LRUCache cache;
  protected List<SerializableTVList> tvLists;

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
    return cache.get(index / internalTVListCapacity).isNull(index % internalTVListCapacity);
  }

  public long getTime(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getTime(index % internalTVListCapacity);
  }

  public int getInt(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getInt(index % internalTVListCapacity);
  }

  public long getLong(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getLong(index % internalTVListCapacity);
  }

  public float getFloat(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getFloat(index % internalTVListCapacity);
  }

  public double getDouble(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getDouble(index % internalTVListCapacity);
  }

  public boolean getBoolean(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBoolean(index % internalTVListCapacity);
  }

  public org.apache.iotdb.tsfile.utils.Binary getBinary(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinary(index % internalTVListCapacity);
  }

  public String getString(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinary(index % internalTVListCapacity)
        .getStringValue(TSFileConfig.STRING_CHARSET);
  }

  public void putColumn(TimeColumn timeColumn, Column valueColumn) throws IOException {
    checkExpansion();

    int begin = 0, end = 0;
    int total = timeColumn.getPositionCount();
    while (total > 0) {
      int consumed = Math.min(total, internalTVListCapacity) - size % internalTVListCapacity;
      end += consumed;

      // Construct sub-regions
      TimeColumn subTimeRegion = (TimeColumn) timeColumn.getRegion(begin, consumed);
      Column subValueRegion = valueColumn.getRegion(begin, consumed);

      // Fill row record list
      cache.get(size / internalTVListCapacity).putColumns(subTimeRegion, subValueRegion);

      total -= consumed;
      size += consumed;
      begin = end;

      if (total > 0) {
        doExpansion();
      }
    }
  }

  private void checkExpansion() {
    if (size % internalTVListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    tvLists.add(SerializableTVList.newSerializableTVList(dataType, queryId));
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
   * Set the upper bound.
   *
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

    SerializableTVList get(int targetIndex) throws IOException {
      if (!containsKey(targetIndex)) {
        if (cacheCapacity <= super.size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalTVListCapacity) {
            tvLists.set(lastIndex, null);
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
