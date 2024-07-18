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

import org.apache.iotdb.db.queryengine.transformation.datastructure.Cache;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.TVListForwardIterator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSerializableTVList {
  protected TSDataType dataType;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalTVListCapacity;
  protected int cacheSize;

  protected LRUCache cache;
  protected List<SerializableTVList> internalTVList;
  protected List<Integer> internalColumnCountList;

  protected int pointCount;
  protected int evictionUpperBound;

  // Observer pattern
  protected List<TVListForwardIterator> iteratorList;

  public static ElasticSerializableTVList construct(
      TSDataType dataType, String queryId, float memoryLimitInMB, int cacheSize) {
    return dataType.equals(TSDataType.TEXT)
        ? new ElasticSerializableBinaryTVList(queryId, memoryLimitInMB, cacheSize)
        : new ElasticSerializableTVList(dataType, queryId, memoryLimitInMB, cacheSize);
  }

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
    internalTVList = new ArrayList<>();
    internalColumnCountList = new ArrayList<>();
    pointCount = 0;
    evictionUpperBound = 0;

    iteratorList = new ArrayList<>();
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
    internalTVList = new ArrayList<>();
    internalColumnCountList = new ArrayList<>();
    pointCount = 0;
    evictionUpperBound = 0;

    iteratorList = new ArrayList<>();
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public int size() {
    return pointCount;
  }

  public int getInternalTVListCapacity() {
    return internalTVListCapacity;
  }

  public SerializableTVList getSerializableTVList(int index) {
    // Do not cache this TVList
    return internalTVList.get(index);
  }

  public int getSerializableTVListSize() {
    return internalTVList.size();
  }

  // region single data point methods for row window
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
    return cache.get(index / internalTVListCapacity).getFloat(index % internalTVListCapacity);
  }

  public double getDouble(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getDouble(index % internalTVListCapacity);
  }

  public boolean getBoolean(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getBoolean(index % internalTVListCapacity);
  }

  public Binary getBinary(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getBinary(index % internalTVListCapacity);
  }

  public String getString(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinary(index % internalTVListCapacity)
        .getStringValue(TSFileConfig.STRING_CHARSET);
  }

  // endregion

  // region batch data points methods
  public Column getTimeColumn(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getTimeColumn(internalIndex);
  }

  public Column getValueColumn(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getValueColumn(internalIndex);
  }

  public void putColumn(Column timeColumn, Column valueColumn) throws IOException {
    checkExpansion();

    int begin = 0, end = 0;
    int total = timeColumn.getPositionCount();
    while (total > 0) {
      int consumed;
      Column insertedTimeColumn;
      Column insertedValueColumn;
      if (total + pointCount % internalTVListCapacity < internalTVListCapacity) {
        consumed = total;
        if (begin == 0) {
          // No need to copy if the columns do not split
          insertedTimeColumn = timeColumn;
          insertedValueColumn = valueColumn;
        } else {
          insertedTimeColumn = timeColumn.getRegionCopy(begin, consumed);
          insertedValueColumn = valueColumn.getRegionCopy(begin, consumed);
        }
      } else {
        consumed = internalTVListCapacity - pointCount % internalTVListCapacity;
        // Construct sub-regions
        insertedTimeColumn = timeColumn.getRegionCopy(begin, consumed);
        insertedValueColumn = valueColumn.getRegionCopy(begin, consumed);
      }

      end += consumed;
      begin = end;
      total -= consumed;

      // Fill row record list
      cache
          .get(pointCount / internalTVListCapacity)
          .putColumns(insertedTimeColumn, insertedValueColumn);
      pointCount += consumed;

      if (total > 0) {
        doExpansion();
      }
    }
  }

  // endregion

  public TVListForwardIterator constructIterator() {
    TVListForwardIterator iterator = new TVListForwardIterator(this);
    iteratorList.add(iterator);

    return iterator;
  }

  private void checkExpansion() {
    if (pointCount % internalTVListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    if (internalTVList.size() > 0) {
      int lastIndex = internalTVList.size() - 1;
      SerializableTVList lastInternalList = internalTVList.get(lastIndex);
      internalColumnCountList.add(lastInternalList.getColumnCount());
    }
    internalTVList.add(SerializableTVList.construct(queryId));
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

  public int getColumnCount(int index) {
    if (index == internalTVList.size() - 1) {
      SerializableTVList lastList = internalTVList.get(index);
      return lastList.getColumnCount();
    } else {
      return internalColumnCountList.get(index);
    }
  }

  public int getLastPointIndex(int externalIndex, int internalIndex) {
    int index = internalTVListCapacity * externalIndex;
    int offset = internalTVList.get(externalIndex).getLastPointIndex(internalIndex);

    return index + offset;
  }

  public class LRUCache extends Cache {

    LRUCache(int capacity) {
      super(capacity);
    }

    public SerializableTVList get(int targetIndex) throws IOException {
      if (!containsKey(targetIndex)) {
        if (cacheCapacity <= super.size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalTVListCapacity) {
            internalTVList.set(lastIndex, null);
          } else {
            internalTVList.get(lastIndex).serialize();
          }
        }
        internalTVList.get(targetIndex).deserialize();
      }
      putKey(targetIndex);
      return internalTVList.get(targetIndex);
    }
  }
}
