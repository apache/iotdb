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
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator.TVListForwardIterator;
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
  protected List<SerializableTVList> internalTVList;

  protected int pointCount; // Expose for row window
  protected int columnCount; // Only for internal use
  protected int evictionUpperBound;

  // Observer pattern
  protected List<TVListForwardIterator> iteratorList;

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
    pointCount = 0;
    columnCount = 0;
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
    pointCount = 0;
    columnCount = 0;
    evictionUpperBound = 0;

    iteratorList = new ArrayList<>();
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public int getPointCount() {
    return pointCount;
  }

  public int getColumnCount() {
    return columnCount;
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
    return cache.get(index / internalTVListCapacity).getFloat(index % internalTVListCapacity);
  }

  public double getDouble(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getDouble(index % internalTVListCapacity);
  }

  public boolean getBoolean(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getBoolean(index % internalTVListCapacity);
  }

  public org.apache.iotdb.tsfile.utils.Binary getBinary(int index) throws IOException {
    return cache.get(index / internalTVListCapacity).getBinary(index % internalTVListCapacity);
  }

  public String getString(int index) throws IOException {
    return cache
        .get(index / internalTVListCapacity)
        .getBinary(index % internalTVListCapacity)
        .getStringValue(TSFileConfig.STRING_CHARSET);
  }

  public TimeColumn getTimeColumn(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getTimeColumn(internalIndex);
  }

  public Column getValueColumn(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getValueColumn(internalIndex);
  }

  public List<SerializableTVList> getInternalTVList() {
    return internalTVList;
  }

  public TVListForwardIterator constructIterator() {
    TVListForwardIterator iterator = new TVListForwardIterator(this);
    iteratorList.add(iterator);

    return iterator;
  }

  public TVListForwardIterator constructIteratorByEvictionUpperBound() {
    int externalColumnIndex = evictionUpperBound / internalTVListCapacity;

    int internalPointIndex = evictionUpperBound % internalTVListCapacity;
    int internalColumnIndex =
        internalTVList.get(externalColumnIndex).getColumnIndex(internalPointIndex);

    // This iterator is for memory control.
    // So there is no need to put it into iterator list since it won't be affected by new memory
    // control strategy.
    return new TVListForwardIterator(this, externalColumnIndex, internalColumnIndex);
  }

  public void notifyAllIterators() throws IOException {
    for (TVListForwardIterator iterator : iteratorList) {
      iterator.adjust();
    }
  }

  public void putColumn(TimeColumn timeColumn, Column valueColumn) throws IOException {
    checkExpansion();

    int begin = 0, end = 0;
    int total = timeColumn.getPositionCount();
    while (total > 0) {
      int consumed;
      TimeColumn insertedTimeColumn;
      Column insertedValueColumn;
      if (total + pointCount % internalTVListCapacity < internalTVListCapacity) {
        consumed = total;
        insertedTimeColumn = timeColumn;
        insertedValueColumn = valueColumn;
      } else {
        consumed = internalTVListCapacity - pointCount % internalTVListCapacity;
        // Construct sub-regions
        insertedTimeColumn = (TimeColumn) timeColumn.copyRegion(begin, consumed);
        insertedValueColumn = valueColumn.copyRegion(begin, consumed);
      }
      end += consumed;

      // Fill row record list
      cache
          .get(pointCount / internalTVListCapacity)
          .putColumns(insertedTimeColumn, insertedValueColumn);

      total -= consumed;
      pointCount += consumed;
      columnCount++;
      begin = end;

      if (total > 0) {
        doExpansion();
      }
    }
  }

  private void checkExpansion() {
    if (pointCount % internalTVListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    internalTVList.add(SerializableTVList.newSerializableTVList(dataType, queryId));
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
