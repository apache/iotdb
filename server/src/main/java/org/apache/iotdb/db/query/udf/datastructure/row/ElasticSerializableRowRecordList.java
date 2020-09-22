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

package org.apache.iotdb.db.query.udf.datastructure.row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class ElasticSerializableRowRecordList {

  public static final float DEFAULT_MEMORY_USAGE_LIMIT = 100;
  public static final int DEFAULT_CACHE_SIZE = 3;

  protected TSDataType[] dataTypes;
  protected long queryId;
  protected String dataId;
  protected int internalRowRecordListCapacity;
  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowRecordLists;
  protected List<Long> minTimestamps;
  protected int size;
  protected int evictionUpperBound;

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
    evictionUpperBound = 0;
  }

  public int size() {
    return size;
  }

  public long getTime(int index) throws IOException {
    return cache.get(index / internalRowRecordListCapacity)
        .getTime(index % internalRowRecordListCapacity);
  }

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

  public void setEvictionUpperBound(int evictionUpperBound) {
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

    SerializableRowRecordList get(int targetIndex) throws IOException {
      if (rowRecordLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            rowRecordLists.set(lastIndex, null);
          } else {
            rowRecordLists.get(lastIndex).serialize();
          }
        }
        rowRecordLists.get(targetIndex).deserialize();
      }
      cache.addFirst(targetIndex);
      return rowRecordLists.get(targetIndex);
    }
  }
}
