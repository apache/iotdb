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

package org.apache.iotdb.db.query.udf.datastructure.primitive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;

public class ElasticSerializableIntList implements IntList {

  public static final float DEFAULT_MEMORY_USAGE_LIMIT = 100;
  public static final int DEFAULT_CACHE_SIZE = 1;

  protected long queryId;
  protected String dataId;
  protected int internalIntListCapacity;
  protected ElasticSerializableIntList.LRUCache cache;
  protected List<SerializableIntList> intLists;
  protected int size;

  public ElasticSerializableIntList(long queryId, String dataId, float memoryLimitInMB,
      int cacheSize) throws QueryProcessException {
    this.queryId = queryId;
    this.dataId = dataId;
    int allocatableCapacity = SerializableIntList.calculateCapacity(memoryLimitInMB);
    internalIntListCapacity = allocatableCapacity / cacheSize;
    if (internalIntListCapacity == 0) {
      cacheSize = 1;
      internalIntListCapacity = allocatableCapacity;
    }
    cache = new ElasticSerializableIntList.LRUCache(cacheSize);
    intLists = new ArrayList<>();
    size = 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int get(int index) throws IOException {
    return cache.get(index / internalIntListCapacity).get(index % internalIntListCapacity);
  }

  @Override
  public void put(int value) throws IOException {
    checkExpansion();
    cache.get(size / internalIntListCapacity).put(value);
    ++size;
  }

  @Override
  public void clear() {
    cache.clear();
    intLists.clear();
    size = 0;
  }

  private void checkExpansion() {
    if (size % internalIntListCapacity == 0) {
      int index = intLists.size();
      intLists.add(SerializableIntList.newSerializableIntList(queryId, dataId, index));
    }
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

    SerializableIntList get(int targetIndex) throws IOException {
      if (intLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          intLists.get(lastIndex).serialize();
        }
        intLists.get(targetIndex).deserialize();
      }
      cache.addFirst(targetIndex);
      return intLists.get(targetIndex);
    }

    void clear() {
      cache.clear();
    }
  }
}
