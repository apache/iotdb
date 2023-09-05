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

package org.apache.iotdb.db.queryengine.transformation.datastructure;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public abstract class Cache extends LinkedHashMap<Integer, Integer> {

  protected final int cacheCapacity;

  protected Cache(int cacheCapacity) {
    super(cacheCapacity, 0.75F, true);
    this.cacheCapacity = cacheCapacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
    return size() > cacheCapacity;
  }

  // get the eldest key
  public int getLast() {
    return this.entrySet().iterator().next().getKey();
  }

  protected Integer putKey(Integer index) {
    return put(index, index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Cache cache = (Cache) o;
    return cacheCapacity == cache.cacheCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), cacheCapacity);
  }
}
