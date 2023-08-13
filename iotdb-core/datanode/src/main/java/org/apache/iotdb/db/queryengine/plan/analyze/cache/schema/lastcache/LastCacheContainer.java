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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.value.ILastCacheValue;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.value.LastCacheValue;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

/**
 * This class possesses the ILastCacheValue and implements the basic last cache operations.
 *
 * <p>The ILastCacheValue may be extended to ILastCacheValue List in future to support batched last
 * value cache.
 */
public class LastCacheContainer implements ILastCacheContainer {

  ILastCacheValue lastCacheValue;

  @Override
  public TimeValuePair getCachedLast() {
    return lastCacheValue == null ? null : lastCacheValue.getTimeValuePair();
  }

  @Override
  public synchronized int updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (highPriorityUpdate) { // for write, we won't cache null value
      if (timeValuePair == null || timeValuePair.getValue() == null) {
        return 0;
      }
    } else { // for read, we need to cache null value, because it means that there is no data for
      // this time series
      if (timeValuePair == null) {
        return 0;
      }
    }

    if (lastCacheValue == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastCacheValue = new LastCacheValue(timeValuePair.getTimestamp(), timeValuePair.getValue());
        return lastCacheValue.estimateSize();
      }
    } else if (timeValuePair.getTimestamp() > lastCacheValue.getTimestamp()
        || (timeValuePair.getTimestamp() == lastCacheValue.getTimestamp() && highPriorityUpdate)) {
      TsPrimitiveType oldValue = lastCacheValue.getValue();
      lastCacheValue.setTimestamp(timeValuePair.getTimestamp());
      lastCacheValue.setValue(timeValuePair.getValue());
      return getDiffSize(oldValue, timeValuePair.getValue());
    }
    return 0;
  }

  private int getDiffSize(TsPrimitiveType oldValue, TsPrimitiveType newValue) {
    if (oldValue == null) {
      return newValue == null ? 0 : newValue.getSize();
    } else if (oldValue instanceof TsPrimitiveType.TsBinary) {
      return (newValue == null ? 0 : newValue.getSize()) - oldValue.getSize();
    } else {
      return newValue == null ? -oldValue.getSize() : 0;
    }
  }

  /**
   * Total basic 100B
   *
   * <ul>
   *   <li>LastCacheContainer Object header, 8B
   *   <li>ILastCacheValue reference, 8B
   * </ul>
   */
  @Override
  public int estimateSize() {
    return 16 + (lastCacheValue == null ? 0 : lastCacheValue.estimateSize());
  }
}
