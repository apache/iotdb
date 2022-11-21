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

package org.apache.iotdb.db.metadata.cache.lastCache.container;

import org.apache.iotdb.db.metadata.cache.lastCache.container.value.ILastCacheValue;
import org.apache.iotdb.db.metadata.cache.lastCache.container.value.LastCacheValue;
import org.apache.iotdb.tsfile.read.TimeValuePair;

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
  public synchronized void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (lastCacheValue == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastCacheValue = new LastCacheValue(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > lastCacheValue.getTimestamp()
        || (timeValuePair.getTimestamp() == lastCacheValue.getTimestamp() && highPriorityUpdate)) {
      lastCacheValue.setTimestamp(timeValuePair.getTimestamp());
      lastCacheValue.setValue(timeValuePair.getValue());
    }
  }

  @Override
  public synchronized void resetLastCache() {
    lastCacheValue = null;
  }

  @Override
  public boolean isEmpty() {
    return lastCacheValue == null;
  }
}
