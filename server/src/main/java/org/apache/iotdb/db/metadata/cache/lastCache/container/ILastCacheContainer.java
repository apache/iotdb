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

import org.apache.iotdb.tsfile.read.TimeValuePair;

/** this interface declares the operations of LastCache data */
public interface ILastCacheContainer {

  // get lastCache of monad timseries
  TimeValuePair getCachedLast();

  /**
   * update last point cache
   *
   * @param timeValuePair last point
   * @param highPriorityUpdate whether it's a high priority update
   * @param latestFlushedTime latest flushed time
   */
  void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime);

  // reset all lastCache data of one timeseries(monad or vector)
  void resetLastCache();

  // whether the entry contains lastCache Value.
  boolean isEmpty();
}
