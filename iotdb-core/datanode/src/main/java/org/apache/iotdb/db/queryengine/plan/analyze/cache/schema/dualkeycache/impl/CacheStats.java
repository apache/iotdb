/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheStats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

class CacheStats implements IDualKeyCacheStats {

  // prepare some buffer for high load scenarios
  private static final double MEMORY_THRESHOLD_RATIO = 0.8;

  private final long memoryThreshold;

  private final Supplier<Long> memoryComputation;
  private final Supplier<Long> entriesComputation;

  private final AtomicLong requestCount = new AtomicLong(0);
  private final AtomicLong hitCount = new AtomicLong(0);

  CacheStats(
      long memoryCapacity,
      final Supplier<Long> memoryComputation,
      final Supplier<Long> entriesComputation) {
    this.memoryThreshold = (long) (memoryCapacity * MEMORY_THRESHOLD_RATIO);
    this.memoryComputation = memoryComputation;
    this.entriesComputation = entriesComputation;
  }

  long getExceedMemory() {
    return memoryUsage() - memoryThreshold;
  }

  void recordHit(int num) {
    if (requestCount.get() < 0) {
      requestCount.set(0);
      hitCount.set(0);
    }
    requestCount.getAndAdd(num);
    hitCount.getAndAdd(num);
  }

  void recordMiss(int num) {
    if (requestCount.get() < 0) {
      requestCount.set(0);
      hitCount.set(0);
    }
    requestCount.getAndAdd(num);
  }

  @Override
  public long requestCount() {
    return requestCount.get();
  }

  @Override
  public long hitCount() {
    return hitCount.get();
  }

  @Override
  public double hitRate() {
    long hitCount = this.hitCount.get();
    if (hitCount == 0) {
      return 0;
    }
    long requestCount = this.requestCount.get();
    if (requestCount == 0) {
      return 0;
    }
    return hitCount * 1.0 / requestCount;
  }

  @Override
  public long memoryUsage() {
    return memoryComputation.get();
  }

  @Override
  public long capacity() {
    return memoryThreshold;
  }

  @Override
  public long entriesCount() {
    return entriesComputation.get();
  }
}
