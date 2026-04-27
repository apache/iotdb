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

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.ICache;

import java.util.function.Function;

/**
 * This class defines and implements the behaviour needed for building a dual key cache.
 *
 * @param <FK> The first key of cache value
 * @param <SK> The second key of cache value
 * @param <V> The cache value
 */
public class CacheBuilder<SK, V> {

  private CachePolicy policy;

  private long memoryCapacity;

  private Function<SK, Integer> secondKeySizeComputer;

  private Function<V, Integer> valueSizeComputer;

  /** Initiate and return a dual key cache instance. */
  public ICache<SK, V> build() {
    ICacheEntryManager<SK, V> cacheEntryManager = null;
    switch (policy) {
      case LRU:
        cacheEntryManager = new LRUCacheEntryManager<>();
        break;
      case FIFO:
        cacheEntryManager = new FIFOCacheEntryManager<>();
        break;
    }
    return new CacheImpl<>(
        cacheEntryManager,
        new CacheSizeComputerImpl<>(secondKeySizeComputer, valueSizeComputer),
        memoryCapacity);
  }

  /** Define the cache eviction policy of dual key cache. */
  public CacheBuilder<SK, V> cacheEvictionPolicy(CachePolicy policy) {
    this.policy = policy;
    return this;
  }

  /** Define the memory capacity of dual key cache. */
  public CacheBuilder<SK, V> memoryCapacity(long memoryCapacity) {
    this.memoryCapacity = memoryCapacity;
    return this;
  }

  /** Define how to compute the memory usage of a second key in dual key cache. */
  public CacheBuilder<SK, V> secondKeySizeComputer(Function<SK, Integer> computer) {
    this.secondKeySizeComputer = computer;
    return this;
  }

  /** Define how to compute the memory usage of a cache value in dual key cache. */
  public CacheBuilder<SK, V> valueSizeComputer(Function<V, Integer> computer) {
    this.valueSizeComputer = computer;
    return this;
  }
}
