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

package org.apache.iotdb.db.metadata.cache.dualkeycache.impl;

import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCache;

import java.util.function.Function;

public class DualKeyCacheBuilder<FK, SK, V> {

  private LRUCacheEntryManager<FK, SK, V> cacheEntryManager;

  private long memoryCapacity;

  private Function<FK, Integer> firstKeySizeComputer;

  private Function<SK, Integer> secondKeySizeComputer;

  private Function<V, Integer> valueSizeComputer;

  public IDualKeyCache<FK, SK, V> build() {
    return new DualKeyCacheImpl<>(
        cacheEntryManager,
        new CacheSizeComputerImpl<>(firstKeySizeComputer, secondKeySizeComputer, valueSizeComputer),
        memoryCapacity);
  }

  public DualKeyCacheBuilder<FK, SK, V> cacheEvictionPolicy(DualKeyCachePolicy policy) {
    if (policy == DualKeyCachePolicy.LRU) {
      this.cacheEntryManager = new LRUCacheEntryManager<>();
      return this;
    }
    throw new IllegalStateException();
  }

  public DualKeyCacheBuilder<FK, SK, V> memoryCapacity(long memoryCapacity) {
    this.memoryCapacity = memoryCapacity;
    return this;
  }

  public DualKeyCacheBuilder<FK, SK, V> firstKeySizeComputer(Function<FK, Integer> computer) {
    this.firstKeySizeComputer = computer;
    return this;
  }

  public DualKeyCacheBuilder<FK, SK, V> secondKeySizeComputer(Function<SK, Integer> computer) {
    this.secondKeySizeComputer = computer;
    return this;
  }

  public DualKeyCacheBuilder<FK, SK, V> valueSizeComputer(Function<V, Integer> computer) {
    this.valueSizeComputer = computer;
    return this;
  }
}
