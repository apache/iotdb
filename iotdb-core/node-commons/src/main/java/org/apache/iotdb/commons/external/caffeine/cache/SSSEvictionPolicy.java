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

package org.apache.iotdb.commons.external.caffeine.cache;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>Holds the eviction policy of a cache bounded by size or weight, which only the maintenance
 * thread uses.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
abstract class SSSEvictionPolicy<K, V> extends SSS<K, V> {
  final WindowClimber climber;

  final FrequencySketch sketch;

  final AccessOrderDeque<Node<K, V>> accessOrderWindowDeque;

  final AccessOrderDeque<Node<K, V>> accessOrderProbationDeque;

  final AccessOrderDeque<Node<K, V>> accessOrderProtectedDeque;

  SSSEvictionPolicy(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
    this.climber = new WindowClimber();
    this.climber.resized(maximum());
    this.sketch = new FrequencySketch();
    if (builder.hasInitialCapacity()) {
      long capacity = Math.min(builder.getMaximum(), builder.getInitialCapacity());
      this.sketch.ensureCapacity(capacity);
      recordReads();
    }
    this.accessOrderWindowDeque =
        builder.evicts() || builder.expiresAfterAccess()
            ? new AccessOrderDeque<Node<K, V>>()
            : null;
    this.accessOrderProbationDeque = new AccessOrderDeque<Node<K, V>>();
    this.accessOrderProtectedDeque = new AccessOrderDeque<Node<K, V>>();
  }

  protected final WindowClimber climber() {
    return climber;
  }

  protected final FrequencySketch frequencySketch() {
    return sketch;
  }

  protected final AccessOrderDeque<Node<K, V>> accessOrderWindowDeque() {
    return accessOrderWindowDeque;
  }

  protected final AccessOrderDeque<Node<K, V>> accessOrderProbationDeque() {
    return accessOrderProbationDeque;
  }

  protected final AccessOrderDeque<Node<K, V>> accessOrderProtectedDeque() {
    return accessOrderProtectedDeque;
  }
}
