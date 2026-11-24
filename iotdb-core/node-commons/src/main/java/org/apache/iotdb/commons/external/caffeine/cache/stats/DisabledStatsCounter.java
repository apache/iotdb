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

package org.apache.iotdb.commons.external.caffeine.cache.stats;

import org.apache.iotdb.commons.external.caffeine.cache.RemovalCause;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StatsCounter} implementation that does not record any cache events.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
enum DisabledStatsCounter implements StatsCounter {
  INSTANCE;

  @Override
  public void recordHits(int count) {}

  @Override
  public void recordMisses(int count) {}

  @Override
  public void recordLoadSuccess(long loadTime) {}

  @Override
  public void recordLoadFailure(long loadTime) {}

  @Override
  public void recordEviction(int weight, RemovalCause cause) {
    requireNonNull(cause);
  }

  @Override
  public CacheStats snapshot() {
    return CacheStats.empty();
  }

  @Override
  public String toString() {
    return snapshot().toString();
  }
}
