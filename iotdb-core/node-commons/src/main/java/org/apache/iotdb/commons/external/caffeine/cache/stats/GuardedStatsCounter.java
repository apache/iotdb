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
import org.apache.iotdb.commons.i18n.CommonMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StatsCounter} implementation that suppresses and logs any exception thrown by the
 * delegate <code>statsCounter</code>.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
final class GuardedStatsCounter implements StatsCounter {
  static final Logger LOGGER = LoggerFactory.getLogger(GuardedStatsCounter.class);

  final StatsCounter delegate;

  GuardedStatsCounter(StatsCounter delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public void recordHits(int count) {
    try {
      delegate.recordHits(count);
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
    }
  }

  @Override
  public void recordMisses(int count) {
    try {
      delegate.recordMisses(count);
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
    }
  }

  @Override
  public void recordLoadSuccess(long loadTime) {
    try {
      delegate.recordLoadSuccess(loadTime);
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
    }
  }

  @Override
  public void recordLoadFailure(long loadTime) {
    try {
      delegate.recordLoadFailure(loadTime);
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
    }
  }

  @Override
  public void recordEviction(int weight, RemovalCause cause) {
    requireNonNull(cause);
    try {
      delegate.recordEviction(weight, cause);
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
    }
  }

  @Override
  public CacheStats snapshot() {
    try {
      return delegate.snapshot();
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_STATS_COUNTER_259634D3, t);
      return CacheStats.empty();
    }
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
