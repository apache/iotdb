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

package org.apache.iotdb.github.benmanes.caffeine.cache;

/**
 * The reason why a cached entry was removed.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public enum RemovalCause {

  /**
   * The entry was manually removed by the user. This can result from the user invoking any of the
   * following methods on the cache or map view.
   *
   * <ul>
   *   <li>{@link Cache#invalidate}
   *   <li>{@link Cache#invalidateAll(Iterable)}
   *   <li>{@link Cache#invalidateAll()}
   *   <li>{@link java.util.Map#remove}
   *   <li>{@link java.util.Map#computeIfPresent}
   *   <li>{@link java.util.Map#compute}
   *   <li>{@link java.util.Map#merge}
   *   <li>{@link java.util.concurrent.ConcurrentMap#remove}
   * </ul>
   *
   * A manual removal may also be performed through the key, value, or entry collections views by
   * the user invoking any of the following methods.
   *
   * <ul>
   *   <li>{@link java.util.Collection#remove}
   *   <li>{@link java.util.Collection#removeAll}
   *   <li>{@link java.util.Collection#removeIf}
   *   <li>{@link java.util.Collection#retainAll}
   *   <li>{@link java.util.Iterator#remove}
   * </ul>
   */
  EXPLICIT {
    @Override
    public boolean wasEvicted() {
      return false;
    }
  },

  /**
   * The entry itself was not actually removed, but its value was replaced by the user. This can
   * result from the user invoking any of the following methods on the cache or map view.
   *
   * <ul>
   *   <li>{@link Cache#put}
   *   <li>{@link Cache#putAll}
   *   <li>{@link LoadingCache#getAll}
   *   <li>{@link LoadingCache#refresh}
   *   <li>{@link java.util.Map#put}
   *   <li>{@link java.util.Map#putAll}
   *   <li>{@link java.util.Map#replace}
   *   <li>{@link java.util.Map#computeIfPresent}
   *   <li>{@link java.util.Map#compute}
   *   <li>{@link java.util.Map#merge}
   * </ul>
   */
  REPLACED {
    @Override
    public boolean wasEvicted() {
      return false;
    }
  },

  /**
   * The entry was removed automatically because its key or value was garbage-collected. This can
   * occur when using {@link Caffeine#weakKeys}, {@link Caffeine#weakValues}, or {@link
   * Caffeine#softValues}.
   */
  COLLECTED {
    @Override
    public boolean wasEvicted() {
      return true;
    }
  },

  /**
   * The entry's expiration timestamp has passed. This can occur when using {@link
   * Caffeine#expireAfterWrite}, {@link Caffeine#expireAfterAccess}, or {@link
   * Caffeine#expireAfter(Expiry)}.
   */
  EXPIRED {
    @Override
    public boolean wasEvicted() {
      return true;
    }
  },

  /**
   * The entry was evicted due to size constraints. This can occur when using {@link
   * Caffeine#maximumSize} or {@link Caffeine#maximumWeight}.
   */
  SIZE {
    @Override
    public boolean wasEvicted() {
      return true;
    }
  };

  /**
   * Returns {@code true} if there was an automatic removal due to eviction (the cause is neither
   * {@link #EXPLICIT} nor {@link #REPLACED}).
   *
   * @return if the entry was automatically removed due to eviction
   */
  public abstract boolean wasEvicted();
}
