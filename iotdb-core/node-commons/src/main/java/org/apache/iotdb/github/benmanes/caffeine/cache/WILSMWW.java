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
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache that provides the following features:
 *
 * <ul>
 *   <li>ExpireWrite
 *   <li>WeakKeys (inherited)
 *   <li>InfirmValues (inherited)
 *   <li>Listening (inherited)
 *   <li>Stats (inherited)
 *   <li>MaximumWeight (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"unchecked", "MissingOverride", "NullAway"})
class WILSMWW<K, V> extends WILSMW<K, V> {
  final Ticker ticker;

  final WriteOrderDeque<Node<K, V>> writeOrderDeque;

  volatile long expiresAfterWriteNanos;

  final Pacer pacer;

  WILSMWW(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
    this.ticker = builder.getTicker();
    this.writeOrderDeque = new WriteOrderDeque<Node<K, V>>();
    this.expiresAfterWriteNanos = builder.getExpiresAfterWriteNanos();
    this.pacer =
        (builder.getScheduler() == Scheduler.disabledScheduler())
            ? null
            : new Pacer(builder.getScheduler());
  }

  public final Ticker expirationTicker() {
    return ticker;
  }

  protected final WriteOrderDeque<Node<K, V>> writeOrderDeque() {
    return writeOrderDeque;
  }

  protected final boolean expiresAfterWrite() {
    return true;
  }

  protected final long expiresAfterWriteNanos() {
    return expiresAfterWriteNanos;
  }

  protected final void setExpiresAfterWriteNanos(long expiresAfterWriteNanos) {
    this.expiresAfterWriteNanos = expiresAfterWriteNanos;
  }

  public final Pacer pacer() {
    return pacer;
  }
}
