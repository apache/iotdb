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
 *   <li>ExpireAccess
 *   <li>WeakKeys (inherited)
 *   <li>InfirmValues (inherited)
 *   <li>Listening (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"unchecked", "MissingOverride", "NullAway"})
class WILA<K, V> extends WIL<K, V> {
  final Ticker ticker;

  final AccessOrderDeque<Node<K, V>> accessOrderWindowDeque;

  final Expiry<K, V> expiry;

  final TimerWheel<K, V> timerWheel;

  volatile long expiresAfterAccessNanos;

  final Pacer pacer;

  WILA(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
    this.ticker = builder.getTicker();
    this.accessOrderWindowDeque =
        builder.evicts() || builder.expiresAfterAccess()
            ? new AccessOrderDeque<Node<K, V>>()
            : null;
    this.expiry = builder.getExpiry(isAsync);
    this.timerWheel = builder.expiresVariable() ? new TimerWheel<K, V>(this) : null;
    this.expiresAfterAccessNanos = builder.getExpiresAfterAccessNanos();
    this.pacer =
        (builder.getScheduler() == Scheduler.disabledScheduler())
            ? null
            : new Pacer(builder.getScheduler());
  }

  public final Ticker expirationTicker() {
    return ticker;
  }

  protected final AccessOrderDeque<Node<K, V>> accessOrderWindowDeque() {
    return accessOrderWindowDeque;
  }

  protected final boolean expiresVariable() {
    return (timerWheel != null);
  }

  protected final Expiry<K, V> expiry() {
    return expiry;
  }

  protected final TimerWheel<K, V> timerWheel() {
    return timerWheel;
  }

  protected final boolean expiresAfterAccess() {
    return (timerWheel == null);
  }

  protected final long expiresAfterAccessNanos() {
    return expiresAfterAccessNanos;
  }

  protected final void setExpiresAfterAccessNanos(long expiresAfterAccessNanos) {
    this.expiresAfterAccessNanos = expiresAfterAccessNanos;
  }

  public final Pacer pacer() {
    return pacer;
  }
}
