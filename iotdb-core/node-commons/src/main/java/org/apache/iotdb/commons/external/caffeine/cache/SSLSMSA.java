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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache that provides the following features:
 *
 * <ul>
 *   <li>ExpireAccess
 *   <li>StrongKeys (inherited)
 *   <li>StrongValues (inherited)
 *   <li>MaximumSize (inherited)
 *   <li>Listening (inherited)
 *   <li>Stats (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
class SSLSMSA<K, V> extends SSLSMSPadAfterMaximum<K, V> {
  static final LocalCacheFactory FACTORY = SSLSMSA::new;

  protected static final VarHandle EXPIRES_AFTER_ACCESS_NANOS;

  static {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      EXPIRES_AFTER_ACCESS_NANOS =
          lookup.findVarHandle(
              SSLSMSA.class, LocalCacheFactory.EXPIRES_AFTER_ACCESS_NANOS, long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  final Ticker ticker;

  volatile long expiresAfterAccessNanos;

  final Pacer pacer;

  SSLSMSA(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
    this.ticker = builder.getTicker();
    setExpiresAfterAccessNanos(builder.getExpiresAfterAccessNanos());
    Scheduler scheduler = builder.getScheduler();
    this.pacer = (scheduler == Scheduler.disabledScheduler()) ? null : new Pacer(scheduler);
  }

  public final Ticker expirationTicker() {
    return ticker;
  }

  protected final boolean fastpath() {
    return false;
  }

  protected final long expiresAfterAccessNanos() {
    return (long) EXPIRES_AFTER_ACCESS_NANOS.getAcquire(this);
  }

  protected final void setExpiresAfterAccessNanos(long expiresAfterAccessNanos) {
    EXPIRES_AFTER_ACCESS_NANOS.setRelease(this, expiresAfterAccessNanos);
  }

  protected final boolean expiresAfterAccess() {
    return true;
  }

  public final Pacer pacer() {
    return pacer;
  }
}
