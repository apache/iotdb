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
 *   <li>ExpireWrite
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
class SSLSMSW<K, V> extends SSLSMSPadAfterMaximum<K, V> {
  static final LocalCacheFactory FACTORY = SSLSMSW::new;

  protected static final VarHandle EXPIRES_AFTER_WRITE_NANOS;

  static {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      EXPIRES_AFTER_WRITE_NANOS =
          lookup.findVarHandle(
              SSLSMSW.class, LocalCacheFactory.EXPIRES_AFTER_WRITE_NANOS, long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  final Ticker ticker;

  final WriteOrderDeque<Node<K, V>> writeOrderDeque;

  volatile long expiresAfterWriteNanos;

  final Pacer pacer;

  SSLSMSW(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
    this.ticker = builder.getTicker();
    this.writeOrderDeque = new WriteOrderDeque<Node<K, V>>();
    setExpiresAfterWriteNanos(builder.getExpiresAfterWriteNanos());
    Scheduler scheduler = builder.getScheduler();
    this.pacer = (scheduler == Scheduler.disabledScheduler()) ? null : new Pacer(scheduler);
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
    return (long) EXPIRES_AFTER_WRITE_NANOS.getAcquire(this);
  }

  protected final void setExpiresAfterWriteNanos(long expiresAfterWriteNanos) {
    EXPIRES_AFTER_WRITE_NANOS.setRelease(this, expiresAfterWriteNanos);
  }

  public final Pacer pacer() {
    return pacer;
  }
}
