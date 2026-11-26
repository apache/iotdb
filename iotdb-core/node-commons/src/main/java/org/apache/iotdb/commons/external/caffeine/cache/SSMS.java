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
 *   <li>MaximumSize
 *   <li>StrongKeys (inherited)
 *   <li>StrongValues (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
class SSMS<K, V> extends SSPadBeforeMaximum<K, V> {
  static final LocalCacheFactory FACTORY = SSMS::new;

  protected static final VarHandle MAXIMUM;

  protected static final VarHandle WEIGHTED_SIZE;

  static {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      MAXIMUM = lookup.findVarHandle(SSMS.class, LocalCacheFactory.MAXIMUM, long.class);
      WEIGHTED_SIZE = lookup.findVarHandle(SSMS.class, LocalCacheFactory.WEIGHTED_SIZE, long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  volatile long maximum;

  volatile long weightedSize;

  long windowMaximum;

  long windowWeightedSize;

  long mainProtectedMaximum;

  long mainProtectedWeightedSize;

  SSMS(Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
  }

  protected final boolean evicts() {
    return true;
  }

  protected final long maximum() {
    return (long) MAXIMUM.get(this);
  }

  protected final long maximumAcquire() {
    return (long) MAXIMUM.getAcquire(this);
  }

  protected final void setMaximum(long maximum) {
    MAXIMUM.setRelease(this, maximum);
  }

  protected final long weightedSize() {
    return (long) WEIGHTED_SIZE.get(this);
  }

  protected final long weightedSizeAcquire() {
    return (long) WEIGHTED_SIZE.getAcquire(this);
  }

  protected final void setWeightedSize(long weightedSize) {
    WEIGHTED_SIZE.setRelease(this, weightedSize);
  }

  protected final long windowMaximum() {
    return windowMaximum;
  }

  protected final void setWindowMaximum(long windowMaximum) {
    this.windowMaximum = windowMaximum;
  }

  protected final long windowWeightedSize() {
    return windowWeightedSize;
  }

  protected final void setWindowWeightedSize(long windowWeightedSize) {
    this.windowWeightedSize = windowWeightedSize;
  }

  protected final long mainProtectedMaximum() {
    return mainProtectedMaximum;
  }

  protected final void setMainProtectedMaximum(long mainProtectedMaximum) {
    this.mainProtectedMaximum = mainProtectedMaximum;
  }

  protected final long mainProtectedWeightedSize() {
    return mainProtectedWeightedSize;
  }

  protected final void setMainProtectedWeightedSize(long mainProtectedWeightedSize) {
    this.mainProtectedWeightedSize = mainProtectedWeightedSize;
  }

  protected boolean fastpath() {
    return true;
  }
}
