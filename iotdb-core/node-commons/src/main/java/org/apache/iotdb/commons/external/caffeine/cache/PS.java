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
import java.lang.ref.ReferenceQueue;
import java.util.Objects;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache entry that provides the following features:
 *
 * <ul>
 *   <li>StrongKeys
 *   <li>StrongValues
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"all", "unchecked"})
class PS<K, V> extends Node<K, V> implements NodeFactory<K, V> {
  protected static final VarHandle KEY;

  protected static final VarHandle VALUE;

  static {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      KEY = lookup.findVarHandle(PS.class, NodeFactory.KEY, Object.class);
      VALUE = lookup.findVarHandle(PS.class, NodeFactory.VALUE, Object.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  volatile K key;

  volatile V value;

  PS() {}

  PS(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    KEY.set(this, keyReference);
    VALUE.set(this, value);
  }

  public final K getKey() {
    return (K) KEY.getOpaque(this);
  }

  public final Object getKeyReference() {
    return KEY.getOpaque(this);
  }

  public final Object getKeyReferenceOrNull() {
    return KEY.getOpaque(this);
  }

  public final V getValue() {
    return (V) VALUE.getAcquire(this);
  }

  public final void setValue(V value, ReferenceQueue<V> referenceQueue) {
    // The fence orders the store before any subsequent timestamp store, pairing with the reader's
    // fence in BoundedLocalCache.hasExpired so that a fresh expiration timestamp is never observed
    // with a stale value
    VALUE.setRelease(this, value);
    VarHandle.storeStoreFence();
  }

  public final Object getValueReference() {
    return VALUE.getAcquire(this);
  }

  public final boolean containsValue(Object value) {
    return Objects.equals(value, getValue());
  }

  public Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new PS<>(keyReference, value, valueReferenceQueue, weight, now);
  }

  public final boolean isAlive() {
    Object key = getKeyReference();
    return (key != RETIRED_STRONG_KEY) && (key != DEAD_STRONG_KEY);
  }

  public final boolean isRetired() {
    return (getKeyReference() == RETIRED_STRONG_KEY);
  }

  public final void retire() {
    KEY.set(this, RETIRED_STRONG_KEY);
  }

  public final boolean isDead() {
    return (getKeyReference() == DEAD_STRONG_KEY);
  }

  public final void die() {
    VALUE.set(this, null);
    KEY.set(this, DEAD_STRONG_KEY);
  }
}
