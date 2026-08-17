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

import org.apache.iotdb.github.benmanes.caffeine.cache.References.LookupKeyReference;
import org.apache.iotdb.github.benmanes.caffeine.cache.References.WeakKeyReference;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Objects;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache entry that provides the following features:
 *
 * <ul>
 *   <li>WeakKeys
 *   <li>StrongValues
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"unchecked", "PMD.UnusedFormalParameter", "MissingOverride", "NullAway"})
class FS<K, V> extends Node<K, V> implements NodeFactory<K, V> {
  protected static final long KEY_OFFSET =
      UnsafeAccess.objectFieldOffset(
          FS.class, org.apache.iotdb.github.benmanes.caffeine.cache.LocalCacheFactory.KEY);

  protected static final long VALUE_OFFSET =
      UnsafeAccess.objectFieldOffset(
          FS.class, org.apache.iotdb.github.benmanes.caffeine.cache.LocalCacheFactory.VALUE);

  volatile WeakKeyReference<K> key;

  volatile V value;

  FS() {}

  FS(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    this(new WeakKeyReference<K>(key, keyReferenceQueue), value, valueReferenceQueue, weight, now);
  }

  FS(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    UnsafeAccess.UNSAFE.putObject(this, KEY_OFFSET, keyReference);
    UnsafeAccess.UNSAFE.putObject(this, VALUE_OFFSET, value);
  }

  public final K getKey() {
    return ((Reference<K>) UnsafeAccess.UNSAFE.getObject(this, KEY_OFFSET)).get();
  }

  public final Object getKeyReference() {
    return UnsafeAccess.UNSAFE.getObject(this, KEY_OFFSET);
  }

  public final V getValue() {
    return value;
  }

  public final Object getValueReference() {
    return UnsafeAccess.UNSAFE.getObject(this, VALUE_OFFSET);
  }

  public final void setValue(V value, ReferenceQueue<V> referenceQueue) {
    UnsafeAccess.UNSAFE.putObject(this, VALUE_OFFSET, value);
  }

  public final boolean containsValue(Object value) {
    return Objects.equals(value, getValue());
  }

  public Node<K, V> newNode(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    return new FS<>(key, keyReferenceQueue, value, valueReferenceQueue, weight, now);
  }

  public Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new FS<>(keyReference, value, valueReferenceQueue, weight, now);
  }

  public Object newLookupKey(Object key) {
    return new LookupKeyReference<>(key);
  }

  public Object newReferenceKey(K key, ReferenceQueue<K> referenceQueue) {
    return new WeakKeyReference<K>(key, referenceQueue);
  }

  public final boolean isAlive() {
    Object key = getKeyReference();
    return (key != RETIRED_WEAK_KEY) && (key != DEAD_WEAK_KEY);
  }

  public final boolean isRetired() {
    return (getKeyReference() == RETIRED_WEAK_KEY);
  }

  public final void retire() {
    UnsafeAccess.UNSAFE.putObject(this, KEY_OFFSET, RETIRED_WEAK_KEY);
  }

  public final boolean isDead() {
    return (getKeyReference() == DEAD_WEAK_KEY);
  }

  public final void die() {
    UnsafeAccess.UNSAFE.putObject(this, VALUE_OFFSET, null);
    UnsafeAccess.UNSAFE.putObject(this, KEY_OFFSET, DEAD_WEAK_KEY);
  }
}
