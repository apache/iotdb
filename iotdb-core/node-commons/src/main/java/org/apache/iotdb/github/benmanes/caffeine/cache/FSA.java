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

import java.lang.ref.ReferenceQueue;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache entry that provides the following features:
 *
 * <ul>
 *   <li>ExpireAccess
 *   <li>WeakKeys (inherited)
 *   <li>StrongValues (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"unchecked", "PMD.UnusedFormalParameter", "MissingOverride", "NullAway"})
class FSA<K, V> extends FS<K, V> {
  protected static final long ACCESS_TIME_OFFSET =
      UnsafeAccess.objectFieldOffset(
          FSA.class, org.apache.iotdb.github.benmanes.caffeine.cache.LocalCacheFactory.ACCESS_TIME);

  volatile long accessTime;

  Node<K, V> previousInAccessOrder;

  Node<K, V> nextInAccessOrder;

  FSA() {}

  FSA(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    super(key, keyReferenceQueue, value, valueReferenceQueue, weight, now);
    UnsafeAccess.UNSAFE.putLong(this, ACCESS_TIME_OFFSET, now);
  }

  FSA(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    super(keyReference, value, valueReferenceQueue, weight, now);
    UnsafeAccess.UNSAFE.putLong(this, ACCESS_TIME_OFFSET, now);
  }

  public Node<K, V> getPreviousInVariableOrder() {
    return previousInAccessOrder;
  }

  public void setPreviousInVariableOrder(Node<K, V> previousInAccessOrder) {
    this.previousInAccessOrder = previousInAccessOrder;
  }

  public Node<K, V> getNextInVariableOrder() {
    return nextInAccessOrder;
  }

  public void setNextInVariableOrder(Node<K, V> nextInAccessOrder) {
    this.nextInAccessOrder = nextInAccessOrder;
  }

  public long getVariableTime() {
    return UnsafeAccess.UNSAFE.getLong(this, ACCESS_TIME_OFFSET);
  }

  public void setVariableTime(long accessTime) {
    UnsafeAccess.UNSAFE.putLong(this, ACCESS_TIME_OFFSET, accessTime);
  }

  public boolean casVariableTime(long expect, long update) {
    return (accessTime == expect)
        && UnsafeAccess.UNSAFE.compareAndSwapLong(this, ACCESS_TIME_OFFSET, expect, update);
  }

  public final long getAccessTime() {
    return UnsafeAccess.UNSAFE.getLong(this, ACCESS_TIME_OFFSET);
  }

  public final void setAccessTime(long accessTime) {
    UnsafeAccess.UNSAFE.putLong(this, ACCESS_TIME_OFFSET, accessTime);
  }

  public final Node<K, V> getPreviousInAccessOrder() {
    return previousInAccessOrder;
  }

  public final void setPreviousInAccessOrder(Node<K, V> previousInAccessOrder) {
    this.previousInAccessOrder = previousInAccessOrder;
  }

  public final Node<K, V> getNextInAccessOrder() {
    return nextInAccessOrder;
  }

  public final void setNextInAccessOrder(Node<K, V> nextInAccessOrder) {
    this.nextInAccessOrder = nextInAccessOrder;
  }

  public Node<K, V> newNode(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    return new FSA<>(key, keyReferenceQueue, value, valueReferenceQueue, weight, now);
  }

  public Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new FSA<>(keyReference, value, valueReferenceQueue, weight, now);
  }
}
