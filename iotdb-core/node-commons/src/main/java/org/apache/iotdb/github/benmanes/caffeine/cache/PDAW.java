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
 *   <li>ExpireWrite
 *   <li>StrongKeys (inherited)
 *   <li>SoftValues (inherited)
 *   <li>ExpireAccess (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"unchecked", "PMD.UnusedFormalParameter", "MissingOverride", "NullAway"})
class PDAW<K, V> extends PDA<K, V> {
  protected static final long WRITE_TIME_OFFSET =
      UnsafeAccess.objectFieldOffset(
          PDAW.class, org.apache.iotdb.github.benmanes.caffeine.cache.LocalCacheFactory.WRITE_TIME);

  volatile long writeTime;

  Node<K, V> previousInWriteOrder;

  Node<K, V> nextInWriteOrder;

  PDAW() {}

  PDAW(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    super(key, keyReferenceQueue, value, valueReferenceQueue, weight, now);
    UnsafeAccess.UNSAFE.putLong(this, WRITE_TIME_OFFSET, now);
  }

  PDAW(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    super(keyReference, value, valueReferenceQueue, weight, now);
    UnsafeAccess.UNSAFE.putLong(this, WRITE_TIME_OFFSET, now);
  }

  public Node<K, V> getPreviousInVariableOrder() {
    return previousInWriteOrder;
  }

  public void setPreviousInVariableOrder(Node<K, V> previousInWriteOrder) {
    this.previousInWriteOrder = previousInWriteOrder;
  }

  public Node<K, V> getNextInVariableOrder() {
    return nextInWriteOrder;
  }

  public void setNextInVariableOrder(Node<K, V> nextInWriteOrder) {
    this.nextInWriteOrder = nextInWriteOrder;
  }

  public long getVariableTime() {
    return UnsafeAccess.UNSAFE.getLong(this, WRITE_TIME_OFFSET);
  }

  public void setVariableTime(long writeTime) {
    UnsafeAccess.UNSAFE.putLong(this, WRITE_TIME_OFFSET, writeTime);
  }

  public boolean casVariableTime(long expect, long update) {
    return (writeTime == expect)
        && UnsafeAccess.UNSAFE.compareAndSwapLong(this, WRITE_TIME_OFFSET, expect, update);
  }

  public final long getWriteTime() {
    return UnsafeAccess.UNSAFE.getLong(this, WRITE_TIME_OFFSET);
  }

  public final void setWriteTime(long writeTime) {
    UnsafeAccess.UNSAFE.putLong(this, WRITE_TIME_OFFSET, writeTime);
  }

  public final Node<K, V> getPreviousInWriteOrder() {
    return previousInWriteOrder;
  }

  public final void setPreviousInWriteOrder(Node<K, V> previousInWriteOrder) {
    this.previousInWriteOrder = previousInWriteOrder;
  }

  public final Node<K, V> getNextInWriteOrder() {
    return nextInWriteOrder;
  }

  public final void setNextInWriteOrder(Node<K, V> nextInWriteOrder) {
    this.nextInWriteOrder = nextInWriteOrder;
  }

  public Node<K, V> newNode(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now) {
    return new PDAW<>(key, keyReferenceQueue, value, valueReferenceQueue, weight, now);
  }

  public Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new PDAW<>(keyReference, value, valueReferenceQueue, weight, now);
  }
}
