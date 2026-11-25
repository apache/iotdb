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

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache entry that provides the following features:
 *
 * <ul>
 *   <li>ExpireAccess
 *   <li>StrongKeys (inherited)
 *   <li>StrongValues (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
class PSA<K, V> extends PS<K, V> {
  protected static final VarHandle ACCESS_TIME;

  static {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      ACCESS_TIME = lookup.findVarHandle(PSA.class, NodeFactory.ACCESS_TIME, long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  volatile long accessTime;

  Node<K, V> previousInAccessOrder;

  Node<K, V> nextInAccessOrder;

  PSA() {}

  PSA(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    super(keyReference, value, valueReferenceQueue, weight, now);
    ACCESS_TIME.set(this, now);
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
    return (long) ACCESS_TIME.getOpaque(this);
  }

  public void setVariableTime(long accessTime) {
    ACCESS_TIME.setOpaque(this, accessTime);
  }

  public boolean casVariableTime(long expect, long update) {
    return (accessTime == expect) && ACCESS_TIME.compareAndSet(this, expect, update);
  }

  public final long getAccessTime() {
    return (long) ACCESS_TIME.getOpaque(this);
  }

  public final void setAccessTime(long accessTime) {
    ACCESS_TIME.setOpaque(this, accessTime);
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
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new PSA<>(keyReference, value, valueReferenceQueue, weight, now);
  }
}
