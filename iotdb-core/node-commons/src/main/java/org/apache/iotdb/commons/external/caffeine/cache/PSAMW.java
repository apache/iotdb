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

import java.lang.ref.ReferenceQueue;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A cache entry that provides the following features:
 *
 * <ul>
 *   <li>MaximumWeight
 *   <li>StrongKeys (inherited)
 *   <li>StrongValues (inherited)
 *   <li>ExpireAccess (inherited)
 * </ul>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
final class PSAMW<K, V> extends PSA<K, V> {
  int metadata;

  int weight;

  int policyWeight;

  PSAMW() {}

  PSAMW(Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    super(keyReference, value, valueReferenceQueue, weight, now);
    this.weight = weight;
  }

  public int getQueueType() {
    return metadata & QUEUE_MASK;
  }

  public void setQueueType(int queueType) {
    this.metadata = (this.metadata & ~QUEUE_MASK) | queueType;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public long getPolicyWeight() {
    return ((long) (metadata >> QUEUE_BITS) << Integer.SIZE) | Integer.toUnsignedLong(policyWeight);
  }

  public void setPolicyWeight(long policyWeight) {
    this.policyWeight = (int) policyWeight;
    this.metadata =
        (this.metadata & QUEUE_MASK) | ((int) (policyWeight >> Integer.SIZE) << QUEUE_BITS);
  }

  public Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now) {
    return new PSAMW<>(keyReference, value, valueReferenceQueue, weight, now);
  }
}
