/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl;

public class CacheLinkedList<SK, V> {
  protected final CacheEntry<SK, V> head;
  protected final CacheEntry<SK, V> tail;

  public CacheLinkedList() {
    head = new CacheEntry<>(null, null, null);
    tail = new CacheEntry<>(null, null, null);
    head.next = tail;
    tail.pre = head;
  }

  synchronized void add(final CacheEntry<SK, V> cacheEntry) {
    CacheEntry<SK, V> nextEntry;

    do {
      nextEntry = head.next;
    } while (nextEntry.isInvalidated.get());

    cacheEntry.next = head.next;
    cacheEntry.pre = head;
    head.next.pre = cacheEntry;
    head.next = cacheEntry;
  }

  synchronized CacheEntry<SK, V> evict() {
    CacheEntry<SK, V> cacheEntry;

    do {
      cacheEntry = tail.pre;
      if (cacheEntry == head) {
        return null;
      }

    } while (cacheEntry.isInvalidated.compareAndSet(false, true));

    cacheEntry.pre.next = cacheEntry.next;
    cacheEntry.next.pre = cacheEntry.pre;
    cacheEntry.next = null;
    cacheEntry.pre = null;
    return cacheEntry;
  }
}
