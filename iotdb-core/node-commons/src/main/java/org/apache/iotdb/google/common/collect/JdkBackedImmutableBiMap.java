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

package org.apache.iotdb.google.common.collect;

import org.apache.iotdb.google.common.annotations.GwtCompatible;
import org.apache.iotdb.google.common.annotations.VisibleForTesting;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of ImmutableBiMap backed by a pair of JDK HashMaps, which have smartness
 * protecting against hash flooding.
 */
@GwtCompatible(emulated = true)
@ElementTypesAreNonnullByDefault
final class JdkBackedImmutableBiMap<K, V> extends ImmutableBiMap<K, V> {
  @VisibleForTesting
  static <K, V> ImmutableBiMap<K, V> create(int n, Entry<K, V>[] entryArray) {
    Map<K, V> forwardDelegate = Maps.newHashMapWithExpectedSize(n);
    Map<V, K> backwardDelegate = Maps.newHashMapWithExpectedSize(n);
    for (int i = 0; i < n; i++) {
      // requireNonNull is safe because the first `n` elements have been filled in.
      Entry<K, V> e = RegularImmutableMap.makeImmutable(requireNonNull(entryArray[i]));
      entryArray[i] = e;
      V oldValue = forwardDelegate.putIfAbsent(e.getKey(), e.getValue());
      if (oldValue != null) {
        throw conflictException("key", e.getKey() + "=" + oldValue, entryArray[i]);
      }
      K oldKey = backwardDelegate.putIfAbsent(e.getValue(), e.getKey());
      if (oldKey != null) {
        throw conflictException("value", oldKey + "=" + e.getValue(), entryArray[i]);
      }
    }
    ImmutableList<Entry<K, V>> entryList = ImmutableList.asImmutableList(entryArray, n);
    return new JdkBackedImmutableBiMap<>(entryList, forwardDelegate, backwardDelegate);
  }

  private final transient ImmutableList<Entry<K, V>> entries;
  private final Map<K, V> forwardDelegate;
  private final Map<V, K> backwardDelegate;

  private JdkBackedImmutableBiMap(
      ImmutableList<Entry<K, V>> entries, Map<K, V> forwardDelegate, Map<V, K> backwardDelegate) {
    this.entries = entries;
    this.forwardDelegate = forwardDelegate;
    this.backwardDelegate = backwardDelegate;
  }

  @Override
  public int size() {
    return entries.size();
  }

  private transient JdkBackedImmutableBiMap<V, K> inverse;

  @Override
  public ImmutableBiMap<V, K> inverse() {
    JdkBackedImmutableBiMap<V, K> result = inverse;
    if (result == null) {
      inverse =
          result =
              new JdkBackedImmutableBiMap<>(
                  new InverseEntries(), backwardDelegate, forwardDelegate);
      result.inverse = this;
    }
    return result;
  }

  private final class InverseEntries extends ImmutableList<Entry<V, K>> {
    @Override
    public Entry<V, K> get(int index) {
      Entry<K, V> entry = entries.get(index);
      return Maps.immutableEntry(entry.getValue(), entry.getKey());
    }

    @Override
    boolean isPartialView() {
      return false;
    }

    @Override
    public int size() {
      return entries.size();
    }
  }

  @Override
  public V get(Object key) {
    return forwardDelegate.get(key);
  }

  @Override
  ImmutableSet<Entry<K, V>> createEntrySet() {
    return new ImmutableMapEntrySet.RegularEntrySet<>(this, entries);
  }

  @Override
  ImmutableSet<K> createKeySet() {
    return new ImmutableMapKeySet<>(this);
  }

  @Override
  boolean isPartialView() {
    return false;
  }
}
