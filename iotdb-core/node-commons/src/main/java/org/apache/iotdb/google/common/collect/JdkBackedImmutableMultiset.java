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
import org.apache.iotdb.google.common.primitives.Ints;

import java.util.Collection;
import java.util.Map;

import static org.apache.iotdb.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of ImmutableMultiset backed by a JDK Map and a list of entries. Used to protect
 * against hash flooding attacks.
 *
 * @author Louis Wasserman
 */
@GwtCompatible
@ElementTypesAreNonnullByDefault
final class JdkBackedImmutableMultiset<E> extends ImmutableMultiset<E> {
  private final Map<E, Integer> delegateMap;
  private final ImmutableList<Entry<E>> entries;
  private final long size;

  static <E> ImmutableMultiset<E> create(Collection<? extends Entry<? extends E>> entries) {
    @SuppressWarnings("unchecked")
    Entry<E>[] entriesArray = entries.toArray(new Entry[0]);
    Map<E, Integer> delegateMap = Maps.newHashMapWithExpectedSize(entriesArray.length);
    long size = 0;
    for (int i = 0; i < entriesArray.length; i++) {
      Entry<E> entry = entriesArray[i];
      int count = entry.getCount();
      size += count;
      E element = checkNotNull(entry.getElement());
      delegateMap.put(element, count);
      if (!(entry instanceof Multisets.ImmutableEntry)) {
        entriesArray[i] = Multisets.immutableEntry(element, count);
      }
    }
    return new JdkBackedImmutableMultiset<>(
        delegateMap, ImmutableList.asImmutableList(entriesArray), size);
  }

  private JdkBackedImmutableMultiset(
      Map<E, Integer> delegateMap, ImmutableList<Entry<E>> entries, long size) {
    this.delegateMap = delegateMap;
    this.entries = entries;
    this.size = size;
  }

  @Override
  public int count(Object element) {
    return delegateMap.getOrDefault(element, 0);
  }

  private transient ImmutableSet<E> elementSet;

  @Override
  public ImmutableSet<E> elementSet() {
    ImmutableSet<E> result = elementSet;
    return (result == null) ? elementSet = new ElementSet<>(entries, this) : result;
  }

  @Override
  Entry<E> getEntry(int index) {
    return entries.get(index);
  }

  @Override
  boolean isPartialView() {
    return false;
  }

  @Override
  public int size() {
    return Ints.saturatedCast(size);
  }
}
