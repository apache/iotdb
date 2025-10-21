/*
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.collect;

import com.google.common.base.Predicate;

import javax.annotation.CheckForNull;

import java.util.List;

/**
 * Implementation of {@link Multimaps#filterKeys(ListMultimap, Predicate)}.
 *
 * @author Louis Wasserman
 */
final class FilteredKeyListMultimap<K extends Object, V extends Object>
    extends FilteredKeyMultimap<K, V> implements ListMultimap<K, V> {
  FilteredKeyListMultimap(ListMultimap<K, V> unfiltered, Predicate<? super K> keyPredicate) {
    super(unfiltered, keyPredicate);
  }

  @Override
  public ListMultimap<K, V> unfiltered() {
    return (ListMultimap<K, V>) super.unfiltered();
  }

  @Override
  public List<V> get(K key) {
    return (List<V>) super.get(key);
  }

  @Override
  public List<V> removeAll(@CheckForNull Object key) {
    return (List<V>) super.removeAll(key);
  }

  @Override
  public List<V> replaceValues(K key, Iterable<? extends V> values) {
    return (List<V>) super.replaceValues(key, values);
  }
}
