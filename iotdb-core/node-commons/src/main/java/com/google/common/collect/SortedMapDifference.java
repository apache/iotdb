/*
 * Copyright (C) 2010 The Guava Authors
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

import java.util.SortedMap;

/**
 * An object representing the differences between two sorted maps.
 *
 * @author Louis Wasserman
 * @since 8.0
 */
public interface SortedMapDifference<K extends Object, V extends Object>
    extends MapDifference<K, V> {

  @Override
  SortedMap<K, V> entriesOnlyOnLeft();

  @Override
  SortedMap<K, V> entriesOnlyOnRight();

  @Override
  SortedMap<K, V> entriesInCommon();

  @Override
  SortedMap<K, ValueDifference<V>> entriesDiffering();
}
