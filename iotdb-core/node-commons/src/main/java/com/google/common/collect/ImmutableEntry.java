/*
 * Copyright (C) 2008 The Guava Authors
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

import java.io.Serializable;

/**
 * An immutable {@code Map.Entry}, used both by {@link
 * com.google.common.collect.Maps#immutableEntry(Object, Object)} and by other parts of {@code
 * common.collect} as a superclass.
 */
class ImmutableEntry<K extends Object, V extends Object> extends AbstractMapEntry<K, V>
    implements Serializable {
  final K key;
  final V value;

  ImmutableEntry(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public final K getKey() {
    return key;
  }

  @Override
  public final V getValue() {
    return value;
  }

  @Override
  public final V setValue(V value) {
    throw new UnsupportedOperationException();
  }

  private static final long serialVersionUID = 0;
}
