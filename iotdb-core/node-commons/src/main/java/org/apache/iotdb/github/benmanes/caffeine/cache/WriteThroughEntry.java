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

import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * An entry that allows updates to write through to the backing map.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class WriteThroughEntry<K, V> extends SimpleEntry<K, V> {
  static final long serialVersionUID = 1;

  private final ConcurrentMap<K, V> map;

  WriteThroughEntry(ConcurrentMap<K, V> map, K key, V value) {
    super(key, value);
    this.map = requireNonNull(map);
  }

  @Override
  @SuppressWarnings("PMD.LinguisticNaming")
  public V setValue(V value) {
    map.put(getKey(), value);
    return super.setValue(value);
  }

  Object writeReplace() {
    return new SimpleEntry<>(this);
  }
}
