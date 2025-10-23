/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.commons.external.collections4.map;

import org.apache.tsfile.external.commons.collections4.IterableMap;
import org.apache.tsfile.external.commons.collections4.MapIterator;

/**
 * Provide a basic {@link IterableMap} implementation.
 *
 * @param <K> the type of the keys in this map
 * @param <V> the type of the values in this map
 * @since 4.0
 */
public abstract class AbstractIterableMap<K, V> implements IterableMap<K, V> {

  /** {@inheritDoc} */
  @Override
  public MapIterator<K, V> mapIterator() {
    return new EntrySetToMapIteratorAdapter<>(entrySet());
  }
}
