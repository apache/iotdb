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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Provides a base decorator that enables additional functionality to be added to a Map via
 * decoration.
 *
 * <p>Methods are forwarded directly to the decorated map.
 *
 * <p>This implementation does not perform any special processing with {@link #entrySet()}, {@link
 * #keySet()} or {@link #values()}. Instead it simply returns the set/collection from the wrapped
 * map. This may be undesirable, for example if you are trying to write a validating implementation
 * it would provide a loophole around the validation. But, you might want that loophole, so this
 * class is kept simple.
 *
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 * @since 3.0
 */
public abstract class AbstractMapDecorator<K, V> extends AbstractIterableMap<K, V> {

  /** The map to decorate */
  transient Map<K, V> map;

  /**
   * Constructor only used in deserialization, do not use otherwise.
   *
   * @since 3.1
   */
  protected AbstractMapDecorator() {
    super();
  }

  /**
   * Constructor that wraps (not copies).
   *
   * @param map the map to decorate, must not be null
   * @throws NullPointerException if the map is null
   */
  protected AbstractMapDecorator(final Map<K, V> map) {
    if (map == null) {
      throw new NullPointerException("Map must not be null.");
    }
    this.map = map;
  }

  /**
   * Gets the map being decorated.
   *
   * @return the decorated map
   */
  protected Map<K, V> decorated() {
    return map;
  }

  // -----------------------------------------------------------------------
  @Override
  public void clear() {
    decorated().clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    return decorated().containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return decorated().containsValue(value);
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return decorated().entrySet();
  }

  @Override
  public V get(final Object key) {
    return decorated().get(key);
  }

  @Override
  public boolean isEmpty() {
    return decorated().isEmpty();
  }

  @Override
  public Set<K> keySet() {
    return decorated().keySet();
  }

  @Override
  public V put(final K key, final V value) {
    return decorated().put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> mapToCopy) {
    decorated().putAll(mapToCopy);
  }

  @Override
  public V remove(final Object key) {
    return decorated().remove(key);
  }

  @Override
  public int size() {
    return decorated().size();
  }

  @Override
  public Collection<V> values() {
    return decorated().values();
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    return decorated().equals(object);
  }

  @Override
  public int hashCode() {
    return decorated().hashCode();
  }

  @Override
  public String toString() {
    return decorated().toString();
  }
}
