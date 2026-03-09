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

import org.apache.iotdb.commons.external.collections4.IterableSortedMap;
import org.apache.iotdb.commons.external.collections4.iterators.ListIteratorWrapper;

import org.apache.tsfile.external.commons.collections4.OrderedMapIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Provides a base decorator that enables additional functionality to be added to a Map via
 * decoration.
 *
 * <p>Methods are forwarded directly to the decorated map.
 *
 * <p>This implementation does not perform any special processing with the map views. Instead it
 * simply returns the set/collection from the wrapped map. This may be undesirable, for example if
 * you are trying to write a validating implementation it would provide a loophole around the
 * validation. But, you might want that loophole, so this class is kept simple.
 *
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 * @since 3.0
 */
public abstract class AbstractSortedMapDecorator<K, V> extends AbstractMapDecorator<K, V>
    implements IterableSortedMap<K, V> {

  /**
   * Constructor only used in deserialization, do not use otherwise.
   *
   * @since 3.1
   */
  protected AbstractSortedMapDecorator() {
    super();
  }

  /**
   * Constructor that wraps (not copies).
   *
   * @param map the map to decorate, must not be null
   * @throws NullPointerException if the map is null
   */
  public AbstractSortedMapDecorator(final SortedMap<K, V> map) {
    super(map);
  }

  /**
   * Gets the map being decorated.
   *
   * @return the decorated map
   */
  @Override
  protected SortedMap<K, V> decorated() {
    return (SortedMap<K, V>) super.decorated();
  }

  // -----------------------------------------------------------------------
  @Override
  public Comparator<? super K> comparator() {
    return decorated().comparator();
  }

  @Override
  public K firstKey() {
    return decorated().firstKey();
  }

  @Override
  public K lastKey() {
    return decorated().lastKey();
  }

  @Override
  public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
    return decorated().subMap(fromKey, toKey);
  }

  @Override
  public SortedMap<K, V> headMap(final K toKey) {
    return decorated().headMap(toKey);
  }

  @Override
  public SortedMap<K, V> tailMap(final K fromKey) {
    return decorated().tailMap(fromKey);
  }

  @Override
  public K previousKey(final K key) {
    final SortedMap<K, V> headMap = headMap(key);
    return headMap.isEmpty() ? null : headMap.lastKey();
  }

  @Override
  public K nextKey(final K key) {
    final Iterator<K> it = tailMap(key).keySet().iterator();
    it.next();
    return it.hasNext() ? it.next() : null;
  }

  /** {@inheritDoc} */
  @Override
  public OrderedMapIterator<K, V> mapIterator() {
    return new SortedMapIterator<>(entrySet());
  }

  /**
   * OrderedMapIterator implementation.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  protected static class SortedMapIterator<K, V> extends EntrySetToMapIteratorAdapter<K, V>
      implements OrderedMapIterator<K, V> {

    /**
     * Create a new AbstractSortedMapDecorator.SortedMapIterator.
     *
     * @param entrySet the entrySet to iterate
     */
    protected SortedMapIterator(final Set<Map.Entry<K, V>> entrySet) {
      super(entrySet);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void reset() {
      super.reset();
      iterator = new ListIteratorWrapper<>(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasPrevious() {
      return ((ListIterator<Map.Entry<K, V>>) iterator).hasPrevious();
    }

    /** {@inheritDoc} */
    @Override
    public K previous() {
      entry = ((ListIterator<Map.Entry<K, V>>) iterator).previous();
      return getKey();
    }
  }
}
