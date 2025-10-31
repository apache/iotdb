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
package org.apache.iotdb.commons.external.collections4;

import org.apache.tsfile.external.commons.collections4.IterableMap;

import java.util.Set;

/**
 * Defines a map that allows bidirectional lookup between key and values.
 *
 * <p>This extended <code>Map</code> represents a mapping where a key may lookup a value and a value
 * may lookup a key with equal ease. This interface extends <code>Map</code> and so may be used
 * anywhere a map is required. The interface provides an inverse map view, enabling full access to
 * both directions of the <code>BidiMap</code>.
 *
 * <p>Implementations should allow a value to be looked up from a key and a key to be looked up from
 * a value with equal performance.
 *
 * <p>This map enforces the restriction that there is a 1:1 relation between keys and values,
 * meaning that multiple keys cannot map to the same value. This is required so that "inverting" the
 * map results in a map without duplicate keys. See the {@link #put} method description for more
 * information.
 *
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 * @since 3.0
 */
public interface BidiMap<K, V> extends IterableMap<K, V> {

  /**
   * Puts the key-value pair into the map, replacing any previous pair.
   *
   * <p>When adding a key-value pair, the value may already exist in the map against a different
   * key. That mapping is removed, to ensure that the value only occurs once in the inverse map.
   *
   * <pre>
   *  BidiMap map1 = new DualHashBidiMap();
   *  map.put("A","B");  // contains A mapped to B, as per Map
   *  map.put("A","C");  // contains A mapped to C, as per Map
   *
   *  BidiMap map2 = new DualHashBidiMap();
   *  map.put("A","B");  // contains A mapped to B, as per Map
   *  map.put("C","B");  // contains C mapped to B, key A is removed
   * </pre>
   *
   * @param key the key to store
   * @param value the value to store
   * @return the previous value mapped to this key
   * @throws UnsupportedOperationException if the <code>put</code> method is not supported
   * @throws ClassCastException (optional) if the map limits the type of the value and the specified
   *     value is inappropriate
   * @throws IllegalArgumentException (optional) if the map limits the values in some way and the
   *     value was invalid
   * @throws NullPointerException (optional) if the map limits the values to non-null and null was
   *     specified
   */
  @Override
  V put(K key, V value);

  /**
   * Gets the key that is currently mapped to the specified value.
   *
   * <p>If the value is not contained in the map, <code>null</code> is returned.
   *
   * <p>Implementations should seek to make this method perform equally as well as <code>get(Object)
   * </code>.
   *
   * @param value the value to find the key for
   * @return the mapped key, or <code>null</code> if not found
   * @throws ClassCastException (optional) if the map limits the type of the value and the specified
   *     value is inappropriate
   * @throws NullPointerException (optional) if the map limits the values to non-null and null was
   *     specified
   */
  K getKey(Object value);

  /**
   * Removes the key-value pair that is currently mapped to the specified value (optional
   * operation).
   *
   * <p>If the value is not contained in the map, <code>null</code> is returned.
   *
   * <p>Implementations should seek to make this method perform equally as well as <code>
   * remove(Object)</code>.
   *
   * @param value the value to find the key-value pair for
   * @return the key that was removed, <code>null</code> if nothing removed
   * @throws ClassCastException (optional) if the map limits the type of the value and the specified
   *     value is inappropriate
   * @throws NullPointerException (optional) if the map limits the values to non-null and null was
   *     specified
   * @throws UnsupportedOperationException if this method is not supported by the implementation
   */
  K removeValue(Object value);

  /**
   * Gets a view of this map where the keys and values are reversed.
   *
   * <p>Changes to one map will be visible in the other and vice versa. This enables both directions
   * of the map to be accessed as a <code>Map</code>.
   *
   * <p>Implementations should seek to avoid creating a new object every time this method is called.
   * See <code>AbstractMap.values()</code> etc. Calling this method on the inverse map should return
   * the original.
   *
   * @return an inverted bidirectional map
   */
  BidiMap<V, K> inverseBidiMap();

  /**
   * Returns a {@link Set} view of the values contained in this map. The set is backed by the map,
   * so changes to the map are reflected in the set, and vice-versa. If the map is modified while an
   * iteration over the set is in progress (except through the iterator's own {@code remove}
   * operation), the results of the iteration are undefined. The set supports element removal, which
   * removes the corresponding mapping from the map, via the {@code Iterator.remove}, {@code
   * Collection.remove}, {@code removeAll}, {@code retainAll} and {@code clear} operations. It does
   * not support the {@code add} or {@code addAll} operations.
   *
   * @return a set view of the values contained in this map
   */
  @Override
  Set<V> values();
}
