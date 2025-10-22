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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Methods factored out so that they can be emulated differently in GWT.
 *
 * @author Hayward Chan
 */
final class Platform {
  private static final java.util.logging.Logger logger =
      java.util.logging.Logger.getLogger(Platform.class.getName());

  /** Returns the platform preferred implementation of a map based on a hash table. */
  static <K extends Object, V extends Object> Map<K, V> newHashMapWithExpectedSize(
      int expectedSize) {
    return Maps.newHashMapWithExpectedSize(expectedSize);
  }

  /**
   * Returns the platform preferred implementation of an insertion ordered map based on a hash
   * table.
   */
  static <K extends Object, V extends Object> Map<K, V> newLinkedHashMapWithExpectedSize(
      int expectedSize) {
    return Maps.newLinkedHashMapWithExpectedSize(expectedSize);
  }

  /** Returns the platform preferred implementation of a set based on a hash table. */
  static <E extends Object> Set<E> newHashSetWithExpectedSize(int expectedSize) {
    return Sets.newHashSetWithExpectedSize(expectedSize);
  }

  /** Returns the platform preferred implementation of a thread-safe hash set. */
  static <E> Set<E> newConcurrentHashSet() {
    return ConcurrentHashMap.newKeySet();
  }

  /**
   * Returns the platform preferred implementation of an insertion ordered set based on a hash
   * table.
   */
  static <E extends Object> Set<E> newLinkedHashSetWithExpectedSize(int expectedSize) {
    return Sets.newLinkedHashSetWithExpectedSize(expectedSize);
  }

  /**
   * Returns the platform preferred map implementation that preserves insertion order when used only
   * for insertions.
   */
  static <K extends Object, V extends Object> Map<K, V> preservesInsertionOrderOnPutsMap() {
    return Maps.newLinkedHashMap();
  }

  /**
   * Returns the platform preferred set implementation that preserves insertion order when used only
   * for insertions.
   */
  static <E extends Object> Set<E> preservesInsertionOrderOnAddsSet() {
    return Sets.newLinkedHashSet();
  }

  /**
   * Returns a new array of the given length with the same type as a reference array.
   *
   * @param reference any array of the desired type
   * @param length the length of the new array
   */
  /*
   * The new array contains nulls, even if the old array did not. If we wanted to be accurate, we
   * would declare a return type of `T[]`. However, we've decided not to think too hard
   * about arrays for now, as they're a mess. (We previously discussed this in the review of
   * ObjectArrays, which is the main caller of this method.)
   */
  static <T extends Object> T[] newArray(T[] reference, int length) {
    T[] empty = reference.length == 0 ? reference : Arrays.copyOf(reference, 0);
    return Arrays.copyOf(empty, length);
  }

  /** Equivalent to Arrays.copyOfRange(source, from, to, arrayOfType.getClass()). */
  /*
   * Arrays are a mess from a nullness perspective, and Class instances for object-array types are
   * even worse. For now, we just suppress and move on with our lives.
   *
   * - https://github.com/jspecify/jspecify/issues/65
   *
   * - https://github.com/jspecify/jdk/commit/71d826792b8c7ef95d492c50a274deab938f2552
   */
  @SuppressWarnings("nullness")
  static <T extends Object> T[] copy(Object[] source, int from, int to, T[] arrayOfType) {
    return Arrays.copyOfRange(source, from, to, (Class<? extends T[]>) arrayOfType.getClass());
  }

  /**
   * Configures the given map maker to use weak keys, if possible; does nothing otherwise (i.e., in
   * GWT). This is sometimes acceptable, when only server-side code could generate enough volume
   * that reclamation becomes important.
   */
  static MapMaker tryWeakKeys(MapMaker mapMaker) {
    return mapMaker.weakKeys();
  }

  static <E extends Enum<E>> Class<E> getDeclaringClassOrObjectForJ2cl(E e) {
    return e.getDeclaringClass();
  }

  static int reduceIterationsIfGwt(int iterations) {
    return iterations;
  }

  static int reduceExponentIfGwt(int exponent) {
    return exponent;
  }

  private Platform() {}
}
