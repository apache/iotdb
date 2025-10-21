/*
 * Copyright (C) 2009 The Guava Authors
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

import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * "Overrides" the {@link ImmutableMap} static methods that lack {@link ImmutableSortedMap}
 * equivalents with deprecated, exception-throwing versions. See {@link
 * ImmutableSortedSetFauxverideShim} for details.
 *
 * @author Chris Povirk
 */
abstract class ImmutableSortedMapFauxverideShim<K, V> extends ImmutableMap<K, V> {
  /**
   * Not supported. Use {@link ImmutableSortedMap#toImmutableSortedMap}, which offers better
   * type-safety, instead. This method exists only to hide {@link ImmutableMap#toImmutableMap} from
   * consumers of {@code ImmutableSortedMap}.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Use {@link ImmutableSortedMap#toImmutableSortedMap}.
   */
  @Deprecated
  public static <T extends Object, K, V> Collector<T, ?, ImmutableMap<K, V>> toImmutableMap(
      Function<? super T, ? extends K> keyFunction,
      Function<? super T, ? extends V> valueFunction) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Use {@link ImmutableSortedMap#toImmutableSortedMap}, which offers better
   * type-safety, instead. This method exists only to hide {@link ImmutableMap#toImmutableMap} from
   * consumers of {@code ImmutableSortedMap}.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Use {@link ImmutableSortedMap#toImmutableSortedMap}.
   */
  @Deprecated
  public static <T extends Object, K, V> Collector<T, ?, ImmutableMap<K, V>> toImmutableMap(
      Function<? super T, ? extends K> keyFunction,
      Function<? super T, ? extends V> valueFunction,
      BinaryOperator<V> mergeFunction) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Use {@link ImmutableSortedMap#naturalOrder}, which offers better type-safety,
   * instead. This method exists only to hide {@link ImmutableMap#builder} from consumers of {@code
   * ImmutableSortedMap}.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Use {@link ImmutableSortedMap#naturalOrder}, which offers better type-safety.
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap.Builder<K, V> builder() {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported for ImmutableSortedMap.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Not supported for ImmutableSortedMap.
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap.Builder<K, V> builderWithExpectedSize(int expectedSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain a non-{@code Comparable}
   * key.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this dummy
   * version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass a key of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(K k1, V v1) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(K k1, V v1, K k2, V v2) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls to will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8,
      K k9,
      V v9) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a map that may contain non-{@code Comparable}
   * keys.</b> Proper calls will resolve to the version in {@code ImmutableSortedMap}, not this
   * dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass keys of type {@code Comparable} to use {@link
   *     ImmutableSortedMap#of(Comparable, Object, Comparable, Object, Comparable, Object,
   *     Comparable, Object, Comparable, Object)}.</b>
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8,
      K k9,
      V v9,
      K k10,
      V v10) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. Use {@code ImmutableSortedMap.copyOf(ImmutableMap.ofEntries(...))}.
   *
   * @deprecated Use {@code ImmutableSortedMap.copyOf(ImmutableMap.ofEntries(...))}.
   */
  @Deprecated
  public static <K, V> ImmutableSortedMap<K, V> ofEntries(
      Entry<? extends K, ? extends V>... entries) {
    throw new UnsupportedOperationException();
  }

  // No copyOf() fauxveride; see ImmutableSortedSetFauxverideShim.
}
