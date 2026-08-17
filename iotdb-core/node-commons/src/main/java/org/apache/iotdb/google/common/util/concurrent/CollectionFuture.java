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

package org.apache.iotdb.google.common.util.concurrent;

import org.apache.iotdb.google.common.annotations.GwtCompatible;
import org.apache.iotdb.google.common.collect.ImmutableCollection;
import org.apache.iotdb.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.apache.iotdb.google.common.collect.Lists.newArrayListWithCapacity;

/** Aggregate future that collects (stores) results of each future. */
@GwtCompatible(emulated = true)
@ElementTypesAreNonnullByDefault
abstract class CollectionFuture<V extends Object, C extends Object> extends AggregateFuture<V, C> {
  /*
   * We access this field racily but safely. For discussion of a similar situation, see the comments
   * on the fields of TimeoutFuture. This field is slightly different from the fields discussed
   * there: cancel() never reads this field, only writes to it. That makes the race here completely
   * harmless, rather than just 99.99% harmless.
   */
  private List<Present<V>> values;

  CollectionFuture(
      ImmutableCollection<? extends ListenableFuture<? extends V>> futures,
      boolean allMustSucceed) {
    super(futures, allMustSucceed, true);

    List<Present<V>> values =
        futures.isEmpty()
            ? Collections.<Present<V>>emptyList()
            : Lists.<Present<V>>newArrayListWithCapacity(futures.size());

    // Populate the results list with null initially.
    for (int i = 0; i < futures.size(); ++i) {
      values.add(null);
    }

    this.values = values;
  }

  @Override
  final void collectOneValue(int index, @ParametricNullness V returnValue) {
    List<Present<V>> localValues = values;
    if (localValues != null) {
      localValues.set(index, new Present<>(returnValue));
    }
  }

  @Override
  final void handleAllCompleted() {
    List<Present<V>> localValues = values;
    if (localValues != null) {
      set(combine(localValues));
    }
  }

  @Override
  void releaseResources(ReleaseResourcesReason reason) {
    super.releaseResources(reason);
    this.values = null;
  }

  abstract C combine(List<Present<V>> values);

  /** Used for {@link Futures#allAsList} and {@link Futures#successfulAsList}. */
  static final class ListFuture<V extends Object> extends CollectionFuture<V, List<V>> {
    ListFuture(
        ImmutableCollection<? extends ListenableFuture<? extends V>> futures,
        boolean allMustSucceed) {
      super(futures, allMustSucceed);
      init();
    }

    @Override
    public List<V> combine(List<Present<V>> values) {
      List<V> result = newArrayListWithCapacity(values.size());
      for (Present<V> element : values) {
        result.add(element != null ? element.value : null);
      }
      return unmodifiableList(result);
    }
  }

  /** The result of a successful {@code Future}. */
  private static final class Present<V extends Object> {
    @ParametricNullness final V value;

    Present(@ParametricNullness V value) {
      this.value = value;
    }
  }
}
