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

package org.apache.iotdb.library.dprofile.util;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * this class is copied and modified from <a
 * href="https://github.com/ggalmazor/lt_downsampling_java8">...</a> project
 */
class Bucket<T extends Pair<Long, Double>> {
  private final List<T> data;
  private final T first;
  private final T last;
  private final Pair<Long, Double> center;
  private final T result;

  private Bucket(List<T> data, T first, T last, Pair<Long, Double> center, T result) {
    this.data = data;
    this.first = first;
    this.last = last;
    this.center = center;
    this.result = result;
  }

  static <U extends Pair<Long, Double>> Bucket<U> of(List<U> us) {
    U first = us.get(0);
    U last = us.get(us.size() - 1);
    Pair<Long, Double> center = centerBetween(first, last);
    return new Bucket<>(us, first, last, center, first);
  }

  static <U extends Pair<Long, Double>> Bucket<U> of(U u) {
    return new Bucket<>(Collections.singletonList(u), u, u, u, u);
  }

  T getResult() {
    return result;
  }

  T getFirst() {
    return first;
  }

  T getLast() {
    return last;
  }

  Pair<Long, Double> getCenter() {
    return center;
  }

  <U> List<U> map(Function<T, U> mapper) {
    return data.stream().map(mapper).collect(toList());
  }

  static Pair<Long, Double> centerBetween(Pair<Long, Double> a, Pair<Long, Double> b) {
    Pair<Long, Double> vector = Pair.of(b.getLeft() - a.getLeft(), b.getRight() - a.getRight());
    Pair<Long, Double> halfVector = Pair.of(vector.getLeft() / 2, vector.getRight() / 2);
    return Pair.of(a.getLeft() + halfVector.getLeft(), a.getRight() + halfVector.getRight());
  }
}
