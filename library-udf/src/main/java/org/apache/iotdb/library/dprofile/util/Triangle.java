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

import java.util.List;

import static java.util.Comparator.comparing;

/**
 * this class is copied and modified from <a
 * href="https://github.com/ggalmazor/lt_downsampling_java8">...</a> project
 */
class Triangle<T extends Pair<Long, Double>> {
  private final Bucket<T> left, center, right;

  private Triangle(Bucket<T> left, Bucket<T> center, Bucket<T> right) {
    this.left = left;
    this.center = center;
    this.right = right;
  }

  static <U extends Pair<Long, Double>> Triangle<U> of(List<Bucket<U>> buckets) {
    return new Triangle<>(buckets.get(0), buckets.get(1), buckets.get(2));
  }

  T getFirst() {
    return left.getFirst();
  }

  T getLast() {
    return right.getLast();
  }

  T getResult() {
    return center.map(b -> Area.ofTriangle(left.getResult(), b, right.getCenter())).stream()
        .max(comparing(Area::getValue))
        .orElseThrow(() -> new RuntimeException("Can't obtain max area triangle"))
        .getGenerator();
  }
}
