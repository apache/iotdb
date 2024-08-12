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

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.abs;

/**
 * this class is copied and modified from <a
 * href="https://github.com/ggalmazor/lt_downsampling_java8">...</a> project
 */
class Area<T extends Pair<Long, Double>> {
  private final T generator;
  private final double value;

  private Area(T generator, double value) {
    this.generator = generator;
    this.value = value;
  }

  static <U extends Pair<Long, Double>> Area<U> ofTriangle(
      Pair<Long, Double> a, U b, Pair<Long, Double> c) {
    // area of a triangle = |[Ax(By - Cy) + Bx(Cy - Ay) + Cx(Ay - By)] / 2|
    List<Double> addends =
        Arrays.asList(
            a.getLeft() * (b.getRight() - c.getRight()),
            b.getLeft() * (c.getRight() - a.getRight()),
            c.getLeft() * (a.getRight() - b.getRight()));
    double sum = addends.stream().reduce(0d, Double::sum);
    double value = abs(sum / 2);
    return new Area<>(b, value);
  }

  T getGenerator() {
    return generator;
  }

  public double getValue() {
    return value;
  }
}
