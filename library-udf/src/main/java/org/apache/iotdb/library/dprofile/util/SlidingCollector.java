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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.lang.Integer.max;
import static java.util.stream.Collectors.toList;

/**
 * this class is copied and modified from <a
 * href="https://github.com/ggalmazor/lt_downsampling_java8">...</a> project
 */
public class SlidingCollector<T> implements Collector<T, List<List<T>>, List<List<T>>> {

  private final int size;
  private final int step;
  private final int window;
  private final Queue<T> buffer = new ArrayDeque<>();
  private int totalIn = 0;

  public SlidingCollector(int size, int step) {
    this.size = size;
    this.step = step;
    this.window = max(size, step);
  }

  @Override
  public Supplier<List<List<T>>> supplier() {
    return ArrayList::new;
  }

  @Override
  public BiConsumer<List<List<T>>, T> accumulator() {
    return (lists, t) -> {
      buffer.offer(t);
      ++totalIn;
      if (buffer.size() == window) {
        dumpCurrent(lists);
        shiftBy(step);
      }
    };
  }

  @Override
  public Function<List<List<T>>, List<List<T>>> finisher() {
    return lists -> {
      if (!buffer.isEmpty()) {
        final int totalOut = estimateTotalOut();
        if (totalOut > lists.size()) {
          dumpCurrent(lists);
        }
      }
      return lists;
    };
  }

  private int estimateTotalOut() {
    return max(0, (totalIn + step - size - 1) / step) + 1;
  }

  private void dumpCurrent(List<List<T>> lists) {
    final List<T> batch = buffer.stream().limit(size).collect(toList());
    lists.add(batch);
  }

  private void shiftBy(int by) {
    for (int i = 0; i < by; i++) {
      buffer.remove();
    }
  }

  @Override
  public BinaryOperator<List<List<T>>> combiner() {
    return (l1, l2) -> {
      throw new UnsupportedOperationException("Combining not possible");
    };
  }

  @Override
  public Set<Characteristics> characteristics() {
    return EnumSet.noneOf(Characteristics.class);
  }
}
