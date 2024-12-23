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

import java.util.ArrayList;
import java.util.List;

/**
 * this class is copied and modified from <a
 * href="https://github.com/ggalmazor/lt_downsampling_java8">...</a> project
 */
public final class LTThreeBuckets {

  public static List<Pair<Long, Double>> sorted(
      List<Pair<Long, Double>> input, int desiredBuckets) {
    return sorted(input, input.size(), desiredBuckets);
  }

  public static List<Pair<Long, Double>> sorted(
      List<Pair<Long, Double>> input, int inputSize, int desiredBuckets) {
    List<Pair<Long, Double>> results = new ArrayList<>();

    OnePassBucketizer.bucketize(input, inputSize, desiredBuckets).stream()
        .collect(new SlidingCollector<>(3, 1))
        .stream()
        .map(Triangle::of)
        .forEach(
            triangle -> {
              if (results.isEmpty()) results.add(triangle.getFirst());

              results.add(triangle.getResult());

              if (results.size() == desiredBuckets + 1) results.add(triangle.getLast());
            });

    return results;
  }
}
