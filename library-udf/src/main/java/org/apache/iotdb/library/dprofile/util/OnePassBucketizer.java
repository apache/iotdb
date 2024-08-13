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
class OnePassBucketizer {

  static List<Bucket<Pair<Long, Double>>> bucketize(
      List<Pair<Long, Double>> input, int inputSize, int desiredBuckets) {
    int middleSize = inputSize - 2;
    int bucketSize = middleSize / desiredBuckets;
    int remainingElements = middleSize % desiredBuckets;

    if (bucketSize == 0)
      throw new IllegalArgumentException(
          "Can't produce "
              + desiredBuckets
              + " buckets from an input series of "
              + (middleSize + 2)
              + " elements");

    List<Bucket<Pair<Long, Double>>> buckets = new ArrayList<>();

    // Add first point as the only point in the first bucket
    buckets.add(Bucket.of(input.get(0)));

    List<Pair<Long, Double>> rest = input.subList(1, input.size() - 1);

    // Add middle buckets.
    // When inputSize is not a multiple of desiredBuckets, remaining elements are equally
    // distributed on the first buckets.
    while (buckets.size() < desiredBuckets + 1) {
      int size = buckets.size() <= remainingElements ? bucketSize + 1 : bucketSize;
      buckets.add(Bucket.of(rest.subList(0, size)));
      rest = rest.subList(size, rest.size());
    }

    // Add last point as the only point in the last bucket
    buckets.add(Bucket.of(input.get(input.size() - 1)));

    return buckets;
  }
}
