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

package org.apache.iotdb.commons.pipe.resource;

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PipeRecentFailureCounter {

  public static final long WINDOW_MILLIS = TimeUnit.MINUTES.toMillis(1);
  private static final long BUCKET_MILLIS = TimeUnit.SECONDS.toMillis(1);
  // Keep one extra slot for a failure exactly WINDOW_MILLIS old.
  private static final int BUCKET_COUNT = (int) (WINDOW_MILLIS / BUCKET_MILLIS) + 1;

  private final Map<PipeResourceFailureType, FailureBucket[]> failureBuckets =
      new EnumMap<>(PipeResourceFailureType.class);

  public PipeRecentFailureCounter() {
    for (final PipeResourceFailureType failureType : PipeResourceFailureType.values()) {
      final FailureBucket[] buckets = new FailureBucket[BUCKET_COUNT];
      for (int i = 0; i < BUCKET_COUNT; ++i) {
        buckets[i] = new FailureBucket();
      }
      failureBuckets.put(failureType, buckets);
    }
  }

  public void record(final PipeResourceFailureType failureType) {
    record(failureType, System.currentTimeMillis());
  }

  @TestOnly
  synchronized void record(final PipeResourceFailureType failureType, final long timestamp) {
    final long bucketStartTime = Math.floorDiv(timestamp, BUCKET_MILLIS) * BUCKET_MILLIS;
    final int bucketIndex = Math.floorMod(Math.floorDiv(timestamp, BUCKET_MILLIS), BUCKET_COUNT);
    final FailureBucket bucket = failureBuckets.get(failureType)[bucketIndex];
    if (bucket.startTime != bucketStartTime) {
      bucket.startTime = bucketStartTime;
      bucket.count = 0;
    }
    ++bucket.count;
  }

  public Map<String, Long> getRecentFailures() {
    return getRecentFailures(System.currentTimeMillis());
  }

  @TestOnly
  synchronized Map<String, Long> getRecentFailures(final long currentTime) {
    final Map<String, Long> result = new LinkedHashMap<>();
    final long earliestIncludedTime = currentTime - WINDOW_MILLIS;
    for (final PipeResourceFailureType failureType : PipeResourceFailureType.values()) {
      long count = 0;
      for (final FailureBucket bucket : failureBuckets.get(failureType)) {
        if (bucket.startTime >= earliestIncludedTime && bucket.startTime <= currentTime) {
          count += bucket.count;
        }
      }
      if (count > 0) {
        result.put(failureType.getDisplayName(), count);
      }
    }
    return Collections.unmodifiableMap(result);
  }

  private static class FailureBucket {

    private long startTime = Long.MIN_VALUE;
    private long count;
  }
}
