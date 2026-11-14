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

package org.apache.iotdb.consensus.common.research;

import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Observational counters only. No counter is used to make a protocol decision. */
public final class ResearchMetrics implements ResearchMetricsMBean {
  public static final String OBJECT_NAME = "org.apache.iotdb.research:type=ConsensusMetrics";
  private static final ConcurrentHashMap<String, LongAdder> COUNTERS = new ConcurrentHashMap<>();
  // Per latency key: the counters latency() updates, so a record builds no key strings.
  private static final ConcurrentHashMap<String, Latency> LATENCIES = new ConcurrentHashMap<>();

  private static final class Latency {
    final String key;
    final LongAdder count;
    final LongAdder sum;
    // Created on first use, so a snapshot lists only the buckets that were hit, as before.
    final LongAdder[] buckets = new LongAdder[65];

    Latency(String key) {
      this.key = key;
      count = counter(key + ".count");
      sum = counter(key + ".sum_nanos");
    }

    LongAdder bucket(int bucket) {
      LongAdder adder = buckets[bucket];
      if (adder == null) {
        // A racing thread finds the same adder in COUNTERS.
        adder = counter(key + ".bucket_" + bucket);
        buckets[bucket] = adder;
      }
      return adder;
    }
  }

  static {
    try {
      ManagementFactory.getPlatformMBeanServer()
          .registerMBean(new ResearchMetrics(), new ObjectName(OBJECT_NAME));
    } catch (Exception ignored) {
      // Restricted JMX must never stop consensus. The benchmark treats absence as unavailable.
    }
  }

  private ResearchMetrics() {}

  public static void add(String key, long value) {
    counter(key).add(value);
  }

  /** The counter behind {@code key}; callers on hot paths may keep it instead of the key. */
  public static LongAdder counter(String key) {
    LongAdder counter = COUNTERS.get(key);
    return counter != null ? counter : COUNTERS.computeIfAbsent(key, ignored -> new LongAdder());
  }

  public static long value(String key) {
    LongAdder counter = COUNTERS.get(key);
    return counter == null ? 0 : counter.sum();
  }

  /** Logarithmic latency buckets: bounds are powers of two nanoseconds, not exact quantiles. */
  public static void latency(String key, long nanos) {
    Latency latency = LATENCIES.get(key);
    if (latency == null) {
      latency = LATENCIES.computeIfAbsent(key, Latency::new);
    }
    record(latency, nanos);
  }

  private static void record(Latency latency, long nanos) {
    long value = Math.max(0, nanos);
    latency.count.increment();
    latency.sum.add(value);
    latency.bucket(value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value)).increment();
  }

  /** A held handle on latency(key, ...) for hot paths: records without looking the key up. */
  public static final class LatencyRecorder {
    private final Latency latency;

    private LatencyRecorder(Latency latency) {
      this.latency = latency;
    }

    public void record(long nanos) {
      ResearchMetrics.record(latency, nanos);
    }
  }

  public static LatencyRecorder latencyRecorder(String key) {
    return new LatencyRecorder(LATENCIES.computeIfAbsent(key, Latency::new));
  }

  @Override
  public String getSnapshot() {
    StringBuilder result = new StringBuilder();
    for (Map.Entry<String, LongAdder> entry : new TreeMap<>(COUNTERS).entrySet()) {
      result.append(entry.getKey()).append('=').append(entry.getValue().sum()).append('\n');
    }
    return result.toString();
  }

  /** Called in the actual serialized leader admission order, separately for every group. */
  public static final class AdmissionTracker {
    private final String backend;
    private boolean hasTimestamp;
    private long maximum;

    public AdmissionTracker(String backend) {
      this.backend = backend;
      add(backend + ".admitted_entries", 0);
      add(backend + ".admitted_data", 0);
      add(backend + ".admitted_disordered", 0);
      add(backend + ".admitted_barriers", 0);
    }

    public void accepted(boolean barrier, long timestamp) {
      add(backend + ".admitted_entries", 1);
      if (barrier) {
        add(backend + ".admitted_barriers", 1);
        return;
      }
      add(backend + ".admitted_data", 1);
      if (hasTimestamp && timestamp <= maximum) {
        add(backend + ".admitted_disordered", 1);
      }
      maximum = hasTimestamp ? Math.max(maximum, timestamp) : timestamp;
      hasTimestamp = true;
    }
  }
}
