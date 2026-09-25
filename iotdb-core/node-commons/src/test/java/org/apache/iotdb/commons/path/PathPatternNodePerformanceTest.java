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

package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.path.PathPatternNode.VoidSerializer;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class PathPatternNodePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.path.pattern.node.perf.enabled";
  private static final String WILDCARD_COUNTS_PROPERTY =
      "iotdb.path.pattern.node.perf.wildcard-counts";
  private static final String QUERY_COUNT_PROPERTY = "iotdb.path.pattern.node.perf.query-count";
  private static final String WARMUP_LOOKUPS_PROPERTY =
      "iotdb.path.pattern.node.perf.warmup-lookups";
  private static final String LOOKUPS_PROPERTY = "iotdb.path.pattern.node.perf.lookups";
  private static final String ROUNDS_PROPERTY = "iotdb.path.pattern.node.perf.rounds";

  private static final int[] DEFAULT_WILDCARD_COUNTS = {1, 10, 100};

  private static volatile long benchmarkBlackhole;

  /**
   * Compares the old compile-on-every-match path with the compiled-pattern cache used by {@link
   * PathPatternNode#getMatchChildren(String)}. This test is disabled by default because it is a
   * manual microbenchmark rather than a correctness or regression threshold.
   */
  @Test
  public void benchmarkCompiledNonTrivialWildcardCache() {
    assumePerformanceTestEnabled();

    final int queryCount = Integer.getInteger(QUERY_COUNT_PROPERTY, 1_000);
    final int warmupLookups = Integer.getInteger(WARMUP_LOOKUPS_PROPERTY, 2_000);
    final int lookups = Integer.getInteger(LOOKUPS_PROPERTY, 10_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(queryCount > 0);
    Assert.assertTrue(warmupLookups > 0);
    Assert.assertTrue(lookups > 0);
    Assert.assertTrue(rounds > 0);

    for (final int wildcardCount : parseWildcardCounts()) {
      Assert.assertTrue(wildcardCount > 0);
      runScenario(wildcardCount, queryCount, warmupLookups, lookups, rounds);
    }
  }

  private static void runScenario(
      final int wildcardCount,
      final int queryCount,
      final int warmupLookups,
      final int lookups,
      final int rounds) {
    final PathPatternNode<Void, VoidSerializer> parent = newNode("parent");
    final Set<String> wildcardNames = new HashSet<>();
    for (int i = 0; i < wildcardCount; ++i) {
      final String wildcardName = "device_" + i + "_*";
      wildcardNames.add(wildcardName);
      parent.addChild(newNode(wildcardName));
    }
    final String[] queries = createQueries(wildcardCount, queryCount);

    final BenchmarkResult legacyWarmup =
        runLegacyMatches(parent, wildcardNames, queries, warmupLookups);
    final BenchmarkResult cachedWarmup = runCachedMatches(parent, queries, warmupLookups);
    Assert.assertEquals(legacyWarmup.matchCount, cachedWarmup.matchCount);

    final long[] legacyElapsedNanos = new long[rounds];
    final long[] cachedElapsedNanos = new long[rounds];
    for (int round = 0; round < rounds; ++round) {
      final BenchmarkResult legacyResult;
      final BenchmarkResult cachedResult;
      if ((round & 1) == 0) {
        legacyResult = runLegacyMatches(parent, wildcardNames, queries, lookups);
        cachedResult = runCachedMatches(parent, queries, lookups);
      } else {
        cachedResult = runCachedMatches(parent, queries, lookups);
        legacyResult = runLegacyMatches(parent, wildcardNames, queries, lookups);
      }
      Assert.assertEquals(legacyResult.matchCount, cachedResult.matchCount);
      legacyElapsedNanos[round] = legacyResult.elapsedNanos;
      cachedElapsedNanos[round] = cachedResult.elapsedNanos;
    }

    final double legacyNanosPerLookup = median(legacyElapsedNanos) / lookups;
    final double cachedNanosPerLookup = median(cachedElapsedNanos) / lookups;
    System.out.printf(
        Locale.ROOT,
        "PathPatternNode wildcard benchmark: wildcard-children=%d, distinct-queries=%d, "
            + "warmup-lookups=%d, lookups/round=%d, rounds=%d%n",
        wildcardCount,
        queryCount,
        warmupLookups,
        lookups,
        rounds);
    printResult("compile on every match", legacyNanosPerLookup);
    printResult("compiled-pattern cache", cachedNanosPerLookup);
    System.out.printf(
        Locale.ROOT,
        "  speedup=%.2fx, latency reduction=%.2f%%%n",
        legacyNanosPerLookup / cachedNanosPerLookup,
        (legacyNanosPerLookup - cachedNanosPerLookup) * 100.0 / legacyNanosPerLookup);
  }

  private static BenchmarkResult runLegacyMatches(
      final PathPatternNode<Void, VoidSerializer> parent,
      final Set<String> wildcardNames,
      final String[] queries,
      final int lookups) {
    long matchCount = 0;
    final long startNanos = System.nanoTime();
    for (int i = 0; i < lookups; ++i) {
      matchCount +=
          getMatchChildrenWithoutCompiledPatternCache(
                  parent, wildcardNames, queries[i % queries.length])
              .size();
    }
    final long elapsedNanos = System.nanoTime() - startNanos;
    benchmarkBlackhole = matchCount;
    return new BenchmarkResult(elapsedNanos, matchCount);
  }

  private static BenchmarkResult runCachedMatches(
      final PathPatternNode<Void, VoidSerializer> parent,
      final String[] queries,
      final int lookups) {
    long matchCount = 0;
    final long startNanos = System.nanoTime();
    for (int i = 0; i < lookups; ++i) {
      matchCount += parent.getMatchChildren(queries[i % queries.length]).size();
    }
    final long elapsedNanos = System.nanoTime() - startNanos;
    benchmarkBlackhole = matchCount;
    return new BenchmarkResult(elapsedNanos, matchCount);
  }

  /** Reproduces PathPatternNode.getMatchChildren before the compiled-pattern cache was added. */
  private static List<PathPatternNode<Void, VoidSerializer>>
      getMatchChildrenWithoutCompiledPatternCache(
          final PathPatternNode<Void, VoidSerializer> parent,
          final Set<String> wildcardNames,
          final String nodeName) {
    final Map<String, PathPatternNode<Void, VoidSerializer>> children = parent.getChildren();
    final List<PathPatternNode<Void, VoidSerializer>> result = new ArrayList<>();
    if (children.containsKey(nodeName)) {
      result.add(children.get(nodeName));
    }
    if (children.containsKey(ONE_LEVEL_PATH_WILDCARD)) {
      result.add(children.get(ONE_LEVEL_PATH_WILDCARD));
    }
    if (children.containsKey(MULTI_LEVEL_PATH_WILDCARD)) {
      result.add(children.get(MULTI_LEVEL_PATH_WILDCARD));
    }
    wildcardNames.stream()
        .filter(path -> PathPatternUtil.isNodeMatch(path, nodeName))
        .map(children::get)
        .forEach(result::add);
    return result;
  }

  private static String[] createQueries(final int wildcardCount, final int queryCount) {
    final String[] queries = new String[queryCount];
    for (int i = 0; i < queryCount; ++i) {
      queries[i] = i % 3 == 0 ? "unmatched_" + i : "device_" + (i % wildcardCount) + "_sensor_" + i;
    }
    return queries;
  }

  private static int[] parseWildcardCounts() {
    final String configured = System.getProperty(WILDCARD_COUNTS_PROPERTY);
    if (configured == null || configured.trim().isEmpty()) {
      return DEFAULT_WILDCARD_COUNTS;
    }
    return Arrays.stream(configured.split(","))
        .map(String::trim)
        .mapToInt(Integer::parseInt)
        .toArray();
  }

  private static void assumePerformanceTestEnabled() {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true; optionally tune -D%s, -D%s, "
                + "-D%s, -D%s, and -D%s.",
            ENABLED_PROPERTY,
            WILDCARD_COUNTS_PROPERTY,
            QUERY_COUNT_PROPERTY,
            WARMUP_LOOKUPS_PROPERTY,
            LOOKUPS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
  }

  private static double median(final long[] values) {
    Arrays.sort(values);
    final int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0;
  }

  private static void printResult(final String label, final double nanosPerLookup) {
    System.out.printf(
        Locale.ROOT,
        "  %-24s latency=%10.1f ns/lookup, throughput=%10.0f lookups/s%n",
        label,
        nanosPerLookup,
        1_000_000_000.0 / nanosPerLookup);
  }

  private static PathPatternNode<Void, VoidSerializer> newNode(final String name) {
    return new PathPatternNode<>(name, VoidSerializer.getInstance());
  }

  private static final class BenchmarkResult {

    private final long elapsedNanos;
    private final long matchCount;

    private BenchmarkResult(final long elapsedNanos, final long matchCount) {
      this.elapsedNanos = elapsedNanos;
      this.matchCount = matchCount;
    }
  }
}
