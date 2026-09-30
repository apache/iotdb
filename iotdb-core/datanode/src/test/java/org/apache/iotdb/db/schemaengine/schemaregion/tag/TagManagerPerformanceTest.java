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

package org.apache.iotdb.db.schemaengine.schemaregion.tag;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.impl.TagFilter;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TagManagerPerformanceTest {
  private static final String PREFIX = "iotdb.tag.index.perf.";
  private static volatile List<IMeasurementMNode<?>> benchmarkBlackhole;

  @Test
  public void compareExactLookupAndPathFiltering() throws Exception {
    Assume.assumeTrue(
        "Manual performance UT: enable with -D" + PREFIX + "enabled=true",
        Boolean.getBoolean(PREFIX + "enabled"));
    Assume.assumeTrue(
        "Current-thread CPU and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());
    final int distinctValues = Integer.getInteger(PREFIX + "distinctValues", 10_000);
    final int candidateNodes = Integer.getInteger(PREFIX + "candidateNodes", 2_000);
    final int matchingNodes = Integer.getInteger(PREFIX + "matchingNodes", 100);
    final int warmups = Integer.getInteger(PREFIX + "warmups", 30);
    final int exactIterations = Integer.getInteger(PREFIX + "exactIterations", 50_000);
    final int pathIterations = Integer.getInteger(PREFIX + "pathIterations", 1_000);
    final int rounds = Integer.getInteger(PREFIX + "rounds", 5);
    Assert.assertTrue(
        distinctValues > 0
            && candidateNodes > 0
            && matchingNodes > 0
            && matchingNodes <= candidateNodes);
    Assert.assertTrue(warmups >= 0 && exactIterations > 0 && pathIterations > 0 && rounds > 0);

    final File directory = Files.createTempDirectory("tag-index-performance").toFile();
    try {
      final TagManager manager = new TagManager(directory.getAbsolutePath(), null);
      try {
        final IMNodeFactory<IMemMNode> factory =
            MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();
        final IMemMNode root = factory.createInternalMNode(null, "root");
        final IDeviceMNode<IMemMNode> matchingDevice =
            factory.createDeviceMNode(factory.createInternalMNode(root, "sg"), "d");
        final IDeviceMNode<IMemMNode> otherDevice =
            factory.createDeviceMNode(factory.createInternalMNode(root, "other"), "d");
        final IMeasurementMNode<IMemMNode> shared =
            newMeasurement(factory, matchingDevice, "single");
        for (int i = 0; i < distinctValues; i++) {
          manager.addIndex("cardinality", "v" + i, shared);
        }
        for (int i = 0; i < candidateNodes; i++) {
          manager.addIndex(
              "selectivity",
              "target",
              newMeasurement(factory, i < matchingNodes ? matchingDevice : otherDevice, "s" + i));
        }
        final Map<String, Map<String, Set<IMeasurementMNode<?>>>> index = getIndex(manager);
        final PartialPath pattern = new PartialPath("root.sg.**");
        System.out.printf(
            Locale.ROOT,
            "Tag index benchmark: distinctValues=%d, candidateNodes=%d, matchingNodes=%d, warmups=%d, exactIterations/round=%d, pathIterations/round=%d, rounds=%d%n",
            distinctValues,
            candidateNodes,
            matchingNodes,
            warmups,
            exactIterations,
            pathIterations,
            rounds);
        benchmark(
            "exact value lookup",
            manager,
            index,
            new TagFilter("cardinality", "v0", false),
            pattern,
            1,
            warmups,
            exactIterations,
            rounds);
        benchmark(
            "path filtering before sort",
            manager,
            index,
            new TagFilter("selectivity", "target", false),
            pattern,
            matchingNodes,
            warmups,
            pathIterations,
            rounds);
      } finally {
        manager.clear();
      }
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  private static IMeasurementMNode<IMemMNode> newMeasurement(
      IMNodeFactory<IMemMNode> factory, IDeviceMNode<IMemMNode> parent, String name) {
    return factory.createMeasurementMNode(
        parent,
        name,
        new MeasurementSchema(name, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
        null);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Set<IMeasurementMNode<?>>>> getIndex(TagManager manager)
      throws Exception {
    final Field field = TagManager.class.getDeclaredField("tagIndex");
    field.setAccessible(true);
    return (Map<String, Map<String, Set<IMeasurementMNode<?>>>>) field.get(manager);
  }

  // Reproduce the old value scan and sort, then apply the reader's path filter.
  private static List<IMeasurementMNode<?>> legacyLookup(
      Map<String, Map<String, Set<IMeasurementMNode<?>>>> index,
      TagFilter filter,
      PartialPath pattern) {
    final Map<String, Set<IMeasurementMNode<?>>> values = index.get(filter.getKey());
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    final List<IMeasurementMNode<?>> candidates = new ArrayList<>();
    for (Map.Entry<String, Set<IMeasurementMNode<?>>> entry : values.entrySet()) {
      if (entry.getKey() != null
          && entry.getValue() != null
          && filter.getValue().equals(entry.getKey())) {
        candidates.addAll(entry.getValue());
      }
    }
    final List<IMeasurementMNode<?>> sorted =
        candidates.stream()
            .sorted(Comparator.comparing(IMNode::getFullPath))
            .collect(Collectors.toList());
    sorted.removeIf(node -> !pattern.matchFullPath(node.getPartialPath()));
    return sorted;
  }

  private static void benchmark(
      String label,
      TagManager manager,
      Map<String, Map<String, Set<IMeasurementMNode<?>>>> index,
      TagFilter filter,
      PartialPath pattern,
      int expectedMatches,
      int warmups,
      int iterations,
      int rounds) {
    final List<IMeasurementMNode<?>> legacyResult = legacyLookup(index, filter, pattern);
    final List<IMeasurementMNode<?>> optimizedResult =
        manager.getMatchedTimeseriesInIndex(filter, pattern, false);
    Assert.assertEquals(expectedMatches, optimizedResult.size());
    Assert.assertEquals(legacyResult, optimizedResult);
    final Runnable legacy = () -> benchmarkBlackhole = legacyLookup(index, filter, pattern);
    final Runnable optimized =
        () -> benchmarkBlackhole = manager.getMatchedTimeseriesInIndex(filter, pattern, false);
    for (int i = 0; i < warmups; i++) {
      if ((i & 1) == 0) {
        legacy.run();
        optimized.run();
      } else {
        optimized.run();
        legacy.run();
      }
    }
    final Measurement[] legacyMeasurements = new Measurement[rounds];
    final Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; i++) {
      if ((i & 1) == 0) {
        legacyMeasurements[i] = ManualPerformanceTestUtils.measure(iterations, legacy);
        optimizedMeasurements[i] = ManualPerformanceTestUtils.measure(iterations, optimized);
      } else {
        optimizedMeasurements[i] = ManualPerformanceTestUtils.measure(iterations, optimized);
        legacyMeasurements[i] = ManualPerformanceTestUtils.measure(iterations, legacy);
      }
    }
    final Summary oldSummary = ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations);
    final Summary newSummary =
        ManualPerformanceTestUtils.summarize(optimizedMeasurements, iterations);
    System.out.printf(Locale.ROOT, "  %s (matches=%d):%n", label, expectedMatches);
    printSummary("legacy", oldSummary);
    printSummary("optimized", newSummary);
    final double allocationReduction =
        oldSummary.getAllocatedBytesPerOperation() == 0
            ? 0
            : (oldSummary.getAllocatedBytesPerOperation()
                    - newSummary.getAllocatedBytesPerOperation())
                * 100.0
                / oldSummary.getAllocatedBytesPerOperation();
    if (oldSummary.getCpuNanosPerOperation() > 0 && newSummary.getCpuNanosPerOperation() > 0) {
      System.out.printf(
          Locale.ROOT,
          "    CPU speedup=%.2fx, allocation reduction=%.1f%%%n",
          oldSummary.getCpuNanosPerOperation() / newSummary.getCpuNanosPerOperation(),
          allocationReduction);
    } else {
      System.out.printf(
          Locale.ROOT,
          "    CPU speedup=n/a (timer resolution), allocation reduction=%.1f%%%n",
          allocationReduction);
    }
  }

  private static void printSummary(String label, Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "    %-9s CPU=%.3f us/query, allocated=%.1f B/query%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000.0,
        summary.getAllocatedBytesPerOperation());
  }
}
