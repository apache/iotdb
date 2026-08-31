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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class SchemaRegionTableDevicePerformanceTest extends AbstractSchemaRegionTest {

  private static final String ENABLED_PROPERTY = "iotdb.schema.non.leading.in.perf.enabled";
  private static final String METERS_PROPERTY = "iotdb.schema.non.leading.in.perf.meters";
  private static final String CHILDREN_PROPERTY =
      "iotdb.schema.non.leading.in.perf.children.per.meter";
  private static final String VALUES_PROPERTY = "iotdb.schema.non.leading.in.perf.values";
  private static final String HIT_VALUES_PROPERTY = "iotdb.schema.non.leading.in.perf.hit.values";
  private static final String BATCH_SIZE_PROPERTY = "iotdb.schema.non.leading.in.perf.batch.size";
  private static final String WARMUPS_PROPERTY = "iotdb.schema.non.leading.in.perf.warmups";
  private static final String ITERATIONS_PROPERTY = "iotdb.schema.non.leading.in.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.schema.non.leading.in.perf.rounds";

  private static volatile long benchmarkBlackhole;

  public SchemaRegionTableDevicePerformanceTest(final SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  public void benchmarkNonLeadingTagInTraversal() throws Exception {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true; tune the other iotdb.schema.non.leading.in.perf.* properties as needed.",
            ENABLED_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "The table-device schema reader is available only in MemoryMode.",
        testParams.getTestModeName().equals("MemoryMode"));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int meterCount = Integer.getInteger(METERS_PROPERTY, 20_000);
    final int childrenPerMeter = Integer.getInteger(CHILDREN_PROPERTY, 8);
    final List<Integer> valueCounts = getPositiveIntValues(VALUES_PROPERTY, "6");
    final int hitValueCount = Integer.getInteger(HIT_VALUES_PROPERTY, 0);
    final int batchSize = Integer.getInteger(BATCH_SIZE_PROPERTY, 1_000);
    final int warmups = Integer.getInteger(WARMUPS_PROPERTY, 2);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 1);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(meterCount > 0);
    Assert.assertTrue(childrenPerMeter > 0);
    Assert.assertTrue(hitValueCount >= 0);
    Assert.assertTrue(hitValueCount <= childrenPerMeter);
    Assert.assertTrue(batchSize > 0);
    Assert.assertTrue(warmups >= 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    final ISchemaRegion schemaRegion = getSchemaRegion("db", 0);
    final String tableName = "non_leading_in_perf";
    createBenchmarkDevices(schemaRegion, tableName, meterCount, childrenPerMeter, batchSize);

    for (final int valueCount : valueCounts) {
      Assert.assertTrue(hitValueCount <= valueCount);
      runBenchmarkScenario(
          schemaRegion,
          tableName,
          meterCount,
          childrenPerMeter,
          valueCount,
          hitValueCount,
          warmups,
          iterations,
          rounds);
    }
  }

  private static void runBenchmarkScenario(
      final ISchemaRegion schemaRegion,
      final String tableName,
      final int meterCount,
      final int childrenPerMeter,
      final int valueCount,
      final int hitValueCount,
      final int warmups,
      final int iterations,
      final int rounds) {
    final List<String> cardValues = new ArrayList<>(valueCount);
    for (int i = 0; i < valueCount; ++i) {
      cardValues.add(i < hitValueCount ? "card_" + i : "missing_card_" + i);
    }

    final List<PartialPath> legacyPatterns =
        createLegacyExpandedPatterns(schemaRegion, tableName, cardValues);
    final List<PartialPath> optimizedPatterns =
        createOptimizedPatterns(schemaRegion, tableName, cardValues);
    Assert.assertEquals(1, optimizedPatterns.size());

    final long legacyMatches = scanPatterns(schemaRegion, legacyPatterns);
    final long optimizedMatches = scanPatterns(schemaRegion, optimizedPatterns);
    Assert.assertEquals(legacyMatches, optimizedMatches);

    for (int i = 0; i < warmups; ++i) {
      if ((i & 1) == 0) {
        benchmarkBlackhole = scanPatterns(schemaRegion, legacyPatterns);
        benchmarkBlackhole = scanPatterns(schemaRegion, optimizedPatterns);
      } else {
        benchmarkBlackhole = scanPatterns(schemaRegion, optimizedPatterns);
        benchmarkBlackhole = scanPatterns(schemaRegion, legacyPatterns);
      }
    }

    final Measurement[] legacyMeasurements = new Measurement[rounds];
    final Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; ++i) {
      if ((i & 1) == 0) {
        legacyMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations, () -> benchmarkBlackhole = scanPatterns(schemaRegion, legacyPatterns));
        optimizedMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations,
                () -> benchmarkBlackhole = scanPatterns(schemaRegion, optimizedPatterns));
      } else {
        optimizedMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations,
                () -> benchmarkBlackhole = scanPatterns(schemaRegion, optimizedPatterns));
        legacyMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations, () -> benchmarkBlackhole = scanPatterns(schemaRegion, legacyPatterns));
      }
    }

    printTraversalBenchmark(
        meterCount,
        childrenPerMeter,
        valueCount,
        hitValueCount,
        legacyMatches,
        warmups,
        iterations,
        rounds,
        ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations),
        ManualPerformanceTestUtils.summarize(optimizedMeasurements, iterations));
  }

  private static List<Integer> getPositiveIntValues(
      final String propertyName, final String defaultValue) {
    final String[] rawValues = System.getProperty(propertyName, defaultValue).split(",");
    final List<Integer> values = new ArrayList<>(rawValues.length);
    for (final String rawValue : rawValues) {
      final int value = Integer.parseInt(rawValue.trim());
      Assert.assertTrue(value > 0);
      values.add(value);
    }
    return values;
  }

  private static void createBenchmarkDevices(
      final ISchemaRegion schemaRegion,
      final String tableName,
      final int meterCount,
      final int childrenPerMeter,
      final int batchSize)
      throws Exception {
    final List<Object[]> deviceIds = new ArrayList<>(batchSize);
    for (int meter = 0; meter < meterCount; ++meter) {
      for (int child = 0; child < childrenPerMeter; ++child) {
        deviceIds.add(new Object[] {"meter_" + meter, "card_" + child});
        if (deviceIds.size() == batchSize) {
          createBenchmarkDeviceBatch(schemaRegion, tableName, deviceIds);
          deviceIds.clear();
        }
      }
    }
    if (!deviceIds.isEmpty()) {
      createBenchmarkDeviceBatch(schemaRegion, tableName, deviceIds);
    }
  }

  private static void createBenchmarkDeviceBatch(
      final ISchemaRegion schemaRegion, final String tableName, final List<Object[]> deviceIds)
      throws Exception {
    schemaRegion.createOrUpdateTableDevice(
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId("non-leading-in-performance"),
            null,
            tableName,
            new ArrayList<>(deviceIds),
            Collections.emptyList(),
            Collections.nCopies(deviceIds.size(), new Object[0])));
  }

  private static List<PartialPath> createLegacyExpandedPatterns(
      final ISchemaRegion schemaRegion, final String tableName, final List<String> cardValues) {
    final List<PartialPath> patterns = new ArrayList<>(cardValues.size());
    for (final String cardValue : cardValues) {
      patterns.add(
          new ExtendedPartialPath(
              new String[] {
                PATH_ROOT, schemaRegion.getDatabaseFullPath(), tableName, "*", cardValue
              },
              false));
    }
    return patterns;
  }

  private static List<PartialPath> createOptimizedPatterns(
      final ISchemaRegion schemaRegion, final String tableName, final List<String> cardValues) {
    final List<List<SchemaFilter>> filterBranches = new ArrayList<>(cardValues.size());
    for (final String cardValue : cardValues) {
      filterBranches.add(Collections.singletonList(new TagFilter(new PreciseFilter(cardValue), 1)));
    }
    return DeviceFilterUtil.convertToDevicePattern(
        new String[] {PATH_ROOT, schemaRegion.getDatabaseFullPath(), tableName},
        2,
        filterBranches,
        false);
  }

  private static long scanPatterns(
      final ISchemaRegion schemaRegion, final List<PartialPath> patterns) {
    long count = 0;
    for (final PartialPath pattern : patterns) {
      try (final ISchemaReader<IDeviceSchemaInfo> reader =
          schemaRegion.getTableDeviceReader(pattern)) {
        while (reader.hasNext()) {
          reader.next();
          ++count;
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return count;
  }

  private static void printTraversalBenchmark(
      final int meterCount,
      final int childrenPerMeter,
      final int valueCount,
      final int hitValueCount,
      final long matchCount,
      final int warmups,
      final int iterations,
      final int rounds,
      final Summary legacy,
      final Summary optimized) {
    System.out.printf(
        Locale.ROOT,
        "Non-leading TAG IN traversal benchmark: meters=%d, children/meter=%d, devices=%d, IN values=%d, hit values=%d, matches=%d, warmups=%d, iterations/round=%d, rounds=%d%n",
        meterCount,
        childrenPerMeter,
        (long) meterCount * childrenPerMeter,
        valueCount,
        hitValueCount,
        matchCount,
        warmups,
        iterations,
        rounds);
    System.out.printf(
        Locale.ROOT,
        "  optimized strategy: %s (IN values=%d, child keys=%d)%n",
        valueCount < childrenPerMeter ? "precise IN lookups" : "child-key iteration",
        valueCount,
        childrenPerMeter);
    printTraversalSummary("legacy-expanded", legacy);
    printTraversalSummary("optimized", optimized);
    System.out.printf(
        Locale.ROOT,
        "  change: CPU speedup=%.2fx, allocation reduction=%.1f%%, peak-heap reduction=%.1f%%%n",
        ratio(legacy.getCpuNanosPerOperation(), optimized.getCpuNanosPerOperation()),
        reduction(
            legacy.getAllocatedBytesPerOperation(), optimized.getAllocatedBytesPerOperation()),
        reduction(legacy.getPeakHeapDeltaBytes(), optimized.getPeakHeapDeltaBytes()));
  }

  private static void printTraversalSummary(final String label, final Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-15s CPU=%.3f ms/op, allocated=%.1f bytes/op, peak heap delta=%.3f MiB%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000_000.0,
        summary.getAllocatedBytesPerOperation(),
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
  }

  private static double ratio(final double baseline, final double optimized) {
    return optimized == 0 ? Double.POSITIVE_INFINITY : baseline / optimized;
  }

  private static double reduction(final double baseline, final double optimized) {
    return baseline == 0 ? 0 : (baseline - optimized) * 100.0 / baseline;
  }
}
