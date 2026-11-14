/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.traft.paper.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A short fixed range of the crash and fault simulation, a few seconds per run. Use the properties
 * traft.fuzz.from and traft.fuzz.seeds for more, and {@link TRaftFuzz} to replay or minimize a
 * seed.
 */
public class TRaftFuzzTest {
  private static TRaftFuzz.Stats runRange(
      long from, long seeds, Consumer<TRaftFuzz.Config> change, Map<String, List<Long>> failures) {
    TRaftFuzz.Stats total = new TRaftFuzz.Stats();
    for (long seed = from; seed < from + seeds; seed++) {
      TRaftFuzz.Result result = TRaftFuzz.runSeed(seed, false, change);
      total.add(result.stats);
      if (result.category != null) {
        List<Long> sample = failures.computeIfAbsent(result.category, ignored -> new ArrayList<>());
        if (sample.size() < 10) {
          sample.add(seed);
        }
      }
    }
    return total;
  }

  @Test
  public void fixedSeedsKeepEveryInvariantAndConverge() {
    long from = Long.getLong("traft.fuzz.from", 0);
    long seeds = Long.getLong("traft.fuzz.seeds", 1000);
    Map<String, List<Long>> failures = new TreeMap<>();
    TRaftFuzz.Stats total = runRange(from, seeds, ignored -> {}, failures);
    assertTrue("Violating seeds; replay one with TRaftFuzz: " + failures, failures.isEmpty());
    assertEquals(seeds, total.converged);
    // The range must keep reaching what it is here for.
    assertTrue(total.winners > seeds);
    assertTrue(total.crashes > 0);
    assertTrue(total.armedEntryCrashes > 0);
    assertTrue(total.tornBatches > 0);
    assertTrue(total.entryOnlyRecords > 0);
    assertTrue(total.entryRunsFalse > 0);
    assertTrue(total.transferRuns > 0);
    assertTrue(total.overtakes > 0);
    // About half of the seeds compact, and every one of them ends bounded.
    assertTrue(total.compactionSeeds > seeds / 4);
    assertTrue(total.compactions > total.compactionSeeds);
    assertEquals(total.compactionSeeds, total.boundedChecks);
  }

  /** A second fixed range in which every seed compacts. */
  @Test
  public void compactingSeedsKeepEveryInvariantAndEndBounded() {
    long seeds = Long.getLong("traft.fuzz.compactionSeeds", 500);
    Map<String, List<Long>> failures = new TreeMap<>();
    TRaftFuzz.Stats total = runRange(100_000, seeds, c -> c.compaction = true, failures);
    assertTrue("Violating seeds; replay one with TRaftFuzz: " + failures, failures.isEmpty());
    assertEquals(seeds, total.converged);
    assertEquals(seeds, total.compactionSeeds);
    assertEquals(seeds, total.boundedChecks);
    assertTrue(total.compactions > 10 * seeds);
    assertTrue(total.crashes > 0);
    assertTrue(total.winners > seeds);
    // Restarts replay COMPACT records and compacted replicas win elections. A restart that lost
    // its newest COMPACT is rare here (stats.compactionRollbacks); TRaftEngineTest covers it.
    assertTrue(total.compactedRestarts > 0);
    assertTrue(total.compactedWinners > 0);
  }

  /**
   * Negative control: compacting without the cluster cap drops data that a lagging member still
   * needs, and the harness must catch it in most seeds.
   */
  @Test
  public void compactingWithoutTheCapIsCaught() {
    long seeds = 50;
    int caught = 0;
    for (long seed = 0; seed < seeds; seed++) {
      TRaftFuzz.Result result =
          TRaftFuzz.runSeed(
              seed,
              false,
              c -> {
                c.compaction = true;
                c.ignoreCap = true;
              });
      if (result.category != null) {
        assertTrue(
            "seed " + seed + " failed another way: " + result.category + " " + result.detail,
            result.category.equals("COMPACTION_UNSAFE") || result.category.startsWith("STALL"));
        caught++;
      }
    }
    assertTrue(
        "the cap-ignoring hook was caught in only " + caught + " seeds", caught >= seeds / 2);
  }
}
