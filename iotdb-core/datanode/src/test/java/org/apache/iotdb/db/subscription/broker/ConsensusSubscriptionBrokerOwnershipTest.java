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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.db.subscription.broker.ConsensusSubscriptionBroker.TopicOwnershipSnapshot;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsensusSubscriptionBrokerOwnershipTest {

  @Test
  public void testEqualNumbersOfConsumersAndRegionsAssignEveryConsumer() {
    final List<String> consumers = consumerIds(40);
    final List<String> regions = regionIds(3, 42);

    final TopicOwnershipSnapshot snapshot = TopicOwnershipSnapshot.create(consumers, regions, null);
    final Map<String, Integer> loads = loads(snapshot, consumers, regions);

    assertEquals(40, loads.size());
    assertTrue(loads.values().stream().allMatch(load -> load == 1));
  }

  @Test
  public void testJoiningConsumerTriggersOnlyRequiredMoves() {
    final List<String> regions = regionIds(3, 42);
    final TopicOwnershipSnapshot oneConsumer =
        TopicOwnershipSnapshot.create(Collections.singletonList("consumer_1"), regions, null);
    final TopicOwnershipSnapshot twoConsumers =
        TopicOwnershipSnapshot.create(
            Arrays.asList("consumer_1", "consumer_2"), regions, oneConsumer);

    assertEquals(20, movedRegionCount(oneConsumer, twoConsumers, regions));
    assertBalanced(twoConsumers, Arrays.asList("consumer_1", "consumer_2"), regions);

    final TopicOwnershipSnapshot threeConsumers =
        TopicOwnershipSnapshot.create(
            Arrays.asList("consumer_1", "consumer_2", "consumer_3"), regions, twoConsumers);
    assertEquals(13, movedRegionCount(twoConsumers, threeConsumers, regions));
    assertBalanced(
        threeConsumers, Arrays.asList("consumer_1", "consumer_2", "consumer_3"), regions);
  }

  @Test
  public void testLeavingConsumerOnlyReassignsItsRegions() {
    final List<String> consumers =
        Arrays.asList("consumer_1", "consumer_2", "consumer_3", "consumer_4");
    final List<String> regions = regionIds(3, 42);
    final TopicOwnershipSnapshot before = TopicOwnershipSnapshot.create(consumers, regions, null);
    final List<String> remainingConsumers = Arrays.asList("consumer_1", "consumer_3", "consumer_4");
    final TopicOwnershipSnapshot after =
        TopicOwnershipSnapshot.create(remainingConsumers, regions, before);

    for (final String region : regions) {
      if (!"consumer_2".equals(before.getOwnerConsumerId(region))) {
        assertEquals(before.getOwnerConsumerId(region), after.getOwnerConsumerId(region));
      }
    }
    assertEquals(10, movedRegionCount(before, after, regions));
    assertBalanced(after, remainingConsumers, regions);
  }

  @Test
  public void testRegionChangesPreserveValidOwnership() {
    final List<String> consumers = Arrays.asList("consumer_1", "consumer_2", "consumer_3");
    final List<String> initialRegions = regionIds(3, 11);
    final TopicOwnershipSnapshot initial =
        TopicOwnershipSnapshot.create(consumers, initialRegions, null);

    final List<String> expandedRegions = new ArrayList<>(initialRegions);
    expandedRegions.addAll(regionIds(12, 14));
    final TopicOwnershipSnapshot expanded =
        TopicOwnershipSnapshot.create(consumers, expandedRegions, initial);
    assertEquals(0, movedRegionCount(initial, expanded, initialRegions));
    assertBalanced(expanded, consumers, expandedRegions);

    final List<String> reducedRegions = new ArrayList<>(expandedRegions);
    reducedRegions.remove("DataRegion[12]");
    reducedRegions.remove("DataRegion[13]");
    reducedRegions.remove("DataRegion[14]");
    final TopicOwnershipSnapshot reduced =
        TopicOwnershipSnapshot.create(consumers, reducedRegions, expanded);
    assertEquals(0, movedRegionCount(expanded, reduced, reducedRegions));
    assertBalanced(reduced, consumers, reducedRegions);
  }

  @Test
  public void testMoreConsumersThanRegionsLeavesOnlyUnavoidableConsumersEmpty() {
    final List<String> consumers = consumerIds(5);
    final List<String> regions = regionIds(3, 5);

    final TopicOwnershipSnapshot snapshot = TopicOwnershipSnapshot.create(consumers, regions, null);
    final Map<String, Integer> loads = loads(snapshot, consumers, regions);

    assertEquals(3, loads.values().stream().filter(load -> load == 1).count());
    assertEquals(2, loads.values().stream().filter(load -> load == 0).count());
    assertBalanced(snapshot, consumers, regions);
  }

  @Test
  public void testEmptyInputsProduceEmptyOwnership() {
    final TopicOwnershipSnapshot noConsumers =
        TopicOwnershipSnapshot.create(
            Collections.emptyList(), Collections.singletonList("DataRegion[3]"), null);
    assertNull(noConsumers.getOwnerConsumerId("DataRegion[3]"));

    final TopicOwnershipSnapshot noRegions =
        TopicOwnershipSnapshot.create(
            Collections.singletonList("consumer_1"), Collections.emptyList(), null);
    assertNull(noRegions.getOwnerConsumerId("DataRegion[3]"));
  }

  private static List<String> consumerIds(final int count) {
    return IntStream.rangeClosed(1, count)
        .mapToObj(index -> "consumer_" + index)
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<String> regionIds(final int startInclusive, final int endInclusive) {
    return IntStream.rangeClosed(startInclusive, endInclusive)
        .mapToObj(index -> "DataRegion[" + index + "]")
        .sorted()
        .collect(Collectors.toList());
  }

  private static Map<String, Integer> loads(
      final TopicOwnershipSnapshot snapshot,
      final List<String> consumers,
      final List<String> regions) {
    final Map<String, Integer> result = new HashMap<>();
    consumers.forEach(consumer -> result.put(consumer, 0));
    for (final String region : regions) {
      result.computeIfPresent(snapshot.getOwnerConsumerId(region), (ignored, count) -> count + 1);
    }
    return result;
  }

  private static void assertBalanced(
      final TopicOwnershipSnapshot snapshot,
      final List<String> consumers,
      final List<String> regions) {
    final Map<String, Integer> loads = loads(snapshot, consumers, regions);
    assertFalse(loads.isEmpty());
    final int minimumLoad = Collections.min(loads.values());
    final int maximumLoad = Collections.max(loads.values());
    assertTrue(maximumLoad - minimumLoad <= 1);
    assertEquals(regions.size(), loads.values().stream().mapToInt(Integer::intValue).sum());
  }

  private static int movedRegionCount(
      final TopicOwnershipSnapshot before,
      final TopicOwnershipSnapshot after,
      final List<String> regions) {
    return (int)
        regions.stream()
            .filter(
                region ->
                    !before.getOwnerConsumerId(region).equals(after.getOwnerConsumerId(region)))
            .count();
  }
}
