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

package org.apache.iotdb.session.subscription.util;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionIdentifierSemanticException;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SubscriptionUtilityTest {

  @Test
  public void testSetPartitionerRepeatsAndBalancesElements() {
    final List<Set<String>> repeated =
        SetPartitioner.partition(new HashSet<>(Arrays.asList("a", "b")), 4);
    assertEquals(4, repeated.size());
    repeated.forEach(partition -> assertEquals(1, partition.size()));

    final Set<String> elements = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e"));
    final List<Set<String>> balanced = SetPartitioner.partition(elements, 3);
    assertEquals(3, balanced.size());
    assertEquals(elements, balanced.stream().collect(HashSet::new, Set::addAll, Set::addAll));
    assertTrue(balanced.stream().allMatch(partition -> !partition.isEmpty()));
  }

  @Test
  public void testIdentifierValidationAndQuotedEscapes() {
    assertEquals("topic_1", IdentifierUtils.checkAndParseIdentifier("topic_1"));
    assertEquals("topic`name", IdentifierUtils.checkAndParseIdentifier("`topic``name`"));

    assertThrows(
        SubscriptionIdentifierSemanticException.class,
        () -> IdentifierUtils.checkAndParseIdentifier(null));
    assertThrows(
        SubscriptionIdentifierSemanticException.class,
        () -> IdentifierUtils.checkAndParseIdentifier(""));
    assertThrows(
        SubscriptionIdentifierSemanticException.class,
        () -> IdentifierUtils.checkAndParseIdentifier("1.5"));
    assertThrows(
        SubscriptionIdentifierSemanticException.class,
        () -> IdentifierUtils.checkAndParseIdentifier("topic-name"));
  }

  @Test
  public void testPollTimerMonotonicityExpirationAndOverflow() {
    final PollTimer timer = new PollTimer(100L, 50L);
    assertEquals(50L, timer.remainingMs());
    assertTrue(timer.notExpired());
    assertTrue(timer.isExpired(50L));

    timer.update(120L);
    assertEquals(20L, timer.elapsedMs());
    assertEquals(30L, timer.remainingMs());
    timer.update(110L);
    assertEquals(120L, timer.currentTimeMs());

    timer.update(150L);
    assertTrue(timer.isExpired());
    assertFalse(timer.notExpired());
    assertEquals(0L, timer.remainingMs());

    timer.update(Long.MAX_VALUE - 5);
    timer.reset(10L);
    assertEquals(5L, timer.remainingMs());
    assertThrows(IllegalArgumentException.class, () -> timer.reset(-1L));
  }
}
