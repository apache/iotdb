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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionPrefetchingQueueStatesTest {

  private static final int LOCAL_COUNT_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchEventLocalCountThreshold();
  private static final int GLOBAL_COUNT_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchEventGlobalCountThreshold();

  @Test
  public void testShouldThrottlePollByLocalInFlightEventCount() {
    assertFalse(newStates(queueWithCounts(0, LOCAL_COUNT_THRESHOLD - 1L), 1).shouldThrottlePoll());
    assertTrue(newStates(queueWithCounts(0, LOCAL_COUNT_THRESHOLD), 1).shouldThrottlePoll());
  }

  @Test
  public void testShouldThrottlePollByGlobalInFlightEventCount() {
    final long inFlightEventCount = LOCAL_COUNT_THRESHOLD - 1L;
    final int belowGlobalQueueCount =
        Math.max(1, (int) ((GLOBAL_COUNT_THRESHOLD - 1L) / inFlightEventCount));
    final int reachingGlobalQueueCount =
        (int) ((GLOBAL_COUNT_THRESHOLD + inFlightEventCount - 1L) / inFlightEventCount);

    assertFalse(
        newStates(queueWithCounts(0, inFlightEventCount), belowGlobalQueueCount)
            .shouldThrottlePoll());
    assertTrue(
        newStates(queueWithCounts(0, inFlightEventCount), reachingGlobalQueueCount)
            .shouldThrottlePoll());
  }

  @Test
  public void testRetainedLocalEventLimitCountsAllRetainedEvents() throws Exception {
    assertFalse(
        invokeStatePredicate(
            newStates(queueWithCounts(LOCAL_COUNT_THRESHOLD, 0), 1),
            "hasTooManyRetainedLocalEvent"));
    assertTrue(
        invokeStatePredicate(
            newStates(queueWithCounts(LOCAL_COUNT_THRESHOLD + 1L, 0), 1),
            "hasTooManyRetainedLocalEvent"));
  }

  @Test
  public void testRetainedGlobalEventLimitUsesInjectedQueueCount() throws Exception {
    final long retainedEventCount = Math.max(1L, GLOBAL_COUNT_THRESHOLD / 2L);
    final int belowOrAtGlobalQueueCount =
        Math.max(1, (int) (GLOBAL_COUNT_THRESHOLD / retainedEventCount));
    final int aboveGlobalQueueCount = belowOrAtGlobalQueueCount + 1;

    assertFalse(
        invokeStatePredicate(
            newStates(queueWithCounts(retainedEventCount, 0), belowOrAtGlobalQueueCount),
            "hasTooManyRetainedGlobalEvent"));
    assertTrue(
        invokeStatePredicate(
            newStates(queueWithCounts(retainedEventCount, 0), aboveGlobalQueueCount),
            "hasTooManyRetainedGlobalEvent"));
  }

  private static SubscriptionPrefetchingQueueStates newStates(
      final SubscriptionPrefetchingQueue queue, final int prefetchingQueueCount) {
    return new SubscriptionPrefetchingQueueStates(queue, () -> prefetchingQueueCount);
  }

  private static SubscriptionPrefetchingQueue queueWithCounts(
      final long retainedEventCount, final long inFlightEventCount) {
    final SubscriptionPrefetchingQueue queue = mock(SubscriptionPrefetchingQueue.class);
    when(queue.getSubscriptionRetainedEventCount()).thenReturn(retainedEventCount);
    when(queue.getSubscriptionUncommittedEventCount()).thenReturn(inFlightEventCount);
    return queue;
  }

  private static boolean invokeStatePredicate(
      final SubscriptionPrefetchingQueueStates states, final String methodName) throws Exception {
    final Method method = SubscriptionPrefetchingQueueStates.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    return (boolean) method.invoke(states);
  }
}
