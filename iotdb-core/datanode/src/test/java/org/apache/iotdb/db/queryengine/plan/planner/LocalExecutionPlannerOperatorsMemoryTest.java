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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class LocalExecutionPlannerOperatorsMemoryTest {

  private static final LocalExecutionPlanner PLANNER = LocalExecutionPlanner.getInstance();

  private long bytesHeldByTest = 0L;

  @After
  public void tearDown() {
    if (bytesHeldByTest > 0) {
      PLANNER.releaseToFreeMemoryForOperators(bytesHeldByTest);
      bytesHeldByTest = 0L;
    }
  }

  @Test
  public void testAllocateOperatorsMemoryFailsWhenInsufficient() {
    long free = PLANNER.getFreeMemoryForOperators();
    Assert.assertEquals(-1L, PLANNER.allocateOperatorsMemoryForTest(free + 1024L));
  }

  @Test
  public void testAllocateOperatorsMemorySucceedsWhenAvailable() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    long reserved = PLANNER.allocateOperatorsMemoryForTest(request);
    Assert.assertEquals(request, reserved);
    bytesHeldByTest = reserved;
  }

  @Test
  public void testHighestPriorityBypassWithoutConsumingOperatorsPool()
      throws MemoryNotEnoughException {
    long freeBefore = PLANNER.getFreeMemoryForOperators();
    long request = freeBefore + 1024L;

    Assert.assertEquals(0L, PLANNER.reserveOperatorsMemoryForFragmentForTest(request, true));
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testHighestPriorityBypassWhenPoolInsufficient() throws MemoryNotEnoughException {
    long free = PLANNER.getFreeMemoryForOperators();
    long request = free + 1024L;

    Assert.assertEquals(0L, PLANNER.reserveOperatorsMemoryForFragmentForTest(request, true));
  }

  @Test
  public void testNormalPriorityThrowsWhenPoolInsufficient() {
    long free = PLANNER.getFreeMemoryForOperators();
    long request = free + 1024L;

    try {
      PLANNER.reserveOperatorsMemoryForFragmentForTest(request, false);
      Assert.fail("Expect MemoryNotEnoughException");
    } catch (MemoryNotEnoughException ignore) {
    }
  }
}
