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

package org.apache.iotdb.confignode.manager.lease;

import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.DataNodeState;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Verdict;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MetadataBroadcastVerdictTest {

  private static final long T_PROCEED_MS = 25_000L;

  private static DataNodeState acked() {
    return new DataNodeState(true, 0L);
  }

  private static DataNodeState fencedSafe() {
    return new DataNodeState(false, T_PROCEED_MS + 1);
  }

  private static DataNodeState freshUnacked() {
    return new DataNodeState(false, 1_000L);
  }

  @Test
  public void allAckedProceeds() {
    assertEquals(
        Verdict.PROCEED,
        MetadataBroadcastVerdict.decide(Arrays.asList(acked(), acked()), T_PROCEED_MS, false));
  }

  @Test
  public void unackedButAllFencedSafeProceeds() {
    assertEquals(
        Verdict.PROCEED,
        MetadataBroadcastVerdict.decide(Arrays.asList(acked(), fencedSafe()), T_PROCEED_MS, false));
  }

  @Test
  public void freshUnackedWaitsWhileBudgetRemains() {
    assertEquals(
        Verdict.WAIT,
        MetadataBroadcastVerdict.decide(
            Collections.singletonList(freshUnacked()), T_PROCEED_MS, false));
  }

  @Test
  public void freshUnackedFailsWhenWaitBudgetExhausted() {
    assertEquals(
        Verdict.FAIL,
        MetadataBroadcastVerdict.decide(
            Collections.singletonList(freshUnacked()), T_PROCEED_MS, true));
  }
}
