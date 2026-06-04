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
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Disposition;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Verdict;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MetadataBroadcastVerdictTest {

  private static final long T_PROCEED_MS = 25_000L;

  // acked
  private static DataNodeState acked() {
    return new DataNodeState(true, false, true, 0L);
  }

  // capable, unacked, out of contact >= T_proceed -> provably fenced
  private static DataNodeState fencedSafe() {
    return new DataNodeState(false, false, true, T_PROCEED_MS + 1);
  }

  // capable, unacked, heartbeat still fresh -> still possibly serving
  private static DataNodeState freshUnacked() {
    return new DataNodeState(false, false, true, 1_000L);
  }

  // ---- classify ----

  @Test
  public void incapableDataNodeIsNeverFencedSafe() {
    // Review point 4: capability is checked before any timing test; an old DN that cannot
    // self-fence must be UNSAFE even if it has been silent far longer than T_proceed.
    final DataNodeState oldDnLongSilent = new DataNodeState(false, false, false, T_PROCEED_MS * 10);
    assertEquals(
        Disposition.UNSAFE, MetadataBroadcastVerdict.classify(oldDnLongSilent, T_PROCEED_MS));
  }

  @Test
  public void retiredFromRoutingIsSafeGone() {
    // Review point 5: only "removed from routing / explicit fence-shutdown ack" is safe-gone,
    // regardless of capability or how recently it was seen.
    final DataNodeState retired = new DataNodeState(false, true, false, 0L);
    assertEquals(Disposition.SAFE_GONE, MetadataBroadcastVerdict.classify(retired, T_PROCEED_MS));
  }

  @Test
  public void removingButStillRoutableIsUnsafe() {
    // Review point 5: a node that is merely Removing (still routable, not retired) with a fresh
    // heartbeat must NOT be treated as safe.
    assertEquals(
        Disposition.UNSAFE, MetadataBroadcastVerdict.classify(freshUnacked(), T_PROCEED_MS));
  }

  @Test
  public void capableAndLongSilentIsFencedSafe() {
    assertEquals(
        Disposition.FENCED_SAFE, MetadataBroadcastVerdict.classify(fencedSafe(), T_PROCEED_MS));
  }

  // ---- decide ----

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
