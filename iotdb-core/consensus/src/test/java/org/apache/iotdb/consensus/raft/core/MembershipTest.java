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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import org.junit.Test;

import java.util.Arrays;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MembershipTest {

  @Test
  public void majorityAndPeers() {
    Membership m = Membership.ofVoters(Arrays.asList(1, 2, 3));
    assertEquals(2, m.majority());
    assertEquals(new TreeSet<>(Arrays.asList(2, 3)), m.peersOf(1));
    assertTrue(m.isVoter(2));
    assertFalse(m.isVoter(4));
    assertEquals(3, Membership.ofVoters(Arrays.asList(1, 2, 3, 4)).majority());
  }

  @Test
  public void learnersAreMembersButNotVoters() {
    Membership m =
        Membership.ofVoters(Arrays.asList(1, 2, 3)).withLearner(4, new TEndPoint("h", 1), 7);
    assertTrue(m.isMember(4));
    assertFalse(m.isVoter(4));
    assertEquals(2, m.majority());
    assertEquals(new TreeSet<>(Arrays.asList(2, 3, 4)), m.peersOf(1));
    assertEquals(7, m.configIndex());
    Membership promoted = m.withVoter(4, null, 8);
    assertTrue(promoted.isVoter(4));
    assertEquals(3, promoted.majority());
    assertEquals(new TEndPoint("h", 1), promoted.endpointOf(4));
    Membership removed = promoted.without(1, 9);
    assertFalse(removed.isMember(1));
    assertEquals(new TreeSet<>(Arrays.asList(2, 3, 4)), removed.voters());
  }

  @Test
  public void emptyMembershipNeverHasAMajorityOfZero() {
    assertTrue(Membership.empty().isEmpty());
    assertEquals(1, Membership.empty().majority());
  }
}
