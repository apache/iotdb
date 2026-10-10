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

package org.apache.iotdb.commons.cluster;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.commons.cluster.NodeStatus.ReadOnly;
import static org.apache.iotdb.commons.cluster.NodeStatus.Removing;
import static org.apache.iotdb.commons.cluster.NodeStatus.Running;
import static org.apache.iotdb.commons.cluster.NodeStatus.Stopped;
import static org.apache.iotdb.commons.cluster.NodeStatus.Unknown;

public class NodeStatusTest {

  @Test
  public void testOrdinaryTransitionTable() {
    NodeStatus[] statuses = NodeStatus.values();

    NodeStatus[][] expected = {
      {Running, Unknown, Removing, ReadOnly, Stopped},
      {Running, Unknown, Removing, ReadOnly, Stopped},
      {Removing, Removing, Removing, Removing, Removing},
      {Running, Unknown, Removing, ReadOnly, Stopped},
      {Running, Stopped, Removing, ReadOnly, Stopped}
    };

    Assert.assertEquals(statuses.length, expected.length);
    for (int previous = 0; previous < statuses.length; previous++) {
      Assert.assertEquals(statuses.length, expected[previous].length);
      for (int requested = 0; requested < statuses.length; requested++) {
        Assert.assertSame(
            statuses[previous] + " -> " + statuses[requested],
            expected[previous][requested],
            NodeStatus.transition(statuses[previous], statuses[requested], false));
      }
    }
  }

  @Test
  public void testManagementCanRestoreEveryPreviousStatus() {
    for (NodeStatus previous : NodeStatus.values()) {
      for (NodeStatus requested : NodeStatus.values()) {
        Assert.assertSame(requested, NodeStatus.transition(previous, requested, true));
      }
    }
  }

  @Test
  public void testStatusClassificationTable() {
    NodeStatus[] statuses = {Running, Unknown, Removing, ReadOnly, Stopped};
    // Columns: offline, possibly offline, Region candidate, readable, persistent.
    boolean[][] expected = {
      {false, false, true, true, false},
      {true, true, true, false, false},
      {false, true, false, true, true},
      {false, false, false, true, true},
      {true, true, true, false, true}
    };

    Assert.assertArrayEquals(NodeStatus.values(), statuses);
    Assert.assertEquals(statuses.length, expected.length);
    for (int i = 0; i < statuses.length; i++) {
      NodeStatus status = statuses[i];
      Assert.assertEquals(status + " isOffline", expected[i][0], status.isOffline());
      Assert.assertEquals(status + " mayBeOffline", expected[i][1], status.mayBeOffline());
      Assert.assertEquals(
          status + " isRegionCandidate", expected[i][2], status.isRegionCandidate());
      Assert.assertEquals(status + " isReadable", expected[i][3], NodeStatus.isReadable(status));
      Assert.assertEquals(
          status + " isPersistentStatus", expected[i][4], status.isPersistentStatus());
    }
  }
}
