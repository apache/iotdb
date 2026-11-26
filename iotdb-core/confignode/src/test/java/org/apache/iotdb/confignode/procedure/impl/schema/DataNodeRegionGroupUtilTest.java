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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class DataNodeRegionGroupUtilTest {
  @Test
  public void testLeaderSelectionAfterReplicaMigration() {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    final TDataNodeLocation firstReplica = new TDataNodeLocation().setDataNodeId(1);
    final TDataNodeLocation secondReplica = new TDataNodeLocation().setDataNodeId(2);
    final TRegionReplicaSet replicas =
        new TRegionReplicaSet(region, Arrays.asList(firstReplica, secondReplica));

    // A stale leader outside the replica set must fall back to a current replica.
    for (final Integer leaderId : Arrays.asList(null, -1, 0, 1, 2)) {
      assertEquals(
          Collections.singletonMap(
              Integer.valueOf(2).equals(leaderId) ? secondReplica : firstReplica,
              Collections.singletonList(region)),
          DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap(
              Collections.singletonMap(region, leaderId),
              Collections.singletonMap(region, replicas)));
    }
  }
}
