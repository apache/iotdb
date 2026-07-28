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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RemoveRegionPeerProcedureTest {
  @Test
  public void cancellationBoundaryTest() {
    RemoveRegionPeerProcedure cancellationWins = new RemoveRegionPeerProcedure();
    Assert.assertTrue(cancellationWins.tryCancel());
    Assert.assertFalse(cancellationWins.disableCancellation());

    RemoveRegionPeerProcedure removalWins = new RemoveRegionPeerProcedure();
    Assert.assertTrue(removalWins.disableCancellation());
    Assert.assertFalse(removalWins.tryCancel());
  }

  @Test
  public void stateOrdinalCompatibilityTest() {
    Assert.assertEquals(0, RemoveRegionPeerState.TRANSFER_REGION_LEADER.ordinal());
    Assert.assertEquals(1, RemoveRegionPeerState.REMOVE_REGION_PEER.ordinal());
    Assert.assertEquals(2, RemoveRegionPeerState.DELETE_OLD_REGION_PEER.ordinal());
    Assert.assertEquals(3, RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE.ordinal());
    Assert.assertEquals(4, RemoveRegionPeerState.DROP_CONSENSUS_PIPES.ordinal());
    Assert.assertEquals(5, RemoveRegionPeerState.PREPARE_REMOVE_REGION_PEER.ordinal());
  }

  @Test
  public void serDeTest() throws Exception {
    RemoveRegionPeerProcedure procedure =
        new RemoveRegionPeerProcedure(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            new TDataNodeLocation(
                1,
                new TEndPoint("127.0.0.1", 0),
                new TEndPoint("127.0.0.1", 1),
                new TEndPoint("127.0.0.1", 2),
                new TEndPoint("127.0.0.1", 3),
                new TEndPoint("127.0.0.1", 4)),
            new TDataNodeLocation(
                5,
                new TEndPoint("127.0.0.1", 5),
                new TEndPoint("127.0.0.1", 6),
                new TEndPoint("127.0.0.1", 7),
                new TEndPoint("127.0.0.1", 8),
                new TEndPoint("127.0.0.1", 9)));
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    }
  }
}
