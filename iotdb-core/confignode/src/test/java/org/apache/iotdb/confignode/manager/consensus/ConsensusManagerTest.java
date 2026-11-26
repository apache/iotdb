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

package org.apache.iotdb.confignode.manager.consensus;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ConsensusManagerTest {

  @Test
  public void createPeerForConsensusGroupShouldIgnoreAlreadyCreatedLocalPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new ConsensusGroupAlreadyExistException(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID))
        .when(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());

    newConsensusManager(consensus)
        .createPeerForConsensusGroup(Collections.singletonList(newConfigNodeLocation(1)));

    Mockito.verify(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
  }

  @Test
  public void addConfigNodePeerShouldIgnoreAlreadyAddedPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new PeerAlreadyInConsensusGroupException(
                ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
                new Peer(
                    ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
                    1,
                    new TEndPoint("127.0.0.1", 10720))))
        .when(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    newConsensusManager(consensus).addConfigNodePeer(newConfigNodeLocation(1));

    Mockito.verify(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));
  }

  @Test
  public void addConfigNodePeerShouldKeepFailingForOtherConsensusErrors() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(new ConsensusException("reconfiguration failed"))
        .when(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    Assert.assertThrows(
        AddPeerException.class,
        () -> newConsensusManager(consensus).addConfigNodePeer(newConfigNodeLocation(1)));
  }

  @Test
  public void removeConfigNodePeerShouldIgnoreAlreadyRemovedPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new PeerNotInConsensusGroupException(
                ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID, "127.0.0.1:10720"))
        .when(consensus)
        .removeRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    Assert.assertTrue(
        newConsensusManager(consensus).removeConfigNodePeer(newConfigNodeLocation(1)));
  }

  private static ConsensusManager newConsensusManager(final IConsensus consensus) throws Exception {
    return new ConsensusManager(Mockito.mock(IManager.class), consensus);
  }

  private static TConfigNodeLocation newConfigNodeLocation(final int configNodeId) {
    return new TConfigNodeLocation(
        configNodeId,
        new TEndPoint("127.0.0.1", 10710 + configNodeId),
        new TEndPoint("127.0.0.1", 10720 + configNodeId));
  }
}
