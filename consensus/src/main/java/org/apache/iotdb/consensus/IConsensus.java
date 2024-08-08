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
package org.apache.iotdb.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.ConsensusException;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.List;

/** Consensus module base class. */
@ThreadSafe
public interface IConsensus {

  void start() throws IOException;

  void stop() throws IOException;

  // write API
  ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest);
  // read API
  ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest);

  // multi consensus group API

  /**
   * Require the <em>local node</em> to create a Peer and become a member of the given consensus
   * group. This node will prepare and initialize local statemachine {@link IStateMachine} and other
   * data structures. After this method returns, we can call {@link #addPeer(ConsensusGroupId,
   * Peer)} to notify original group that this new Peer is prepared to be added into the latest
   * configuration. createPeer should be called on a node that does not contain any peer of the
   * consensus group, to avoid one node having more than one replica.
   *
   * @param groupId the consensus group this Peer belongs
   * @param peers other known peers in this group
   */
  ConsensusGenericResponse createPeer(ConsensusGroupId groupId, List<Peer> peers);

  /**
   * When the <em>local node</em> is no longer a member of the given consensus group, call this
   * method to do cleanup works. This method will close local statemachine {@link IStateMachine},
   * delete local data and do other cleanup works. Be sure this method is called after successfully
   * removing this peer from current consensus group configuration (by calling {@link
   * #removePeer(ConsensusGroupId, Peer)} or {@link #changePeer(ConsensusGroupId, List)}).
   *
   * @param groupId the consensus group this Peer used to belong
   */
  ConsensusGenericResponse deletePeer(ConsensusGroupId groupId);

  // single consensus group API

  /**
   * Tell the group that a new Peer is prepared to be added into this group. Call {@link
   * #createPeer(ConsensusGroupId, List)} on the new Peer before calling this method. When this
   * method returns, the group data should be already transmitted to the new Peer. That is, the new
   * peer is available to answer client requests by the time this method successfully returns.
   * addPeer should be called on a living peer of the consensus group. For example: We'd like to add
   * a peer D to (A, B, C) group. We need to execute addPeer in A, B or C.
   *
   * @param groupId the consensus group this peer belongs
   * @param peer the newly added peer
   */
  ConsensusGenericResponse addPeer(ConsensusGroupId groupId, Peer peer);

  /**
   * Tell the group to remove an active Peer. The removed peer can no longer answer group requests
   * when this method successfully returns. Call {@link #deletePeer(ConsensusGroupId)} on the
   * removed Peer to do cleanup jobs after this method successfully returns. removePeer should be
   * called on a living peer of its consensus group. For example: a group has A, B, C. We'd like to
   * remove C, in case C is dead, the removePeer should be sent to A or B.
   *
   * @param groupId the consensus group this peer belongs
   * @param peer the peer to be removed
   */
  ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer);

  /**
   * Tell the group to update an active Peer. The modifiable part of {@link Peer} is TEndPoint.
   *
   * @param groupId the consensus group this peer belongs
   * @param oldPeer the peer to be updated
   * @param newPeer the peer to be updated to
   */
  // TODO: @SzyWilliam @SpriCoder Please implement this method
  ConsensusGenericResponse updatePeer(ConsensusGroupId groupId, Peer oldPeer, Peer newPeer);

  /**
   * Change group configuration. This method allows you to add/remove multiple Peers at once. This
   * method is similar to {@link #addPeer(ConsensusGroupId, Peer)} or {@link
   * #removePeer(ConsensusGroupId, Peer)}
   *
   * @param groupId the consensus group
   * @param newPeers the new member configuration of this group
   */
  ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers);

  /**
   * Tell the group to [create a new Peer on new node] and [add this member to join the group].
   *
   * <p>The underlying implementation should <br>
   * 1. first call createPeer on the new member <br>
   * 2. then call addPeer for configuration change <br>
   * Call this method on any node of the <em>original group</em>. <br>
   * NOTICE: Currently only RatisConsensus implements this method.
   *
   * @param groupId the consensus group
   * @param newNode the new member
   * @param originalGroup the original members of the existed group
   */
  default ConsensusGenericResponse addNewNodeToExistedGroup(
      ConsensusGroupId groupId, Peer newNode, List<Peer> originalGroup) {
    return ConsensusGenericResponse.newBuilder()
        .setSuccess(false)
        .setException(
            new ConsensusException(
                "addNewNodeToExistedGroup method is not implemented by " + this + " class"))
        .build();
  }

  // management API
  ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader);

  ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId);

  boolean isLeader(ConsensusGroupId groupId);

  Peer getLeader(ConsensusGroupId groupId);

  List<ConsensusGroupId> getAllConsensusGroupIds();
}
