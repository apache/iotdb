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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.List;

/** Consensus module base interface. */
@ThreadSafe
public interface IConsensus {

  /**
   * Start the consensus module. Note: You should call this function immediately after initializing
   * the instance, because calling other functions without start may produce unexpected errors
   *
   * @throws IOException when start consensus errors
   */
  void start() throws IOException;

  /**
   * Stop the consensus module.
   *
   * @throws IOException when stop consensus errors
   */
  void stop() throws IOException;

  /**
   * Write data to the corresponding consensus group.
   *
   * @param groupId the consensus group this request belongs
   * @param request write request
   * @return write result
   * @throws ConsensusGroupNotExistException when the specified consensus group doesn't exist
   * @throws ConsensusException when write doesn't success with other reasons
   */
  TSStatus write(ConsensusGroupId groupId, IConsensusRequest request) throws ConsensusException;

  /**
   * Read data from the corresponding consensus group.
   *
   * @param groupId the consensus group this request belongs
   * @param request read request
   * @return read result
   * @throws ConsensusGroupNotExistException when the specified consensus group doesn't exist
   * @throws ConsensusException when read doesn't success with other reasons
   */
  DataSet read(ConsensusGroupId groupId, IConsensusRequest request) throws ConsensusException;

  // multi consensus group API

  /**
   * Require the <em>local node</em> to create a Peer and become a member of the given consensus
   * group. This node will prepare and initialize local statemachine {@link IStateMachine} and other
   * data structures. After this method returns, we can call {@link #addRemotePeer(ConsensusGroupId,
   * Peer)} to notify original group that this new Peer is prepared to be added into the latest
   * configuration. createLocalPeer should be called on a node that does not contain any peer of the
   * consensus group, to avoid one node having more than one replica.
   *
   * @param groupId the consensus group this peer belongs
   * @param peers other known peers in this group
   * @throws ConsensusGroupAlreadyExistException when the specified consensus group already exists
   * @throws IllegalPeerNumException when the peer num is illegal. The exception is that it is legal
   *     to pass an empty list for RaftConsensus
   * @throws IllegalPeerEndpointException when peers don't contain local node. The exception is that
   *     it is legal to pass an empty list for RaftConsensus
   * @throws ConsensusException when createLocalPeer doesn't success with other reasons
   */
  void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers) throws ConsensusException;

  /**
   * When the <em>local node</em> is no longer a member of the given consensus group, call this
   * method to do cleanup works. This method will close local statemachine {@link IStateMachine},
   * delete local data and do other cleanup works. deleteLocalPeer should be called after
   * successfully removing this peer from current consensus group configuration (by calling {@link
   * #removeRemotePeer(ConsensusGroupId, Peer)}).
   *
   * @param groupId the consensus group this peer used to belong
   * @throws ConsensusGroupNotExistException when the specified consensus group doesn't exist
   * @throws ConsensusException when deleteLocalPeer doesn't success with other reasons
   */
  void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException;

  // single consensus group API

  /**
   * Tell the group that a new Peer is prepared to be added into this group. Call {@link
   * #createLocalPeer(ConsensusGroupId, List)} on the new Peer before calling this method. When this
   * method returns, the group data should be already transmitted to the new Peer. That is, the new
   * peer is available to answer client requests by the time this method successfully returns.
   * addRemotePeer should be called on a living peer of the consensus group. For example: We'd like
   * to add a peer D to (A, B, C) group. We need to execute addPeer in A, B or C.
   *
   * @param groupId the consensus group this peer belongs
   * @param peer the newly added peer
   * @throws PeerAlreadyInConsensusGroupException when the peer has been added into this consensus
   *     group
   * @throws ConsensusException when addRemotePeer doesn't success with other reasons
   */
  void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException;

  /**
   * Tell the group to remove an active Peer. The removed peer can no longer answer group requests
   * when this method successfully returns. Call {@link #deleteLocalPeer(ConsensusGroupId)} on the
   * removed Peer to do cleanup jobs after this method successfully returns. removeRemotePeer should
   * be called on a living peer of its consensus group. For example: a group has A, B, C. We'd like
   * to remove C, in case C is dead, the removePeer should be sent to A or B.
   *
   * @param groupId the consensus group this peer belongs
   * @param peer the peer to be removed
   * @throws PeerNotInConsensusGroupException when the peer hasn't yet joined this consensus group
   * @throws ConsensusException when removeRemotePeer doesn't success with other reasons
   */
  void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException;

  /**
   * Reset the peer list of the corresponding consensus group. Currently only used in the automatic
   * cleanup of region migration as a rollback for {@link #addRemotePeer(ConsensusGroupId, Peer)},
   * so it will only be less but not more.
   *
   * @param groupId the consensus group
   * @param correctPeers the correct peer list
   * @throws ConsensusException when resetPeerList doesn't success with other reasons
   * @throws ConsensusGroupNotExistException when the specified consensus group doesn't exist
   */
  void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers) throws ConsensusException;

  // management API

  /**
   * Transfer the leadership to other peer to meet some load balancing needs.
   *
   * @param groupId the consensus group which should execute this command
   * @param newLeader the target leader peer
   * @throws ConsensusGroupNotExistException when the specified consensus group doesn't exist
   * @throws ConsensusException when transferLeader doesn't success with other reasons
   */
  void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException;

  /**
   * Trigger the snapshot of the corresponding consensus group.
   *
   * @param groupId the consensus group which should execute this command
   * @param force if {@link true}, force to take a snapshot
   * @throws ConsensusException when triggerSnapshot doesn't success with other reasons
   */
  void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException;

  /**
   * Determine if the current peer is the leader in the corresponding consensus group.
   *
   * @param groupId the consensus group
   * @return {@code true} or {@code false}
   */
  boolean isLeader(ConsensusGroupId groupId);

  /**
   * Returns the logic clock of the current consensus group
   *
   * @param groupId the consensus group
   * @return long
   */
  long getLogicalClock(ConsensusGroupId groupId);

  /**
   * Determine if the current peer is the leader and already able to provide services in the
   * corresponding consensus group.
   *
   * @param groupId the consensus group
   * @return {@code true} or {@code false}
   */
  boolean isLeaderReady(ConsensusGroupId groupId);

  /**
   * Return the leader peer of the corresponding consensus group.
   *
   * @param groupId the consensus group
   * @return return null if group doesn't exist or leader is undetermined, or return leader Peer
   */
  Peer getLeader(ConsensusGroupId groupId);

  /**
   * Return the replicationNum of the corresponding consensus group.
   *
   * @param groupId the consensus group
   * @return return 0 if group doesn't exist, or return replicationNum
   */
  int getReplicationNum(ConsensusGroupId groupId);

  /**
   * Return all consensus group ids.
   *
   * @return consensusGroupId list
   */
  List<ConsensusGroupId> getAllConsensusGroupIds();

  /**
   * Return all consensus group ids from disk.
   *
   * <p>We need to parse all the RegionGroupIds from the disk directory before starting the
   * consensus layer, and {@link #getAllConsensusGroupIds()} returns an empty list, so we need to
   * add a new interface.
   *
   * @return consensusGroupId list
   */
  List<ConsensusGroupId> getAllConsensusGroupIdsWithoutStarting();

  /**
   * Return the region directory of the corresponding consensus group.
   *
   * @param groupId the consensus group
   * @return region directory
   */
  String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId);

  /**
   * Reload the consensus config.
   *
   * @param consensusConfig the new consensus config
   */
  void reloadConsensusConfig(ConsensusConfig consensusConfig);
}
