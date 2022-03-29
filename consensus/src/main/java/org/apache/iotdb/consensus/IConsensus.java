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

import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import java.io.IOException;
import java.util.List;

/** Consensus module base class. Each method should be thread-safe */
public interface IConsensus {

  void start() throws IOException;

  void stop() throws IOException;

  // write API
  ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest);
  // read API
  ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest);

  // multi consensus group API
  ConsensusGenericResponse addConsensusGroup(ConsensusGroupId groupId, List<Peer> peers);

  ConsensusGenericResponse removeConsensusGroup(ConsensusGroupId groupId);

  // single consensus group API
  ConsensusGenericResponse addPeer(ConsensusGroupId groupId, Peer peer);

  ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer);

  ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers);

  // management API
  ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader);

  ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId);
}
