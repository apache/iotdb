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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

public interface ISingleConsensus {

  void start() throws IOException;

  void stop() throws IOException;

  // write API
  ConsensusWriteResponse write(IConsensusRequest request);
  // read API
  ConsensusReadResponse read(IConsensusRequest request);

  // single consensus group API
  ConsensusGenericResponse addPeer(Peer peer);

  ConsensusGenericResponse removePeer(Peer peer);

  ConsensusGenericResponse changePeer(List<Peer> newPeers);

  // management API
  ConsensusGenericResponse transferLeader(Peer newLeader);

  ConsensusGenericResponse triggerSnapshot();

  boolean isLeader();

  Peer getLeader();
}
