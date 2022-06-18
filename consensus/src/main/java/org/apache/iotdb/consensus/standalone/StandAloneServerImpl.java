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

package org.apache.iotdb.consensus.standalone;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.io.File;

public class StandAloneServerImpl implements IStateMachine {

  private final Peer peer;
  private final IStateMachine stateMachine;

  public StandAloneServerImpl(Peer peer, IStateMachine stateMachine) {
    this.peer = peer;
    this.stateMachine = stateMachine;
  }

  public Peer getPeer() {
    return peer;
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public void start() {
    stateMachine.start();
    // Notify itself as the leader
    stateMachine.event().notifyLeaderChanged(peer.getGroupId(), peer.getEndpoint());
  }

  @Override
  public void stop() {
    stateMachine.stop();
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    return stateMachine.write(request);
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return stateMachine.takeSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    stateMachine.loadSnapshot(latestSnapshotRootDir);
  }
}
