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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.SnapshotMeta;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.statemachine.IStateMachine;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ApplicationStateMachineProxy extends BaseStateMachine {
  private final Logger logger = LoggerFactory.getLogger(ApplicationStateMachineProxy.class);
  private final IStateMachine applicationStateMachine;

  // Raft Storage sub dir for statemachine data, default (_sm)
  private File statemachineDir;
  private final SnapshotStorage snapshotStorage;

  public ApplicationStateMachineProxy(IStateMachine stateMachine) {
    applicationStateMachine = stateMachine;
    snapshotStorage = new SnapshotStorage(applicationStateMachine);
    applicationStateMachine.start();
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage)
      throws IOException {
    getLifeCycle()
        .startAndTransition(
            () -> {
              snapshotStorage.init(storage);
              this.statemachineDir = snapshotStorage.getStateMachineDir();
              loadSnapshot(applicationStateMachine.getLatestSnapshot(statemachineDir));
            });
  }

  @Override
  public void reinitialize() throws IOException {
    setLastAppliedTermIndex(null);
    loadSnapshot(applicationStateMachine.getLatestSnapshot(statemachineDir));
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      getLifeCycle().transition(LifeCycle.State.STARTING);
      getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
  }

  @Override
  public void pause() {
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  @Override
  public void close() throws IOException {
    getLifeCycle().checkStateAndClose(applicationStateMachine::stop);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    RaftProtos.LogEntryProto log = trx.getLogEntry();
    updateLastAppliedTermIndex(log.getTerm(), log.getIndex());

    IConsensusRequest applicationRequest = null;

    // if this server is leader
    // it will first try to obtain applicationRequest from transaction context
    if (trx.getClientRequest() != null
        && trx.getClientRequest().getMessage() instanceof RequestMessage) {
      RequestMessage requestMessage = (RequestMessage) trx.getClientRequest().getMessage();
      applicationRequest = requestMessage.getActualRequest();
    } else {
      applicationRequest =
          new ByteBufferConsensusRequest(
              log.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    }

    TSStatus result = applicationStateMachine.write(applicationRequest);
    Message ret = new ResponseMessage(result);

    return CompletableFuture.completedFuture(ret);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    if (!(request instanceof RequestMessage)) {
      // return null dataset to indicate an error
      logger.error("An RequestMessage is required but got {}", request);
      return CompletableFuture.completedFuture(new ResponseMessage(null));
    }
    RequestMessage requestMessage = (RequestMessage) request;
    DataSet result = applicationStateMachine.read(requestMessage.getActualRequest());
    return CompletableFuture.completedFuture(new ResponseMessage(result));
  }

  @Override
  public long takeSnapshot() throws IOException {
    final TermIndex lastApplied = getLastAppliedTermIndex();
    if (lastApplied.getTerm() <= 0 || lastApplied.getIndex() <= 0) {
      return RaftLog.INVALID_LOG_INDEX;
    }

    // require the application statemachine to take the latest snapshot
    ByteBuffer metadata = Utils.getMetadataFromTermIndex(lastApplied);
    boolean success = applicationStateMachine.takeSnapshot(metadata, statemachineDir);
    if (!success) {
      return RaftLog.INVALID_LOG_INDEX;
    }

    return lastApplied.getIndex();
  }

  private void loadSnapshot(SnapshotMeta snapshot) {
    if (snapshot == null) {
      return;
    }

    // require the application statemachine to load the latest snapshot
    applicationStateMachine.loadSnapshot(snapshot);
    ByteBuffer metadata = snapshot.getMetadata();
    TermIndex snapshotTermIndex = Utils.getTermIndexFromMetadata(metadata);
    updateLastAppliedTermIndex(snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return snapshotStorage;
  }
}
