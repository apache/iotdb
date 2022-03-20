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

import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ApplicationStateMachineProxy extends BaseStateMachine {
  private final IStateMachine applicationStateMachine;

  public ApplicationStateMachineProxy(IStateMachine stateMachine) {
    applicationStateMachine = stateMachine;
    applicationStateMachine.start();
  }

  @Override
  public void close() throws IOException {
    super.close();
    applicationStateMachine.stop();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    RaftProtos.LogEntryProto log = trx.getLogEntry();
    updateLastAppliedTermIndex(log.getTerm(), log.getIndex());

    IConsensusRequest request =
        new ByteBufferConsensusRequest(
            log.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    TSStatus result = applicationStateMachine.write(request);

    Message ret = null;
    try {
      ByteBuffer serializedStatus = Utils.serializeTSStatus(result);
      ret = Message.valueOf(ByteString.copyFrom(serializedStatus));
    } catch (TException ignored) {
    }

    return CompletableFuture.completedFuture(ret);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    IConsensusRequest req =
        new ByteBufferConsensusRequest(request.getContent().asReadOnlyByteBuffer());
    DataSet result = applicationStateMachine.read(req);
    return CompletableFuture.completedFuture(new ReadLocalMessage(result));
  }
}
