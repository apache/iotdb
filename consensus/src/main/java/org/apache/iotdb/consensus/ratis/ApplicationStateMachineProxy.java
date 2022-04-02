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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ApplicationStateMachineProxy extends BaseStateMachine {
  private final IStateMachine applicationStateMachine;
  private final Logger logger = LoggerFactory.getLogger(ApplicationStateMachineProxy.class);

  public ApplicationStateMachineProxy(IStateMachine stateMachine) {
    applicationStateMachine = stateMachine;
    applicationStateMachine.start();
  }

  @Override
  public void close() throws IOException {
    applicationStateMachine.stop();
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
}
