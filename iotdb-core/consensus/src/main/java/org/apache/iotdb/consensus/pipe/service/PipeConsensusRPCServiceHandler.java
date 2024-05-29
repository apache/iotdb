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

package org.apache.iotdb.consensus.pipe.service;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConsensusRPCServiceHandler implements TServerEventHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusRPCServiceHandler.class);
  private final PipeConsensusRPCServiceProcessor processor;

  public PipeConsensusRPCServiceHandler(PipeConsensusRPCServiceProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void preServe() {}

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    // Construct clientReq from inputProtocol
    TPipeConsensusTransferReq clientReq = new TPipeConsensusTransferReq();
    try {
      clientReq.read(input);
    } catch (TException e) {
      LOGGER.error(
          "PipeConsensus: failed to delete receiver's context when sender's RpcClient exit, because: {}",
          e.getMessage());
    }
    processor.handleClientExit(
        ConsensusGroupId.Factory.createFromTConsensusGroupId(clientReq.getConsensusGroupId()),
        clientReq.getDataNodeId(),
        clientReq.getCommitId());
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {}
}
