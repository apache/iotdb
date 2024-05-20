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

import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.pipe.thrift.PipeConsensusIService;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public class PipeConsensusRPCServiceProcessor implements PipeConsensusIService.AsyncIface {

  private final PipeConsensusConfig.Pipe config;

  public PipeConsensusRPCServiceProcessor(PipeConsensusConfig.Pipe config) {
    this.config = config;
  }

  @Override
  public void pipeConsensusTransfer(
      TPipeConsensusTransferReq req,
      AsyncMethodCallback<TPipeConsensusTransferResp> resultHandler) {
    try {
      TPipeConsensusTransferResp resp = config.getConsensusPipeReceiver().receive(req);
      // we need to call onComplete by hand
      resultHandler.onComplete(resp);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  // TODO: consider batch transfer
  @Override
  public void pipeConsensusBatchTransfer(
      TPipeConsensusBatchTransferReq req,
      AsyncMethodCallback<TPipeConsensusBatchTransferResp> resultHandler)
      throws TException {}

  // TODO：添加发送端重启后，释放资源逻辑
  public void handleClientExit() {}
}
