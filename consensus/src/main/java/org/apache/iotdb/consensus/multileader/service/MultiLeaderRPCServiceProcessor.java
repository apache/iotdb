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

package org.apache.iotdb.consensus.multileader.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.MultiLeaderConsensusRequest;
import org.apache.iotdb.consensus.multileader.MultiLeaderConsensus;
import org.apache.iotdb.consensus.multileader.MultiLeaderServerImpl;
import org.apache.iotdb.consensus.multileader.thrift.MultiLeaderConsensusIService;
import org.apache.iotdb.consensus.multileader.thrift.TLogBatch;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogReq;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiLeaderRPCServiceProcessor implements MultiLeaderConsensusIService.AsyncIface {

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderRPCServiceProcessor.class);

  private final MultiLeaderConsensus consensus;

  public MultiLeaderRPCServiceProcessor(MultiLeaderConsensus consensus) {
    this.consensus = consensus;
  }

  @Override
  public void syncLog(TSyncLogReq req, AsyncMethodCallback<TSyncLogRes> resultHandler) {
    try {
      ConsensusGroupId groupId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      MultiLeaderServerImpl impl = consensus.getImpl(groupId);
      if (impl == null) {
        String message =
            String.format(
                "Unexpected consensusGroupId %s for TSyncLogReq which size is %s",
                groupId, req.getBatches().size());
        logger.error(message);
        TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        status.setMessage(message);
        resultHandler.onComplete(new TSyncLogRes(Collections.singletonList(status)));
        return;
      }
      if (impl.isReadOnly()) {
        String message = "Fail to sync log because system is read-only.";
        logger.error(message);
        resultHandler.onError(
            new IoTDBException(message, TSStatusCode.READ_ONLY_SYSTEM_ERROR.getStatusCode()));
        return;
      }
      BatchIndexedConsensusRequest requestsInThisBatch = new BatchIndexedConsensusRequest();
      // We use synchronized to ensure atomicity of executing multiple logs
      if (!req.getBatches().isEmpty()) {
        List<IConsensusRequest> consensusRequests = new ArrayList<>();
        long currentSearchIndex = req.getBatches().get(0).getSearchIndex();
        for (TLogBatch batch : req.getBatches()) {
          IConsensusRequest request =
              batch.isFromWAL()
                  ? new MultiLeaderConsensusRequest(batch.data)
                  : new ByteBufferConsensusRequest(batch.data);
          // merge TLogBatch with same search index into one request
          if (batch.getSearchIndex() != currentSearchIndex) {
            requestsInThisBatch.add(
                impl.buildIndexedConsensusRequestForRemoteRequest(
                    currentSearchIndex, consensusRequests));
            consensusRequests = new ArrayList<>();
            currentSearchIndex = batch.getSearchIndex();
          }
          consensusRequests.add(request);
        }
        // write last request
        if (!consensusRequests.isEmpty()) {
          requestsInThisBatch.add(
              impl.buildIndexedConsensusRequestForRemoteRequest(
                  currentSearchIndex, consensusRequests));
        }
      }
      TSStatus writeStatus = impl.getStateMachine().write(requestsInThisBatch);
      logger.debug(
          "Execute TSyncLogReq for {} with result {}", req.consensusGroupId, writeStatus.subStatus);
      resultHandler.onComplete(new TSyncLogRes(writeStatus.subStatus));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  public void handleClientExit() {}
}
