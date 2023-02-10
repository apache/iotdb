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

package org.apache.iotdb.consensus.iot.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestStateMachine implements IStateMachine, IStateMachine.EventApi {

  private static final Logger logger = LoggerFactory.getLogger(TestStateMachine.class);
  private final RequestSets requestSets = new RequestSets(ConcurrentHashMap.newKeySet());

  public Set<IndexedConsensusRequest> getRequestSet() {
    return requestSets.getRequestSet();
  }

  public Set<TestEntry> getData() {
    Set<TestEntry> data = new HashSet<>();
    requestSets
        .getRequestSet()
        .forEach(
            x -> {
              for (IConsensusRequest request : x.getRequests()) {
                data.add((TestEntry) request);
              }
            });
    return data;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    synchronized (requestSets) {
      if (request instanceof IndexedConsensusRequest) {
        writeOneRequest((IndexedConsensusRequest) request);
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else if (request instanceof DeserializedBatchIndexedConsensusRequest) {
        DeserializedBatchIndexedConsensusRequest batchIndexedConsensusRequest =
            (DeserializedBatchIndexedConsensusRequest) request;
        List<TSStatus> subStatus = new ArrayList<>();
        for (IConsensusRequest innerRequest : batchIndexedConsensusRequest.getInsertNodes()) {
          writeOneRequest((IndexedConsensusRequest) innerRequest);
          subStatus.add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
        }
        return new TSStatus().setSubStatus(subStatus);
      } else {
        logger.error("Unknown request: {}", request);
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
      }
    }
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    if (request instanceof BatchIndexedConsensusRequest) {
      BatchIndexedConsensusRequest consensusRequest = (BatchIndexedConsensusRequest) request;
      DeserializedBatchIndexedConsensusRequest result =
          new DeserializedBatchIndexedConsensusRequest(
              consensusRequest.getStartSyncIndex(),
              consensusRequest.getEndSyncIndex(),
              consensusRequest.getRequests().size());
      for (IndexedConsensusRequest r : consensusRequest.getRequests()) {
        result.add(r);
      }
      return result;
    } else {
      return request;
    }
  }

  private void writeOneRequest(IndexedConsensusRequest indexedConsensusRequest) {
    List<IConsensusRequest> transformedRequest = new ArrayList<>();
    for (IConsensusRequest innerRequest : indexedConsensusRequest.getRequests()) {
      ByteBuffer buffer = innerRequest.serializeToByteBuffer();
      transformedRequest.add(new TestEntry(buffer.getInt(), Peer.deserialize(buffer)));
    }
    requestSets.add(
        new IndexedConsensusRequest(indexedConsensusRequest.getSearchIndex(), transformedRequest),
        indexedConsensusRequest.getSearchIndex() != ConsensusReqReader.DEFAULT_SEARCH_INDEX);
  }

  @Override
  public synchronized DataSet read(IConsensusRequest request) {
    if (request instanceof GetConsensusReqReaderPlan) {
      return new FakeConsensusReqReader(requestSets);
    }
    return null;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return true;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {}
}
