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

package org.apache.iotdb.consensus.multileader.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestStateMachine implements IStateMachine, IStateMachine.EventApi {

  private final RequestSets requestSets = new RequestSets(ConcurrentHashMap.newKeySet());

  public Set<IndexedConsensusRequest> getRequestSet() {
    return requestSets.getRequestSet();
  }

  public Set<TestEntry> getData() {
    Set<TestEntry> data = new HashSet<>();
    requestSets.getRequestSet().forEach(x -> data.add((TestEntry) x.getRequest()));
    return data;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    synchronized (requestSets) {
      IConsensusRequest innerRequest = ((IndexedConsensusRequest) request).getRequest();
      if (innerRequest instanceof ByteBufferConsensusRequest) {
        ByteBuffer buffer = innerRequest.serializeToByteBuffer();
        requestSets.add(
            new IndexedConsensusRequest(
                ((IndexedConsensusRequest) request).getSearchIndex(),
                -1,
                new TestEntry(buffer.getInt(), Peer.deserialize(buffer))),
            false);
      } else {
        requestSets.add(((IndexedConsensusRequest) request), true);
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
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
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {}
}
