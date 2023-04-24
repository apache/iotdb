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

package org.apache.iotdb.consensus.raft.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestStateMachine implements IStateMachine, IStateMachine.EventApi {

  private static final Logger logger = LoggerFactory.getLogger(TestStateMachine.class);
  private Set<IConsensusRequest> requestSet = new HashSet<>();
  private static Map<String, Set<IConsensusRequest>> dummySnapshot = new ConcurrentHashMap<>();

  public Set<IConsensusRequest> getRequestSet() {
    return requestSet;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    if (request instanceof ByteBufferConsensusRequest) {
      request = deserializeRequest(request);
    }
    synchronized (requestSet) {
      requestSet.add(request);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    if (request instanceof TestEntry) {
      return request;
    }
    ByteBufferConsensusRequest byteBufferConsensusRequest = (ByteBufferConsensusRequest) request;
    ByteBuffer byteBuffer = byteBufferConsensusRequest.serializeToByteBuffer();
    byteBuffer.rewind();
    return TestEntry.deserialize(byteBuffer);
  }

  @Override
  public synchronized DataSet read(IConsensusRequest request) {
    return new FakeDataSet(requestSet);
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    dummySnapshot.put(snapshotDir.getAbsolutePath(), requestSet);
    return true;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    requestSet = dummySnapshot.get(latestSnapshotRootDir.getAbsolutePath());
  }
}
