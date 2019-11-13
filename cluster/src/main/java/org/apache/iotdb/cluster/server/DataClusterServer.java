/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.thrift.async.AsyncMethodCallback;

public class DataClusterServer extends RaftServer implements TSDataService.AsyncIface {

  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  AsyncProcessor getProcessor() {
    return new AsyncProcessor(this);
  }
}
