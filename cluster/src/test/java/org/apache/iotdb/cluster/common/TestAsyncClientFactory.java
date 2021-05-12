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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.client.async.AsyncClientFactory;
import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAsyncClientFactory extends AsyncClientFactory {

  private TProtocolFactory protocolFactory;
  private TAsyncClientManager clientManager;

  private AtomicInteger clientSerialNum = new AtomicInteger();

  public TestAsyncClientFactory() throws IOException {
    protocolFactory = new Factory();
    clientManager = new TAsyncClientManager();
  }

  @Override
  public AsyncClient getAsyncClient(Node node, AsyncClientPool pool) throws IOException {
    return new TestAsyncClient(
        protocolFactory,
        clientManager,
        TNonblockingSocketWrapper.wrap(node.getInternalIp(), node.getMetaPort()),
        clientSerialNum.getAndIncrement());
  }
}
