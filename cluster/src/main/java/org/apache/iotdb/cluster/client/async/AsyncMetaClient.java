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

package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else or there may be conflicts.
 */
// the two classes does not share a common parent and Java does not allow multiple extension
@SuppressWarnings("common-java:DuplicatedBlocks")
public class AsyncMetaClient extends AsyncClient {

  private static final Logger logger = LoggerFactory.getLogger(AsyncMetaClient.class);
  Node node;
  AsyncClientPool pool;

  public AsyncMetaClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport) {
    super(protocolFactory, clientManager, transport);
  }

  public AsyncMetaClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      Node node,
      AsyncClientPool pool)
      throws IOException {
    // the difference of the two clients lies in the port
    super(
        protocolFactory,
        clientManager,
        TNonblockingSocketWrapper.wrap(
            node.getInternalIp(), node.getMetaPort(), RaftServer.getConnectionTimeoutInMS()));
    this.node = node;
    this.pool = pool;
  }

  @Override
  public void onComplete() {
    super.onComplete();
    // return itself to the pool if the job is done
    if (pool != null) {
      pool.putClient(node, this);
      pool.onComplete(node);
    }
  }

  @SuppressWarnings("squid:S1135")
  @Override
  public void onError(Exception e) {
    super.onError(e);
    pool.recreateClient(node);
    // TODO: if e instance of network failure
    pool.onError(node);
  }

  public static class FactoryAsync extends AsyncClientFactory {

    public FactoryAsync(org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
    }

    @Override
    public RaftService.AsyncClient getAsyncClient(Node node, AsyncClientPool pool)
        throws IOException {
      TAsyncClientManager manager = managers[clientCnt.incrementAndGet() % managers.length];
      manager = manager == null ? new TAsyncClientManager() : manager;
      return new AsyncMetaClient(protocolFactory, manager, node, pool);
    }
  }

  @Override
  public String toString() {
    return "MetaClient{" + "node=" + node + '}';
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public Node getNode() {
    return node;
  }

  public boolean isReady() {
    if (___currentMethod != null) {
      logger.warn(
          "Client {} is running {} and will timeout at {}",
          hashCode(),
          ___currentMethod,
          new Date(___currentMethod.getTimeoutTimestamp()));
    }
    return ___currentMethod == null;
  }

  TAsyncMethodCall<Object> getCurrMethod() {
    return ___currentMethod;
  }
}
