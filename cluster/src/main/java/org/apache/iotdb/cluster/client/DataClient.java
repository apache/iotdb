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

package org.apache.iotdb.cluster.client;

import java.io.IOException;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.AsyncClient;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;

public class DataClient extends AsyncClient {

  private Node node;
  private ClientPool pool;

  public DataClient(TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager, Node node, ClientPool pool) throws IOException {
    // the difference of the two clients lies in the port
    super(protocolFactory, clientManager, new TNonblockingSocket(node.getIp(), node.getDataPort()
        , RaftServer.connectionTimeoutInMS));
    this.node = node;
    this.pool = pool;
  }

  @Override
  public void onComplete() {
    super.onComplete();
    // return itself to the pool if the job is done
    pool.putClient(node, this);
  }

  public static class Factory implements ClientFactory {
    private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
    public Factory(org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
    }
    public RaftService.AsyncClient getAsyncClient(Node node, ClientPool pool) throws IOException {
      return new DataClient(protocolFactory, new TAsyncClientManager(), node, pool);
    }
  }
}
