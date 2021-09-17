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

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.Client;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.net.SocketException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else or there may be conflicts.
 */
// the two classes does not share a common parent and Java does not allow multiple extension
@SuppressWarnings("common-java:DuplicatedBlocks")
public class SyncDataClient extends Client implements Closeable {

  Node node;
  SyncClientPool pool;

  public SyncDataClient(TProtocol prot) {
    super(prot);
  }

  public SyncDataClient(TProtocolFactory protocolFactory, Node node, SyncClientPool pool)
      throws TTransportException {
    // the difference of the two clients lies in the port
    super(
        protocolFactory.getProtocol(
            RpcTransportFactory.INSTANCE.getTransport(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    node.getInternalIp(),
                    node.getDataPort(),
                    RaftServer.getConnectionTimeoutInMS()))));
    this.node = node;
    this.pool = pool;
    getInputProtocol().getTransport().open();
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @TestOnly
  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public void putBack() {
    if (pool != null) {
      pool.putClient(node, this);
    } else {
      TProtocol inputProtocol = getInputProtocol();
      if (inputProtocol != null) {
        inputProtocol.getTransport().close();
      }
    }
  }

  /** put the client to pool, instead of close client. */
  @Override
  public void close() {
    putBack();
  }

  public static class FactorySync implements SyncClientFactory {

    private TProtocolFactory protocolFactory;

    public FactorySync(TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
    }

    @Override
    public SyncDataClient getSyncClient(Node node, SyncClientPool pool) throws TTransportException {
      return new SyncDataClient(protocolFactory, node, pool);
    }
  }

  @Override
  public String toString() {
    return "DataClient{" + "node=" + node + '}';
  }

  public Node getNode() {
    return node;
  }
}
