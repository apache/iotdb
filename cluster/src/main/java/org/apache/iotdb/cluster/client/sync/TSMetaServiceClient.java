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
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.net.SocketException;

public class TSMetaServiceClient extends TSMetaService.Client implements Closeable {
  Node target;
  SyncClientPool pool;

  /** @param prot this constructor just create a new instance, but do not open the connection */
  @TestOnly
  public TSMetaServiceClient(TProtocol prot) {
    super(prot);
  }
  /**
   * cerate a new client and open the connection
   *
   * @param protocolFactory
   * @param ip
   * @param port
   * @param timeoutInMS
   * @param target
   * @param pool
   * @throws TTransportException
   */
  public TSMetaServiceClient(
      TProtocolFactory protocolFactory,
      String ip,
      int port,
      int timeoutInMS,
      Node target,
      SyncClientPool pool)
      throws TTransportException {

    // the difference of the two clients lies in the port
    super(
        protocolFactory.getProtocol(
            RpcTransportFactory.INSTANCE.getTransport(ip, port, timeoutInMS)));
    this.target = target;
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

  /**
   * if the client does not open the connection, remove it.
   *
   * <p>if the client's connection is closed, create a new one.
   *
   * <p>if the client's connection is fine, put it back to the pool
   */
  public void putBack() {
    if (pool != null) {
      pool.putClient(target, this);
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

  public Node getTarget() {
    return target;
  }
}
