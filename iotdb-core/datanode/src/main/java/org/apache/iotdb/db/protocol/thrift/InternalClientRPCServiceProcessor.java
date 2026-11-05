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

package org.apache.iotdb.db.protocol.thrift;

import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.impl.IClientRPCServiceWithHandler;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.Socket;

/** Registers a client session only for connections using the multiplexed client RPC service. */
public class InternalClientRPCServiceProcessor extends ProcessorWithMetrics {

  public InternalClientRPCServiceProcessor(IClientRPCServiceWithHandler iface) {
    super(iface);
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    registerSessionIfNecessary(out);
    super.process(in, out);
  }

  private void registerSessionIfNecessary(TProtocol protocol) {
    if (SessionManager.getInstance().getCurrSession() != null) {
      return;
    }

    TTransport socketTransport = ((TElasticFramedTransport) protocol.getTransport()).getSocket();
    Socket socket = ((TSocket) socketTransport).getSocket();
    SessionManager.getInstance().registerSession(new ClientSession(socket));
  }
}
