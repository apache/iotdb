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

package org.apache.iotdb.db.pipe.core.connector.impl.iotdb;

import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class PipeClientRPCServiceClient extends IClientRPCService.Client
    implements ThriftClient, AutoCloseable {
  public PipeClientRPCServiceClient(ThriftClientProperty property, String ipAddress, int port)
      throws TTransportException {
    super(
        property
            .getProtocolFactory()
            .getProtocol(
                RpcTransportFactory.INSTANCE.getTransport(
                    new TSocket(
                        TConfigurationConst.defaultTConfiguration,
                        ipAddress,
                        port,
                        property.getConnectionTimeoutMs()))));
    getInputProtocol().getTransport().open();
  }

  @Override
  public void close() throws Exception {
    invalidate();
  }

  @Override
  public void invalidate() {
    if (getInputProtocol().getTransport().isOpen()) {
      getInputProtocol().getTransport().close();
    }
  }

  @Override
  public void invalidateAll() {
    invalidate();
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return true;
  }
}
