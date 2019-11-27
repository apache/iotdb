/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;

public class MetaClient extends AsyncClient {

  private MetaClient(TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport) {
    super(protocolFactory, clientManager, transport);
  }

  @Override
  protected void onComplete() {
    super.onComplete();
    ___transport.close();
  }

  public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<MetaClient> {
    private org.apache.thrift.async.TAsyncClientManager clientManager;
    private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
    public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
      this.clientManager = clientManager;
      this.protocolFactory = protocolFactory;
    }
    public MetaClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
      return new MetaClient(protocolFactory, clientManager, transport);
    }
  }
}
