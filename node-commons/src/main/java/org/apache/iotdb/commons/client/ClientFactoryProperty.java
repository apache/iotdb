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

package org.apache.iotdb.commons.client;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.TimeUnit;

public class ClientFactoryProperty {

  private final TProtocolFactory protocolFactory;
  private final int connectionTimeoutMs;
  private final int selectorNumOfAsyncClientPool;

  public ClientFactoryProperty(
      TProtocolFactory protocolFactory, int connectionTimeoutMs, int selectorNumOfAsyncClientPool) {
    this.protocolFactory = protocolFactory;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
  }

  public TProtocolFactory getProtocolFactory() {
    return protocolFactory;
  }

  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public int getSelectorNumOfAsyncClientPool() {
    return selectorNumOfAsyncClientPool;
  }

  public static class Builder {
    // whether to use thrift compression
    private boolean rpcThriftCompressionEnabled = DefaultProperty.RPC_THRIFT_COMPRESSED_ENABLED;
    // socket timeout for thrift client
    private int connectionTimeoutMs = DefaultProperty.CONNECTION_TIMEOUT_MS;
    // number of selector threads for asynchronous thrift client in a clientManager
    private int selectorNumOfAsyncClientManager =
        DefaultProperty.SELECTOR_NUM_OF_ASYNC_CLIENT_MANAGER;

    public Builder setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
      this.rpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
      return this;
    }

    public Builder setConnectionTimeoutMs(int connectionTimeoutMs) {
      this.connectionTimeoutMs = connectionTimeoutMs;
      return this;
    }

    public Builder setSelectorNumOfAsyncClientManager(int selectorNumOfAsyncClientManager) {
      this.selectorNumOfAsyncClientManager = selectorNumOfAsyncClientManager;
      return this;
    }

    public ClientFactoryProperty build() {
      return new ClientFactoryProperty(
          rpcThriftCompressionEnabled
              ? new TCompactProtocol.Factory()
              : new TBinaryProtocol.Factory(),
          connectionTimeoutMs,
          selectorNumOfAsyncClientManager);
    }
  }

  public static class DefaultProperty {

    private DefaultProperty() {}

    public static final boolean RPC_THRIFT_COMPRESSED_ENABLED = false;
    public static final int CONNECTION_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(30);
    public static final int SELECTOR_NUM_OF_ASYNC_CLIENT_MANAGER = 1;
  }
}
