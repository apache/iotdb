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

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.time.Duration;

public class ClientManagerProperty<V> {

  private final GenericKeyedObjectPoolConfig<V> config;

  private final TProtocolFactory protocolFactory;
  private final int connectionTimeoutMs;
  private final int selectorNumOfAsyncClientPool;

  private ClientManagerProperty(
      GenericKeyedObjectPoolConfig<V> config,
      TProtocolFactory protocolFactory,
      int connectionTimeoutMs,
      int selectorNumOfAsyncClientPool) {
    this.config = config;
    this.protocolFactory = protocolFactory;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
  }

  public GenericKeyedObjectPoolConfig<V> getConfig() {
    return config;
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

  public static class Builder<V> {

    // when the number of the client to a single node exceeds maxTotalConnectionForEachNode, the
    // current thread will block waitClientTimeoutMS, ClientManager returns NULL if there are no
    // clients after the block time
    private long waitClientTimeoutMS = DefaultProperty.WAIT_CLIENT_TIMEOUT_MS;
    // the maximum number of clients that can be applied for a node
    private int maxTotalConnectionForEachNode = DefaultProperty.MAX_TOTAL_CONNECTION_FOR_EACH_NODE;
    // the maximum number of clients that can be idle for a node. When the number of idle clients on
    // a node exceeds this number, newly returned clients will be released
    private int maxIdleConnectionForEachNode = DefaultProperty.MAX_IDLE_CONNECTION_FOR_EACH_NODE;

    // whether to use thrift compression
    private boolean rpcThriftCompressionEnabled = DefaultProperty.RPC_THRIFT_COMPRESSED_ENABLED;
    // socket timeout for thrift client
    private int connectionTimeoutMs = DefaultProperty.CONNECTION_TIMEOUT_MS;
    // number of selector threads for asynchronous thrift client
    private int selectorNumOfAsyncClientPool = DefaultProperty.SELECTOR_NUM_OF_ASYNC_CLIENT_POOL;

    public Builder<V> setWaitClientTimeoutMS(long waitClientTimeoutMS) {
      this.waitClientTimeoutMS = waitClientTimeoutMS;
      return this;
    }

    public Builder<V> setMaxTotalConnectionForEachNode(int maxTotalConnectionForEachNode) {
      this.maxTotalConnectionForEachNode = maxTotalConnectionForEachNode;
      return this;
    }

    public Builder<V> setMaxIdleConnectionForEachNode(int maxIdleConnectionForEachNode) {
      this.maxIdleConnectionForEachNode = maxIdleConnectionForEachNode;
      return this;
    }

    public Builder<V> setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
      this.rpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
      return this;
    }

    public Builder<V> setConnectionTimeoutMs(int connectionTimeoutMs) {
      this.connectionTimeoutMs = connectionTimeoutMs;
      return this;
    }

    public Builder<V> setSelectorNumOfAsyncClientPool(int selectorNumOfAsyncClientPool) {
      this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
      return this;
    }

    public ClientManagerProperty<V> build() {
      GenericKeyedObjectPoolConfig<V> poolConfig = new GenericKeyedObjectPoolConfig<>();
      poolConfig.setMaxTotalPerKey(maxTotalConnectionForEachNode);
      poolConfig.setMaxIdlePerKey(maxIdleConnectionForEachNode);
      poolConfig.setMaxWait(Duration.ofMillis(waitClientTimeoutMS));
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestOnBorrow(true);
      return new ClientManagerProperty<>(
          poolConfig,
          rpcThriftCompressionEnabled
              ? new TCompactProtocol.Factory()
              : new TBinaryProtocol.Factory(),
          connectionTimeoutMs,
          selectorNumOfAsyncClientPool);
    }
  }

  public static class DefaultProperty {

    private DefaultProperty() {}

    // pool config
    public static final long WAIT_CLIENT_TIMEOUT_MS = 10_000;
    public static final int MAX_TOTAL_CONNECTION_FOR_EACH_NODE = 100;
    public static final int MAX_IDLE_CONNECTION_FOR_EACH_NODE = 100;

    // thrift client config
    public static final boolean RPC_THRIFT_COMPRESSED_ENABLED = false;
    public static final int CONNECTION_TIMEOUT_MS = 20_000;
    public static final int SELECTOR_NUM_OF_ASYNC_CLIENT_POOL = 1;
  }
}
