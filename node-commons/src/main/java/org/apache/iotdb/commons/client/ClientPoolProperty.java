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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ClientPoolProperty<V> {

  private final GenericKeyedObjectPoolConfig<V> config;

  private ClientPoolProperty(GenericKeyedObjectPoolConfig<V> config) {
    this.config = config;
  }

  public GenericKeyedObjectPoolConfig<V> getConfig() {
    return config;
  }

  public static class Builder<V> {
    // when the number of the client to a single node exceeds maxTotalConnectionForEachNode, the
    // current thread will block waitClientTimeoutMS, ClientManager throws ClientManagerException if
    // there are no clients after the block time
    private long waitClientTimeoutMS = DefaultProperty.WAIT_CLIENT_TIMEOUT_MS;
    // the maximum number of clients that can be allocated for a node
    private int maxTotalClientForEachNode = DefaultProperty.MAX_TOTAL_CLIENT_FOR_EACH_NODE;
    // the maximum number of clients that can be idle for a node. When the number of idle clients on
    // a node exceeds this number, newly returned clients will be released
    private int maxIdleClientForEachNode = DefaultProperty.MAX_IDLE_CLIENT_FOR_EACH_NODE;

    public Builder<V> setWaitClientTimeoutMS(long waitClientTimeoutMS) {
      this.waitClientTimeoutMS = waitClientTimeoutMS;
      return this;
    }

    public Builder<V> setMaxTotalClientForEachNode(int maxTotalClientForEachNode) {
      this.maxTotalClientForEachNode = maxTotalClientForEachNode;
      return this;
    }

    public Builder<V> setMaxIdleClientForEachNode(int maxIdleClientForEachNode) {
      this.maxIdleClientForEachNode = maxIdleClientForEachNode;
      return this;
    }

    public ClientPoolProperty<V> build() {
      GenericKeyedObjectPoolConfig<V> poolConfig = new GenericKeyedObjectPoolConfig<>();
      poolConfig.setMaxTotalPerKey(maxTotalClientForEachNode);
      poolConfig.setMaxIdlePerKey(maxIdleClientForEachNode);
      poolConfig.setMaxWait(Duration.ofMillis(waitClientTimeoutMS));
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestOnBorrow(true);
      return new ClientPoolProperty<>(poolConfig);
    }
  }

  public static class DefaultProperty {

    private DefaultProperty() {}

    public static final long WAIT_CLIENT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final int MAX_TOTAL_CLIENT_FOR_EACH_NODE = 100;
    public static final int MAX_IDLE_CLIENT_FOR_EACH_NODE = 100;
  }
}
