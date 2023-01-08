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

package org.apache.iotdb.commons.client.property;

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

    /**
     * when the number of the client to a single node exceeds maxClientNumForEachNode, the thread
     * for applying for a client will be blocked for waitClientTimeoutMs, then ClientManager will
     * throw ClientManagerException if there are no clients after the block time.
     */
    private long waitClientTimeoutMs = DefaultProperty.WAIT_CLIENT_TIMEOUT_MS;

    /** the maximum number of clients that can be allocated for a node. */
    private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;
    /**
     * the maximum number of clients that can be idle for a node. When the number of idle clients on
     * a node exceeds this number, newly returned clients will be released.
     */
    private int coreClientNumForEachNode = DefaultProperty.CORE_CLIENT_NUM_FOR_EACH_NODE;

    public Builder<V> setWaitClientTimeoutMs(long waitClientTimeoutMs) {
      this.waitClientTimeoutMs = waitClientTimeoutMs;
      return this;
    }

    public Builder<V> setMaxClientNumForEachNode(int maxClientNumForEachNode) {
      this.maxClientNumForEachNode = maxClientNumForEachNode;
      return this;
    }

    public Builder<V> setCoreClientNumForEachNode(int coreClientNumForEachNode) {
      this.coreClientNumForEachNode = coreClientNumForEachNode;
      return this;
    }

    public ClientPoolProperty<V> build() {
      GenericKeyedObjectPoolConfig<V> poolConfig = new GenericKeyedObjectPoolConfig<>();
      poolConfig.setMaxTotalPerKey(maxClientNumForEachNode);
      poolConfig.setMaxIdlePerKey(coreClientNumForEachNode);
      poolConfig.setMaxWait(Duration.ofMillis(waitClientTimeoutMs));
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestOnBorrow(true);
      return new ClientPoolProperty<>(poolConfig);
    }
  }

  public static class DefaultProperty {

    private DefaultProperty() {}

    public static final long WAIT_CLIENT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final int MAX_CLIENT_NUM_FOR_EACH_NODE = 300;
    public static final int CORE_CLIENT_NUM_FOR_EACH_NODE = 200;
  }
}
