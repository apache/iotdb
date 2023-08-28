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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.TimeUnit;

/** This class defines the configurations commonly used by the Thrift Client. */
public class ThriftClientProperty {

  private final TProtocolFactory protocolFactory;
  private final int connectionTimeoutMs;
  private final int selectorNumOfAsyncClientPool;
  private final boolean printLogWhenEncounterException;

  private ThriftClientProperty(
      TProtocolFactory protocolFactory,
      int connectionTimeoutMs,
      int selectorNumOfAsyncClientPool,
      boolean printLogWhenEncounterException) {
    this.protocolFactory = protocolFactory;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
    this.printLogWhenEncounterException = printLogWhenEncounterException;
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

  public boolean isPrintLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  public static class Builder {

    /** whether to use thrift compression. */
    private boolean rpcThriftCompressionEnabled = DefaultProperty.RPC_THRIFT_COMPRESSED_ENABLED;
    /** socket timeout for thrift client. */
    private int connectionTimeoutMs = DefaultProperty.CONNECTION_TIMEOUT_MS;
    /** number of selector threads for asynchronous thrift client in a clientManager. */
    private int selectorNumOfAsyncClientManager =
        DefaultProperty.SELECTOR_NUM_OF_ASYNC_CLIENT_MANAGER;

    /**
     * Whether to print logs when the client encounters exceptions. For example, logs are not
     * printed in the heartbeat client.
     */
    private boolean printLogWhenEncounterException =
        DefaultProperty.PRINT_LOG_WHEN_ENCOUNTER_EXCEPTION;

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

    public Builder setPrintLogWhenEncounterException(boolean printLogWhenEncounterException) {
      this.printLogWhenEncounterException = printLogWhenEncounterException;
      return this;
    }

    public ThriftClientProperty build() {
      return new ThriftClientProperty(
          rpcThriftCompressionEnabled
              ? new TCompactProtocol.Factory()
              : new TBinaryProtocol.Factory(),
          connectionTimeoutMs,
          selectorNumOfAsyncClientManager,
          printLogWhenEncounterException);
    }
  }

  private static class DefaultProperty {

    private DefaultProperty() {}

    public static final boolean RPC_THRIFT_COMPRESSED_ENABLED = false;
    public static final int CONNECTION_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(20);
    public static final int SELECTOR_NUM_OF_ASYNC_CLIENT_MANAGER = 1;
    public static final boolean PRINT_LOG_WHEN_ENCOUNTER_EXCEPTION = true;
  }
}
