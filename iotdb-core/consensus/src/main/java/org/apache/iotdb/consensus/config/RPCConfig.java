/*
 * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.consensus.config;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ClientPoolProperty.DefaultProperty;

import java.util.concurrent.TimeUnit;

public class RPCConfig {
  private int rpcSelectorThreadNum;
  private int rpcMinConcurrentClientNum;
  private int rpcMaxConcurrentClientNum;
  private int thriftServerAwaitTimeForStopService;
  private boolean isRpcThriftCompressionEnabled;
  private int selectorNumOfClientManager;
  private int connectionTimeoutInMs;
  private int thriftMaxFrameSize;
  private int coreClientNumForEachNode;
  private int maxClientNumForEachNode;
  private TEndPoint clientRPCEndPoint;

  private RPCConfig(
      int rpcSelectorThreadNum,
      int rpcMinConcurrentClientNum,
      int rpcMaxConcurrentClientNum,
      int thriftServerAwaitTimeForStopService,
      boolean isRpcThriftCompressionEnabled,
      int selectorNumOfClientManager,
      int connectionTimeoutInMs,
      int thriftMaxFrameSize,
      int coreClientNumForEachNode,
      int maxClientNumForEachNode,
      TEndPoint clientRPCEndPoint) {
    this.rpcSelectorThreadNum = rpcSelectorThreadNum;
    this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
    this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
    this.selectorNumOfClientManager = selectorNumOfClientManager;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.coreClientNumForEachNode = coreClientNumForEachNode;
    this.maxClientNumForEachNode = maxClientNumForEachNode;
    this.clientRPCEndPoint = clientRPCEndPoint;
  }

  public int getRpcSelectorThreadNum() {
    return rpcSelectorThreadNum;
  }

  public int getRpcMinConcurrentClientNum() {
    return rpcMinConcurrentClientNum;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public int getConnectionTimeoutInMs() {
    return connectionTimeoutInMs;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public static RPCConfig.Builder newBuilder() {
    return new RPCConfig.Builder();
  }

  public static class Builder {
    private int rpcSelectorThreadNum = 1;
    private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();
    private int rpcMaxConcurrentClientNum = 65535;
    private int thriftServerAwaitTimeForStopService = 60;
    private boolean isRpcThriftCompressionEnabled = false;
    private int selectorNumOfClientManager = 1;
    private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(20);
    private int thriftMaxFrameSize = 536870912;

    private int coreClientNumForEachNode = DefaultProperty.CORE_CLIENT_NUM_FOR_EACH_NODE;

    private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;
    private TEndPoint clientRPCEndPoint;

    public RPCConfig.Builder setRpcSelectorThreadNum(int rpcSelectorThreadNum) {
      this.rpcSelectorThreadNum = rpcSelectorThreadNum;
      return this;
    }

    public RPCConfig.Builder setRpcMinConcurrentClientNum(int rpcMinConcurrentClientNum) {
      this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
      return this;
    }

    public RPCConfig.Builder setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
      this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
      return this;
    }

    public RPCConfig.Builder setThriftServerAwaitTimeForStopService(
        int thriftServerAwaitTimeForStopService) {
      this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
      return this;
    }

    public RPCConfig.Builder setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
      isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
      return this;
    }

    public RPCConfig.Builder setSelectorNumOfClientManager(int selectorNumOfClientManager) {
      this.selectorNumOfClientManager = selectorNumOfClientManager;
      return this;
    }

    public RPCConfig.Builder setConnectionTimeoutInMs(int connectionTimeoutInMs) {
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      return this;
    }

    public RPCConfig.Builder setThriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public RPCConfig.Builder setCoreClientNumForEachNode(int coreClientNumForEachNode) {
      this.coreClientNumForEachNode = coreClientNumForEachNode;
      return this;
    }

    public RPCConfig.Builder setMaxClientNumForEachNode(int maxClientNumForEachNode) {
      this.maxClientNumForEachNode = maxClientNumForEachNode;
      return this;
    }

    public RPCConfig.Builder setClientRPCEndPoint(TEndPoint rpcEndPoint) {
      this.clientRPCEndPoint = rpcEndPoint;
      return this;
    }

    public RPCConfig build() {
      return new RPCConfig(
          rpcSelectorThreadNum,
          rpcMinConcurrentClientNum,
          rpcMaxConcurrentClientNum,
          thriftServerAwaitTimeForStopService,
          isRpcThriftCompressionEnabled,
          selectorNumOfClientManager,
          connectionTimeoutInMs,
          thriftMaxFrameSize,
          coreClientNumForEachNode,
          maxClientNumForEachNode,
          clientRPCEndPoint);
    }
  }

  public int getCoreClientNumForEachNode() {
    return coreClientNumForEachNode;
  }

  public void setCoreClientNumForEachNode(int coreClientNumForEachNode) {
    this.coreClientNumForEachNode = coreClientNumForEachNode;
  }

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public void setMaxClientNumForEachNode(int maxClientNumForEachNode) {
    this.maxClientNumForEachNode = maxClientNumForEachNode;
  }

  public TEndPoint getClientRPCEndPoint() {
    return clientRPCEndPoint;
  }
}
