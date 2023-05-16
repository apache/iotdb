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

package org.apache.iotdb.consensus.config;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.Optional;
import java.util.Properties;

public class ConsensusConfig {

  private final TEndPoint thisNodeEndPoint;
  private final int thisNodeId;
  private final String storageDir;
  private final RatisConfig ratisConfig;
  private final IoTConsensusConfig ioTConsensusConfig;
  private final RPCConfig rpcConfig;
  private final Properties properties;

  private ConsensusConfig(
      TEndPoint thisNode,
      int thisNodeId,
      String storageDir,
      RatisConfig ratisConfig,
      IoTConsensusConfig ioTConsensusConfig,
      Properties properties,
      TEndPoint clientRPCEndPoint) {
    this.thisNodeEndPoint = thisNode;
    this.thisNodeId = thisNodeId;
    this.storageDir = storageDir;
    this.ratisConfig = ratisConfig;
    this.ioTConsensusConfig = ioTConsensusConfig;
    // TODO-Raft: unify rpc config for all protocols
    this.rpcConfig =
        RPCConfig.newBuilder()
            .setSelectorNumOfClientManager(
                ioTConsensusConfig.getRpc().getSelectorNumOfClientManager())
            .setRpcSelectorThreadNum(ioTConsensusConfig.getRpc().getRpcSelectorThreadNum())
            .setRpcMinConcurrentClientNum(
                ioTConsensusConfig.getRpc().getRpcMinConcurrentClientNum())
            .setRpcMaxConcurrentClientNum(
                ioTConsensusConfig.getRpc().getRpcMaxConcurrentClientNum())
            .setThriftMaxFrameSize(ioTConsensusConfig.getRpc().getThriftMaxFrameSize())
            .setCoreClientNumForEachNode(ioTConsensusConfig.getRpc().getCoreClientNumForEachNode())
            .setMaxClientNumForEachNode(ioTConsensusConfig.getRpc().getMaxClientNumForEachNode())
            .setClientRPCEndPoint(clientRPCEndPoint)
            .build();
    this.properties = properties;
  }

  public TEndPoint getThisNodeEndPoint() {
    return thisNodeEndPoint;
  }

  public int getThisNodeId() {
    return thisNodeId;
  }

  public String getStorageDir() {
    return storageDir;
  }

  public RatisConfig getRatisConfig() {
    return ratisConfig;
  }

  public IoTConsensusConfig getIoTConsensusConfig() {
    return ioTConsensusConfig;
  }

  public static ConsensusConfig.Builder newBuilder() {
    return new ConsensusConfig.Builder();
  }

  public RPCConfig getRPCConfig() {
    return rpcConfig;
  }

  public Properties getProperties() {
    return properties;
  }

  public static class Builder {

    private TEndPoint thisNode;
    private int thisNodeId;
    private String storageDir;
    private RatisConfig ratisConfig;
    private IoTConsensusConfig ioTConsensusConfig;
    private Properties properties = new Properties();
    private TEndPoint clientRPCEndPoint;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          thisNodeId,
          storageDir,
          Optional.ofNullable(ratisConfig).orElseGet(() -> RatisConfig.newBuilder().build()),
          Optional.ofNullable(ioTConsensusConfig)
              .orElseGet(() -> IoTConsensusConfig.newBuilder().build()),
          properties,
          clientRPCEndPoint);
    }

    public Builder setThisNode(TEndPoint thisNode) {
      this.thisNode = thisNode;
      return this;
    }

    public Builder setThisNodeId(int thisNodeId) {
      this.thisNodeId = thisNodeId;
      return this;
    }

    public Builder setStorageDir(String storageDir) {
      this.storageDir = storageDir;
      return this;
    }

    public Builder setRatisConfig(RatisConfig ratisConfig) {
      this.ratisConfig = ratisConfig;
      return this;
    }

    public Builder setIoTConsensusConfig(IoTConsensusConfig ioTConsensusConfig) {
      this.ioTConsensusConfig = ioTConsensusConfig;
      return this;
    }

    public Builder setProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public Builder setClientRPCEndPoint(TEndPoint clientRPCEndPoint) {
      this.clientRPCEndPoint = clientRPCEndPoint;
      return this;
    }
  }
}
