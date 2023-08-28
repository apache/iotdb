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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.Optional;
import java.util.Properties;

public class ConsensusConfig {

  private final TEndPoint thisNodeEndPoint;
  private final int thisNodeId;
  private final String storageDir;
  private final TConsensusGroupType consensusGroupType;
  private final RatisConfig ratisConfig;
  private final IoTConsensusConfig iotConsensusConfig;
  private final RPCConfig rpcConfig;
  private final Properties properties;

  private ConsensusConfig(
      TEndPoint thisNode,
      int thisNodeId,
      String storageDir,
      TConsensusGroupType consensusGroupType,
      RatisConfig ratisConfig,
      IoTConsensusConfig iotConsensusConfig,
      Properties properties,
      TEndPoint clientRPCEndPoint) {
    this.thisNodeEndPoint = thisNode;
    this.thisNodeId = thisNodeId;
    this.storageDir = storageDir;
    this.consensusGroupType = consensusGroupType;
    this.ratisConfig = ratisConfig;
    this.iotConsensusConfig = iotConsensusConfig;
    // TODO-Raft: unify rpc config for all protocols
    this.rpcConfig =
        RPCConfig.newBuilder()
            .setSelectorNumOfClientManager(
                iotConsensusConfig.getRpc().getSelectorNumOfClientManager())
            .setRpcSelectorThreadNum(iotConsensusConfig.getRpc().getRpcSelectorThreadNum())
            .setRpcMinConcurrentClientNum(
                iotConsensusConfig.getRpc().getRpcMinConcurrentClientNum())
            .setRpcMaxConcurrentClientNum(
                iotConsensusConfig.getRpc().getRpcMaxConcurrentClientNum())
            .setThriftMaxFrameSize(iotConsensusConfig.getRpc().getThriftMaxFrameSize())
            .setCoreClientNumForEachNode(iotConsensusConfig.getRpc().getCoreClientNumForEachNode())
            .setMaxClientNumForEachNode(iotConsensusConfig.getRpc().getMaxClientNumForEachNode())
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

  public TConsensusGroupType getConsensusGroupType() {
    return consensusGroupType;
  }

  public RatisConfig getRatisConfig() {
    return ratisConfig;
  }

  public IoTConsensusConfig getIotConsensusConfig() {
    return iotConsensusConfig;
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
    private TConsensusGroupType consensusGroupType;
    private RatisConfig ratisConfig;
    private IoTConsensusConfig iotConsensusConfig;
    private Properties properties = new Properties();
    private TEndPoint clientRPCEndPoint;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          thisNodeId,
          storageDir,
          consensusGroupType,
          Optional.ofNullable(ratisConfig).orElseGet(() -> RatisConfig.newBuilder().build()),
          Optional.ofNullable(iotConsensusConfig)
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

    public Builder setConsensusGroupType(TConsensusGroupType groupType) {
      this.consensusGroupType = groupType;
      return this;
    }

    public Builder setRatisConfig(RatisConfig ratisConfig) {
      this.ratisConfig = ratisConfig;
      return this;
    }

    public Builder setIoTConsensusConfig(IoTConsensusConfig iotConsensusConfig) {
      this.iotConsensusConfig = iotConsensusConfig;
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
