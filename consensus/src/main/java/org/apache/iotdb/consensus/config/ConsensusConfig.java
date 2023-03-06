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

public class ConsensusConfig {

  private final TEndPoint thisNodeEndPoint;
  private final int thisNodeId;
  private final String storageDir;
  private final TConsensusGroupType consensusGroupType;
  private final RatisConfig ratisConfig;
  private final IoTConsensusConfig ioTConsensusConfig;

  private ConsensusConfig(
      TEndPoint thisNode,
      int thisNodeId,
      String storageDir,
      TConsensusGroupType consensusGroupType,
      RatisConfig ratisConfig,
      IoTConsensusConfig ioTConsensusConfig) {
    this.thisNodeEndPoint = thisNode;
    this.thisNodeId = thisNodeId;
    this.storageDir = storageDir;
    this.consensusGroupType = consensusGroupType;
    this.ratisConfig = ratisConfig;
    this.ioTConsensusConfig = ioTConsensusConfig;
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

  public IoTConsensusConfig getIoTConsensusConfig() {
    return ioTConsensusConfig;
  }

  public static ConsensusConfig.Builder newBuilder() {
    return new ConsensusConfig.Builder();
  }

  public static class Builder {

    private TEndPoint thisNode;
    private int thisNodeId;
    private String storageDir;
    private TConsensusGroupType consensusGroupType;
    private RatisConfig ratisConfig;
    private IoTConsensusConfig ioTConsensusConfig;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          thisNodeId,
          storageDir,
          consensusGroupType,
          Optional.ofNullable(ratisConfig).orElseGet(() -> RatisConfig.newBuilder().build()),
          Optional.ofNullable(ioTConsensusConfig)
              .orElseGet(() -> IoTConsensusConfig.newBuilder().build()));
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

    public Builder setConsensusGroupType(TConsensusGroupType consensusGroupType) {
      this.consensusGroupType = consensusGroupType;
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
  }
}
