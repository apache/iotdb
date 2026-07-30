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
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;

import java.util.List;
import java.util.Optional;

public class ConsensusConfig {

  private final TEndPoint thisNodeEndPoint;
  private final int thisNodeId;
  private final String storageDir;
  private final List<String> recvSnapshotDirs;
  private final TConsensusGroupType consensusGroupType;
  private final RatisConfig ratisConfig;
  private final IoTConsensusConfig iotConsensusConfig;
  private final IoTConsensusV2Config iotConsensusV2Config;
  private final DirectoryStrategyType directoryStrategyType;

  private ConsensusConfig(
      TEndPoint thisNode,
      int thisNodeId,
      String storageDir,
      List<String> recvSnapshotDirs,
      TConsensusGroupType consensusGroupType,
      RatisConfig ratisConfig,
      IoTConsensusConfig iotConsensusConfig,
      IoTConsensusV2Config iotConsensusV2Config,
      DirectoryStrategyType directoryStrategyType) {
    this.thisNodeEndPoint = thisNode;
    this.thisNodeId = thisNodeId;
    this.storageDir = storageDir;
    this.recvSnapshotDirs = recvSnapshotDirs;
    this.consensusGroupType = consensusGroupType;
    this.ratisConfig = ratisConfig;
    this.iotConsensusConfig = iotConsensusConfig;
    this.iotConsensusV2Config = iotConsensusV2Config;
    this.directoryStrategyType = directoryStrategyType;
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

  public List<String> getRecvSnapshotDirs() {
    return recvSnapshotDirs;
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

  public IoTConsensusV2Config getIoTConsensusV2Config() {
    return iotConsensusV2Config;
  }

  public DirectoryStrategyType getDirectoryStrategyType() {
    return directoryStrategyType;
  }

  public static ConsensusConfig.Builder newBuilder() {
    return new ConsensusConfig.Builder();
  }

  public static class Builder {

    private TEndPoint thisNode;
    private int thisNodeId;
    private String storageDir;
    private List<String> recvSnapshotDirs;
    private TConsensusGroupType consensusGroupType;
    private RatisConfig ratisConfig;
    private IoTConsensusConfig iotConsensusConfig;
    private IoTConsensusV2Config iotConsensusV2Config;
    private DirectoryStrategyType directoryStrategyType =
        DirectoryStrategyType.MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          thisNodeId,
          storageDir,
          recvSnapshotDirs,
          consensusGroupType,
          Optional.ofNullable(ratisConfig).orElseGet(() -> RatisConfig.newBuilder().build()),
          Optional.ofNullable(iotConsensusConfig)
              .orElseGet(() -> IoTConsensusConfig.newBuilder().build()),
          Optional.ofNullable(iotConsensusV2Config)
              .orElseGet(() -> IoTConsensusV2Config.newBuilder().build()),
          directoryStrategyType);
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

    public Builder setRecvSnapshotDirs(List<String> recvSnapshotDirs) {
      this.recvSnapshotDirs = recvSnapshotDirs;
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

    public Builder setIoTConsensusV2Config(IoTConsensusV2Config iotConsensusV2Config) {
      this.iotConsensusV2Config = iotConsensusV2Config;
      return this;
    }

    public Builder setDirectoryStrategyType(DirectoryStrategyType directoryStrategyType) {
      this.directoryStrategyType = directoryStrategyType;
      return this;
    }
  }
}
