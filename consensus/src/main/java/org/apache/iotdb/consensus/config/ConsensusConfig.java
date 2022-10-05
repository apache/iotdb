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

public class ConsensusConfig {

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final String storageDir;
  private final RatisConfig ratisConfig;
  private final MultiLeaderConfig multiLeaderConfig;

  private ConsensusConfig(
      TEndPoint thisNode,
      int thisNodeId,
      String storageDir,
      RatisConfig ratisConfig,
      MultiLeaderConfig multiLeaderConfig) {
    this.thisNode = thisNode;
    this.thisNodeId = thisNodeId;
    this.storageDir = storageDir;
    this.ratisConfig = ratisConfig;
    this.multiLeaderConfig = multiLeaderConfig;
  }

  public TEndPoint getThisNode() {
    return thisNode;
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

  public MultiLeaderConfig getMultiLeaderConfig() {
    return multiLeaderConfig;
  }

  public static ConsensusConfig.Builder newBuilder() {
    return new ConsensusConfig.Builder();
  }

  public static class Builder {

    private TEndPoint thisNode;
    private int thisNodeId;
    private String storageDir;
    private RatisConfig ratisConfig;
    private MultiLeaderConfig multiLeaderConfig;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          thisNodeId,
          storageDir,
          ratisConfig != null ? ratisConfig : RatisConfig.newBuilder().build(),
          multiLeaderConfig != null ? multiLeaderConfig : MultiLeaderConfig.newBuilder().build());
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

    public Builder setMultiLeaderConfig(MultiLeaderConfig multiLeaderConfig) {
      this.multiLeaderConfig = multiLeaderConfig;
      return this;
    }
  }
}
