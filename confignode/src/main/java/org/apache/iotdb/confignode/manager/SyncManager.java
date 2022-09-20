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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.persistence.sync.ClusterSyncInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncManager.class);

  private final IManager configManager;
  private final ClusterSyncInfo clusterSyncInfo;

  public SyncManager(IManager configManager, ClusterSyncInfo clusterSyncInfo) {
    this.configManager = configManager;
    this.clusterSyncInfo = clusterSyncInfo;
  }

  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  public TSStatus createPipeSink(CreatePipeSinkPlan plan) {
    return null;
  }

  public TSStatus dropPipeSink(DropPipeSinkPlan plan) {
    return null;
  }

  // endregion

  // ======================================================
  // region Implement of PipeS
  // ======================================================

  // TODO....

  // endregion

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
