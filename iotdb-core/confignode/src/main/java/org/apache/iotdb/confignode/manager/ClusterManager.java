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

import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateClusterIdPlan;
import org.apache.iotdb.confignode.persistence.ClusterInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ClusterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);

  private final IManager configManager;
  private final ClusterInfo clusterInfo;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public ClusterManager(IManager configManager, ClusterInfo clusterInfo) {
    this.configManager = configManager;
    this.clusterInfo = clusterInfo;
  }

  public void checkClusterId() {
    if (clusterInfo.getClusterId() != null) {
      LOGGER.info("clusterID: {}", clusterInfo.getClusterId());
      return;
    }
    generateClusterId();
  }

  public String getClusterId() {
    return clusterInfo.getClusterId();
  }

  private void generateClusterId() {
    String clusterId = String.valueOf(UUID.randomUUID());
    UpdateClusterIdPlan updateClusterIdPlan = new UpdateClusterIdPlan(clusterId);
    try {
      configManager.getConsensusManager().write(updateClusterIdPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
  }
}
