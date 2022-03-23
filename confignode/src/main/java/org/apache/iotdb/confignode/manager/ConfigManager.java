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

import org.apache.iotdb.commons.hash.DeviceGroupHashExecutor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.GroupType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.standalone.StandAloneConsensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

/**
 * ConfigManager maintains consistency between PartitionTables in the ConfigNodeGroup. Expose the
 * query interface for the PartitionTable
 */
public class ConfigManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  private IConsensus consensusImpl;
  private ConsensusGroupId consensusGroupId;

  private DeviceGroupHashExecutor hashExecutor;

  @TestOnly
  public ConfigManager(String hashExecutorClass, int deviceGroupCount) {
    setHashExecutor(hashExecutorClass, deviceGroupCount);
  }

  public ConfigManager() {
    ConfigNodeConf config = ConfigNodeDescriptor.getInstance().getConf();

    setHashExecutor(config.getDeviceGroupHashExecutorClass(), config.getDeviceGroupCount());
    setConsensusLayer(config);
  }

  /** Build DeviceGroupHashExecutor */
  private void setHashExecutor(String hashExecutorClass, int deviceGroupCount) {
    try {
      Class<?> executor = Class.forName(hashExecutorClass);
      Constructor<?> executorConstructor = executor.getConstructor(int.class);
      hashExecutor = (DeviceGroupHashExecutor) executorConstructor.newInstance(deviceGroupCount);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      LOGGER.error("Couldn't Constructor DeviceGroupHashExecutor class: {}", hashExecutorClass, e);
      hashExecutor = null;
    }
  }

  public int getDeviceGroupID(String device) {
    return hashExecutor.getDeviceGroupID(device);
  }

  /** Build ConfigNodeGroup ConsensusLayer */
  private void setConsensusLayer(ConfigNodeConf config) {
    // TODO: Support other consensus protocol
    this.consensusImpl = new StandAloneConsensus(id -> new PartitionRegionStateMachine());
    this.consensusImpl.start();

    this.consensusGroupId = new ConsensusGroupId(GroupType.PartitionRegion, 0);
    this.consensusImpl.addConsensusGroup(
        this.consensusGroupId,
        Collections.singletonList(
            new Peer(
                this.consensusGroupId,
                new Endpoint(config.getRpcAddress(), config.getInternalPort()))));
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusWriteResponse write(PhysicalPlan plan) {
    return this.consensusImpl.write(this.consensusGroupId, plan);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusReadResponse read(PhysicalPlan plan) {
    return this.consensusImpl.read(this.consensusGroupId, plan);
  }

  // TODO: Interfaces for LoadBalancer control
}
