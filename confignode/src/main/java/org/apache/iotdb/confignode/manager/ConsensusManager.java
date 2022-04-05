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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.commons.hash.DeviceGroupHashExecutor;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** ConsensusManager maintains consensus class, request will redirect to consensus layer */
public class ConsensusManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusManager.class);
  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private IConsensus consensusImpl;

  private ConsensusGroupId consensusGroupId;

  private DeviceGroupHashExecutor hashExecutor;

  public ConsensusManager() throws IOException {
    setHashExecutor();
    setConsensusLayer();
  }

  public void close() throws IOException {
    consensusImpl.stop();
  }

  /** Build DeviceGroupHashExecutor */
  private void setHashExecutor() {
    try {
      Class<?> executor = Class.forName(conf.getDeviceGroupHashExecutorClass());
      Constructor<?> executorConstructor = executor.getConstructor(int.class);
      hashExecutor =
          (DeviceGroupHashExecutor) executorConstructor.newInstance(conf.getDeviceGroupCount());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      LOGGER.error(
          "Couldn't Constructor DeviceGroupHashExecutor class: {}",
          conf.getDeviceGroupHashExecutorClass(),
          e);
      hashExecutor = null;
    }
  }

  public int getDeviceGroupID(String device) {
    return hashExecutor.getDeviceGroupID(device);
  }

  /** Build ConfigNodeGroup ConsensusLayer */
  private void setConsensusLayer() throws IOException {
    // There is only one ConfigNodeGroup
    consensusGroupId = new ConsensusGroupId(GroupType.PartitionRegion, 0);

    // Ratis consensus local implement
    consensusImpl =
        IConsensusFactory.getConsensusImpl(
                conf.getConfigNodeConsensusProtocolClass(),
                new Endpoint(conf.getRpcAddress(), conf.getInternalPort()),
                new File(conf.getConsensusDir()),
                gid -> new PartitionRegionStateMachine())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            IConsensusFactory.CONSTRUCT_FAILED_MSG,
                            conf.getConfigNodeConsensusProtocolClass())));
    consensusImpl.start();

    // Build ratis group from user properties
    LOGGER.info(
        "Set ConfigNode consensus group {}...",
        Arrays.toString(conf.getConfigNodeGroupAddressList()));
    List<Peer> peerList = new ArrayList<>();
    for (Endpoint endpoint : conf.getConfigNodeGroupAddressList()) {
      peerList.add(new Peer(consensusGroupId, endpoint));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusWriteResponse write(PhysicalPlan plan) {
    return consensusImpl.write(consensusGroupId, plan);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusReadResponse read(PhysicalPlan plan) {
    return consensusImpl.read(consensusGroupId, plan);
  }

  // TODO: Interfaces for LoadBalancer control
}
