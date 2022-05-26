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
package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ConfigNode implements ConfigNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);
  private static final int onlineConfigNodeNum = 1;

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private final RegisterManager registerManager = new RegisterManager();

  private final ConfigNodeRPCService configNodeRPCService;
  private final ConfigNodeRPCServiceProcessor configNodeRPCServiceProcessor;

  private ConfigManager configManager;

  private ConfigNode() {
    // Init ConfigManager
    try {
      this.configManager = new ConfigManager();
    } catch (IOException e) {
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      try {
        stop();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
      System.exit(-1);
    }

    // Init RPC service
    this.configNodeRPCService = new ConfigNodeRPCService();
    this.configNodeRPCServiceProcessor = new ConfigNodeRPCServiceProcessor(configManager);
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  /** Register services */
  private void setUp() throws StartupException, IOException {
    LOGGER.info("Setting up {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    configNodeRPCService.initSyncedServiceImpl(configNodeRPCServiceProcessor);
    registerManager.register(configNodeRPCService);
    LOGGER.info("Init rpc server success");
  }

  public void active() {
    try {
      setUp();
    } catch (StartupException | IOException e) {
      LOGGER.error("Meet error while starting up.", e);
      try {
        deactivate();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
      return;
    }

    LOGGER.info(
        "{} has successfully started and joined the cluster.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void deactivate() throws IOException {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    configManager.close();
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void stop() throws IOException {
    deactivate();
  }

  public void doRemoveNode() throws IOException {
    LOGGER.info("Starting to remove {}...", ConfigNodeConstant.GLOBAL_NAME);
    List<TConfigNodeLocation> onlineConfigNodes =
        configManager.getNodeManager().getOnlineConfigNodes();
    ConsensusGroupId consensusGroupId = configManager.getConsensusManager().getConsensusGroupId();
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
    Peer leader = configManager.getConsensusManager().getLeader(consensusGroupId);
    TEndPoint removeConfigNode = new TEndPoint(conf.getRpcAddress(), conf.getRpcPort());
    if (checkConfigNodeDuplicateNum(onlineConfigNodes)) {
      LOGGER.error("Remove ConfigNode failed, because there is only one ConfigNode in Cluster!");
      return;
    }

    if (leader.getEndpoint().equals(removeConfigNode)) {
      LOGGER.info("The remove ConfigNode is leader, need to transfer leader.");
      leader = transferLeader(onlineConfigNodes, consensusGroupId);
    }

    TSStatus status =
        SyncConfigNodeClientPool.getInstance()
            .removeConfigNode(
                leader.getEndpoint(),
                new TConfigNodeLocation(
                    removeConfigNode,
                    new TEndPoint(conf.getRpcAddress(), conf.getConsensusPort())));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(status.getMessage());
      throw new IOException("Remove ConfigNode failed:");
    }
    LOGGER.info("{} is removed.", ConfigNodeConstant.GLOBAL_NAME);
  }

  private Peer transferLeader(
      List<TConfigNodeLocation> onlineConfigNodes, ConsensusGroupId groupId) {
    Peer leader = configManager.getConsensusManager().getLeader(groupId);
    for (TConfigNodeLocation onlineConfigNode : onlineConfigNodes) {
      if (!leader.getEndpoint().equals(onlineConfigNode.getInternalEndPoint())) {
        Peer newLeader = new Peer(groupId, onlineConfigNode.getInternalEndPoint());
        ConsensusGenericResponse resp =
            configManager
                .getConsensusManager()
                .getConsensusImpl()
                .transferLeader(groupId, newLeader);
        if (resp.isSuccess()) {
          LOGGER.info("Transfer ConfigNode Leader success.");
          return newLeader;
        }
      }
    }
    return leader;
  }

  private boolean checkConfigNodeDuplicateNum(List<TConfigNodeLocation> onlineConfigNodes) {
    if (onlineConfigNodes.size() <= onlineConfigNodeNum) {
      LOGGER.info("Only {} ConfigNode in current Cluster!", onlineConfigNodes.size());
      return true;
    }
    return false;
  }

  private static class ConfigNodeHolder {

    private static final ConfigNode INSTANCE = new ConfigNode();

    private ConfigNodeHolder() {
      // Empty constructor
    }
  }

  public static ConfigNode getInstance() {
    return ConfigNodeHolder.INSTANCE;
  }
}
