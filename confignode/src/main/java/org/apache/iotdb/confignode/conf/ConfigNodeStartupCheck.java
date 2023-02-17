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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.consensus.ConsensusFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * ConfigNodeStartupCheck checks the parameters in iotdb-confignode.properties and
 * confignode-system.properties when start and restart
 */
public class ConfigNodeStartupCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeStartupCheck.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  public void startUpCheck() throws StartupException, IOException, ConfigurationException {
    checkGlobalConfig();
    createDirsIfNecessary();
    if (SystemPropertiesUtils.isRestarted()) {
      SystemPropertiesUtils.checkSystemProperties();
    }
  }

  /** Check whether the global configuration of the cluster is correct */
  private void checkGlobalConfig() throws ConfigurationException {
    // When the ConfigNode consensus protocol is set to SIMPLE_CONSENSUS,
    // the target_config_node_list needs to point to itself
    if (CONF.getConfigNodeConsensusProtocolClass().equals(ConsensusFactory.SIMPLE_CONSENSUS)
        && (!CONF.getInternalAddress().equals(CONF.getTargetConfigNode().getIp())
            || CONF.getInternalPort() != CONF.getTargetConfigNode().getPort())) {
      throw new ConfigurationException(
          IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST,
          CONF.getTargetConfigNode().getIp() + ":" + CONF.getTargetConfigNode().getPort(),
          CONF.getInternalAddress() + ":" + CONF.getInternalPort());
    }

    // When the data region consensus protocol is set to SIMPLE_CONSENSUS,
    // the data replication factor must be 1
    if (CONF.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.SIMPLE_CONSENSUS)
        && CONF.getDataReplicationFactor() != 1) {
      throw new ConfigurationException(
          "data_replication_factor",
          String.valueOf(CONF.getDataReplicationFactor()),
          String.valueOf(1));
    }

    // When the schema region consensus protocol is set to SIMPLE_CONSENSUS,
    // the schema replication factor must be 1
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.SIMPLE_CONSENSUS)
        && CONF.getSchemaReplicationFactor() != 1) {
      throw new ConfigurationException(
          "schema_replication_factor",
          String.valueOf(CONF.getSchemaReplicationFactor()),
          String.valueOf(1));
    }

    // When the schema region consensus protocol is set to IoTConsensus,
    // we should report an error
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          String.valueOf(CONF.getSchemaRegionConsensusProtocolClass()),
          String.format(
              "%s or %s", ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS));
    }

    // The leader distribution policy is limited
    if (!ILeaderBalancer.GREEDY_POLICY.equals(CONF.getLeaderDistributionPolicy())
        && !ILeaderBalancer.MIN_COST_FLOW_POLICY.equals(CONF.getLeaderDistributionPolicy())) {
      throw new ConfigurationException(
          "leader_distribution_policy", CONF.getRoutePriorityPolicy(), "GREEDY or MIN_COST_FLOW");
    }

    // The route priority policy is limited
    if (!CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.LEADER_POLICY)
        && !CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.GREEDY_POLICY)) {
      throw new ConfigurationException(
          "route_priority_policy", CONF.getRoutePriorityPolicy(), "LEADER or GREEDY");
    }

    // The ip of target ConfigNode couldn't be 0.0.0.0
    if (CONF.getTargetConfigNode().getIp().equals("0.0.0.0")) {
      throw new ConfigurationException(
          "The ip address of any target_config_node_list couldn't be 0.0.0.0");
    }

    // The least RegionGroupNum should be positive
    if (CONF.getDefaultSchemaRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException("The default_schema_region_group_num should be positive");
    }
    if (CONF.getDefaultDataRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException("The default_data_region_group_num should be positive");
    }
  }

  private void createDirsIfNecessary() throws IOException {
    // If systemDir does not exist, create systemDir
    File systemDir = new File(CONF.getSystemDir());
    createDirIfEmpty(systemDir);

    // If consensusDir does not exist, create consensusDir
    File consensusDir = new File(CONF.getConsensusDir());
    createDirIfEmpty(consensusDir);
  }

  private void createDirIfEmpty(File dir) throws IOException {
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        LOGGER.info("Make dirs: {}", dir);
      } else {
        throw new IOException(
            String.format(
                "Start ConfigNode failed, because couldn't make system dirs: %s.",
                dir.getAbsolutePath()));
      }
    }
  }

  private static class ConfigNodeConfCheckHolder {

    private static final ConfigNodeStartupCheck INSTANCE = new ConfigNodeStartupCheck();

    private ConfigNodeConfCheckHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeStartupCheck getInstance() {
    return ConfigNodeConfCheckHolder.INSTANCE;
  }
}
