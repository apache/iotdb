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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.AbstractLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.consensus.ConsensusFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link ConfigNodeStartupCheck} checks the parameters in iotdb-confignode.properties and
 * confignode-system.properties when start and restart.
 */
public class ConfigNodeStartupCheck extends StartupChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeStartupCheck.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final int CONFIGNODE_PORTS = 2;

  public ConfigNodeStartupCheck(String nodeRole) {
    super(nodeRole);
  }

  @Override
  protected void portCheck() throws StartupException {
    Set<Integer> portSet = new HashSet<>();
    portSet.add(CONF.getConsensusPort());
    portSet.add(CONF.getInternalPort());
    if (portSet.size() != CONFIGNODE_PORTS) {
      throw new StartupException("ports used in configNode have repeat.");
    } else {
      LOGGER.info("configNode port check successful.");
    }
  }

  @Override
  public void startUpCheck() throws StartupException, IOException, ConfigurationException {
    envCheck();
    portCheck();
    verify();
    checkGlobalConfig();
    createDirsIfNecessary();
    checkRequestManager();
    if (SystemPropertiesUtils.isRestarted()) {
      /* Always restore ConfigNodeId first */
      CONF.setConfigNodeId(SystemPropertiesUtils.loadConfigNodeIdWhenRestarted());

      SystemPropertiesUtils.checkSystemProperties();
    }
  }

  /**
   * Check whether the global configuration of the cluster is correct.
   *
   * @throws ConfigurationException checkGlobalConfig()
   */
  private void checkGlobalConfig() throws ConfigurationException {
    // When the ConfigNode consensus protocol is set to SIMPLE_CONSENSUS,
    // the seed_config_node needs to point to itself
    if (CONF.getConfigNodeConsensusProtocolClass().equals(ConsensusFactory.SIMPLE_CONSENSUS)
        && (!CONF.getInternalAddress().equals(CONF.getSeedConfigNode().getIp())
            || CONF.getInternalPort() != CONF.getSeedConfigNode().getPort())) {
      throw new ConfigurationException(
          IoTDBConstant.CN_SEED_CONFIG_NODE,
          CONF.getSeedConfigNode().getIp() + ":" + CONF.getSeedConfigNode().getPort(),
          CONF.getInternalAddress() + ":" + CONF.getInternalPort(),
          "the config_node_consensus_protocol_class is set to" + ConsensusFactory.SIMPLE_CONSENSUS);
    }

    // The replication factor should be positive
    if (CONF.getSchemaReplicationFactor() <= 0) {
      throw new ConfigurationException("The schema_replication_factor should be positive");
    }
    if (CONF.getDataReplicationFactor() <= 0) {
      throw new ConfigurationException("The data_replication_factor should be positive");
    }

    // When the schema_replication_factor is greater than 1
    // the schema_region_consensus_protocol can't be set to SIMPLE_CONSENSUS
    if (CONF.getSchemaReplicationFactor() > 1
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getSchemaRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          CONF.getSchemaRegionConsensusProtocolClass(),
          ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.SIMPLE_CONSENSUS
              + "available only when schema_replication_factor is set to 1");
    }

    // When the data_replication_factor is greater than 1
    // the data_region_consensus_protocol can't be set to SIMPLE_CONSENSUS
    if (CONF.getDataReplicationFactor() > 1
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          "data_region_consensus_protocol_class",
          CONF.getDataRegionConsensusProtocolClass(),
          ConsensusFactory.IOT_CONSENSUS + "or" + ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.SIMPLE_CONSENSUS
              + "available only when data_replication_factor is set to 1");
    }

    // When the schemaengine region consensus protocol is set to IoTConsensus,
    // we should report an error
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          String.valueOf(CONF.getSchemaRegionConsensusProtocolClass()),
          String.format(
              "%s or %s", ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS),
          "the SchemaRegion doesn't support org.apache.iotdb.consensus.iot.IoTConsensus");
    }

    // When the schemaengine region consensus protocol is set to IoTConsensusV2,
    // we should report an error
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          String.valueOf(CONF.getSchemaRegionConsensusProtocolClass()),
          String.format(
              "%s or %s", ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS),
          "the SchemaRegion doesn't support org.apache.iotdb.consensus.iot.IoTConsensusV2");
    }

    // The leader distribution policy is limited
    if (!AbstractLeaderBalancer.GREEDY_POLICY.equals(CONF.getLeaderDistributionPolicy())
        && !AbstractLeaderBalancer.CFD_POLICY.equals(CONF.getLeaderDistributionPolicy())
        && !AbstractLeaderBalancer.HASH_POLICY.equals(CONF.getLeaderDistributionPolicy())) {
      throw new ConfigurationException(
          "leader_distribution_policy",
          CONF.getRoutePriorityPolicy(),
          "GREEDY or MIN_COST_FLOW or HASH",
          "an unrecognized leader_distribution_policy is set");
    }

    // The route priority policy is limited
    if (!CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.LEADER_POLICY)
        && !CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.GREEDY_POLICY)) {
      throw new ConfigurationException(
          "route_priority_policy",
          CONF.getRoutePriorityPolicy(),
          "LEADER or GREEDY",
          "an unrecognized route_priority_policy is set");
    }

    // The default RegionGroupNum should be positive
    if (CONF.getDefaultSchemaRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException("The default_schema_region_group_num should be positive");
    }
    if (CONF.getDefaultDataRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException("The default_data_region_group_num should be positive");
    }

    // Check time partition origin
    if (COMMON_CONFIG.getTimePartitionOrigin() < 0) {
      throw new ConfigurationException("The time_partition_origin should be non-negative");
    }

    // Check time partition interval
    if (COMMON_CONFIG.getTimePartitionInterval() <= 0) {
      throw new ConfigurationException("The time_partition_interval should be positive");
    }

    // Check timestamp precision
    String timestampPrecision = COMMON_CONFIG.getTimestampPrecision();
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      throw new ConfigurationException("The timestamp_precision should be ms, us or ns");
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

  // The checks are in the initialization process of the RequestManager object.
  private void checkRequestManager() {
    SyncDataNodeClientPool.getInstance();
    CnToDnInternalServiceAsyncRequestManager.getInstance();
  }
}
