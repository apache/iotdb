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
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
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
      throw new StartupException(ConfigNodeMessages.PORTS_USED_IN_CONFIGNODE_HAVE_REPEAT);
    } else {
      LOGGER.info(ConfigNodeMessages.CONFIGNODE_PORT_CHECK_SUCCESSFUL);
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
          CONF.getSeedConfigNode().getIp()
              + ConfigNodeMessages.EXCEPTION_COLON_5D70AD09
              + CONF.getSeedConfigNode().getPort(),
          CONF.getInternalAddress()
              + ConfigNodeMessages.EXCEPTION_COLON_5D70AD09
              + CONF.getInternalPort(),
          ConfigNodeMessages.EXCEPTION_CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS_SET_E7A83ED6
              + ConsensusFactory.SIMPLE_CONSENSUS);
    }

    // The replication factor should be positive
    if (CONF.getSchemaReplicationFactor() <= 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_SCHEMA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE);
    }
    if (CONF.getDataReplicationFactor() <= 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_DATA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE);
    }

    // When the schema_replication_factor is greater than 1
    // the schema_region_consensus_protocol can't be set to SIMPLE_CONSENSUS
    if (CONF.getSchemaReplicationFactor() > 1
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getSchemaRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          ConfigNodeMessages.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
          CONF.getSchemaRegionConsensusProtocolClass(),
          ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.SIMPLE_CONSENSUS
              + ConfigNodeMessages
                  .EXCEPTION_AVAILABLE_ONLY_SCHEMA_REPLICATION_FACTOR_SET_1_45667207);
    }

    // When the data_replication_factor is greater than 1
    // the data_region_consensus_protocol can't be set to SIMPLE_CONSENSUS
    if (CONF.getDataReplicationFactor() > 1
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          ConfigNodeMessages.DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
          CONF.getDataRegionConsensusProtocolClass(),
          ConsensusFactory.IOT_CONSENSUS
              + ConfigNodeMessages.EXCEPTION_MESSAGE_E81C4E4F
              + ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.SIMPLE_CONSENSUS
              + ConfigNodeMessages.EXCEPTION_AVAILABLE_ONLY_DATA_REPLICATION_FACTOR_SET_1_71748D3D);
    }

    // When the schemaengine region consensus protocol is set to IoTConsensus,
    // we should report an error
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      throw new ConfigurationException(
          ConfigNodeMessages.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
          String.valueOf(CONF.getSchemaRegionConsensusProtocolClass()),
          String.format(
              ConfigNodeMessages.EXCEPTION_ARG_ARG_6E068B23,
              ConsensusFactory.SIMPLE_CONSENSUS,
              ConsensusFactory.RATIS_CONSENSUS),
          ConfigNodeMessages
              .EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUS_84350FD1);
    }

    // When the schemaengine region consensus protocol is set to IoTConsensusV2,
    // we should report an error
    if (CONF.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
      throw new ConfigurationException(
          ConfigNodeMessages.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
          String.valueOf(CONF.getSchemaRegionConsensusProtocolClass()),
          String.format(
              ConfigNodeMessages.EXCEPTION_ARG_ARG_6E068B23,
              ConsensusFactory.SIMPLE_CONSENSUS,
              ConsensusFactory.RATIS_CONSENSUS),
          ConfigNodeMessages
              .EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUSV2_BA353C6D);
    }

    // The leader distribution policy is limited
    if (!AbstractLeaderBalancer.GREEDY_POLICY.equals(CONF.getLeaderDistributionPolicy())
        && !AbstractLeaderBalancer.CFS_POLICY.equals(CONF.getLeaderDistributionPolicy())
        && !AbstractLeaderBalancer.HASH_POLICY.equals(CONF.getLeaderDistributionPolicy())) {
      throw new ConfigurationException(
          ConfigNodeMessages.LEADER_DISTRIBUTION_POLICY,
          CONF.getRoutePriorityPolicy(),
          ConfigNodeMessages.EXCEPTION_GREEDY_MIN_COST_FLOW_HASH_C07DA2EE,
          ConfigNodeMessages.EXCEPTION_UNRECOGNIZED_LEADER_DISTRIBUTION_POLICY_SET_F9FFB410);
    }

    // The route priority policy is limited
    if (!CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.LEADER_POLICY)
        && !CONF.getRoutePriorityPolicy().equals(IPriorityBalancer.GREEDY_POLICY)) {
      throw new ConfigurationException(
          ConfigNodeMessages.ROUTE_PRIORITY_POLICY,
          CONF.getRoutePriorityPolicy(),
          ConfigNodeMessages.EXCEPTION_LEADER_GREEDY_55C6B994,
          ConfigNodeMessages.EXCEPTION_UNRECOGNIZED_ROUTE_PRIORITY_POLICY_SET_C0012AE4);
    }

    // The default RegionGroupNum should be positive
    if (CONF.getDefaultSchemaRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_DEFAULT_SCHEMA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE);
    }
    if (CONF.getDefaultDataRegionGroupNumPerDatabase() <= 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_DEFAULT_DATA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE);
    }

    // Check time partition origin
    if (COMMON_CONFIG.getTimePartitionOrigin() < 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_TIME_PARTITION_ORIGIN_SHOULD_BE_NON_NEGATIVE);
    }

    // Check time partition interval
    if (COMMON_CONFIG.getTimePartitionInterval() <= 0) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_TIME_PARTITION_INTERVAL_SHOULD_BE_POSITIVE);
    }

    // Check timestamp precision
    String timestampPrecision = COMMON_CONFIG.getTimestampPrecision();
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      throw new ConfigurationException(
          ConfigNodeMessages.THE_TIMESTAMP_PRECISION_SHOULD_BE_MS_US_OR_NS);
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
        LOGGER.info(ConfigNodeMessages.MAKE_DIRS, dir);
      } else {
        throw new IOException(
            String.format(
                ConfigNodeMessages.START_CONFIGNODE_FAILED_BECAUSE_COULDN_T_MAKE_SYSTEM_DIRS,
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
