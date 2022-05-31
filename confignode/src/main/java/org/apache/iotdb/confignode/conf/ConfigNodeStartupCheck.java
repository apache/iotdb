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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

/**
 * ConfigNodeStartupCheck checks the cluster status and parameters in iotdb-confignode.properties
 * when started and restart
 */
public class ConfigNodeStartupCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeStartupCheck.class);

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private final File systemPropertiesFile;
  private final Properties systemProperties;

  private ConfigNodeStartupCheck() {
    systemPropertiesFile =
        new File(conf.getSystemDir() + File.separator + ConfigNodeConstant.SYSTEM_FILE_NAME);
    systemProperties = new Properties();
  }

  public void startUpCheck() throws StartupException, IOException, ConfigurationException {
    checkGlobalConfig();
    if (isFirstStart()) {
      // Do initialization when first start
      if (!isSeedConfigNode()) {
        // Register when the current ConfigNode isn't Seed-ConfigNode
        registerConfigNode();
        // Apply after constructing PartitionRegion
        conf.setNeedApply(true);
      }
      // Persistence the unchangeable parameters
      writeSystemProperties();
    } else {
      checkSystemProperties();
      loadConfigNodeList();
      // TODO: Notify the ConfigNodeGroup if current ConfigNode's ip or port has changed
    }
  }

  /** Check whether the global configuration of the cluster is correct */
  private void checkGlobalConfig() throws ConfigurationException {
    // When the ConfigNode consensus protocol is set to StandAlone,
    // the target_configNode needs to point to itself
    if (conf.getConfigNodeConsensusProtocolClass().equals(ConsensusFactory.StandAloneConsensus)
        && (!conf.getRpcAddress().equals(conf.getTargetConfigNode().getIp())
            || conf.getRpcPort() != conf.getTargetConfigNode().getPort())) {
      throw new ConfigurationException(
          "target_confignode",
          conf.getTargetConfigNode().getIp() + ":" + conf.getTargetConfigNode().getPort(),
          conf.getRpcAddress() + ":" + conf.getRpcPort());
    }

    // When the data region consensus protocol is set to StandAlone,
    // the data replication factor must be 1
    if (conf.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.StandAloneConsensus)
        && conf.getDataReplicationFactor() != 1) {
      throw new ConfigurationException(
          "data_replication_factor",
          String.valueOf(conf.getDataReplicationFactor()),
          String.valueOf(1));
    }

    // When the schema region consensus protocol is set to StandAlone,
    // the schema replication factor must be 1
    if (conf.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.StandAloneConsensus)
        && conf.getSchemaReplicationFactor() != 1) {
      throw new ConfigurationException(
          "schema_replication_factor",
          String.valueOf(conf.getSchemaReplicationFactor()),
          String.valueOf(1));
    }

    // When the schema region consensus protocol is set to MultiLeaderConsensus,
    // we should report an error
    if (conf.getSchemaRegionConsensusProtocolClass()
        .equals(ConsensusFactory.MultiLeaderConsensus)) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          String.valueOf(conf.getSchemaRegionConsensusProtocolClass()),
          String.format(
              "%s or %s", ConsensusFactory.StandAloneConsensus, ConsensusFactory.RatisConsensus));
    }
  }

  /**
   * Check if the ConfigNode is started for the first time. Prepare the configNode-system.properties
   * file along the way.
   *
   * @return True if confignode-system.properties doesn't exist.
   */
  private boolean isFirstStart() throws IOException, StartupException {
    // If systemDir does not exist, create systemDir
    File systemDir = new File(conf.getSystemDir());
    createDirIfEmpty(systemDir);

    // If consensusDir does not exist, create consensusDir
    File consensusDir = new File(conf.getConsensusDir());
    createDirIfEmpty(consensusDir);

    // Check if system properties file exists
    boolean isFirstStart;
    if (!systemPropertiesFile.exists()) {
      // Create the system properties file when first start the ConfigNode
      if (systemPropertiesFile.createNewFile()) {
        LOGGER.info(
            "System properties file {} for ConfigNode is created.",
            systemPropertiesFile.getAbsolutePath());
      } else {
        LOGGER.error(
            "Can't create the system properties file {} for ConfigNode. IoTDB-ConfigNode is shutdown.",
            systemPropertiesFile.getAbsolutePath());
        throw new StartupException("Can't create system properties file");
      }

      isFirstStart = true;
    } else {
      // Load system properties file
      try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
        systemProperties.load(inputStream);
      }

      isFirstStart = false;
    }

    return isFirstStart;
  }

  /**
   * Check if the current ConfigNode is SeedConfigNode. If true, do the SeedConfigNode configuration
   * as well.
   *
   * @return True if the target_confignode points to itself
   */
  private boolean isSeedConfigNode() {
    boolean result =
        conf.getRpcAddress().equals(conf.getTargetConfigNode().getIp())
            && conf.getRpcPort() == conf.getTargetConfigNode().getPort();
    if (result) {
      // TODO: Set PartitionRegionId from iotdb-confignode.properties
      conf.setConfigNodeList(
          Collections.singletonList(
              new TConfigNodeLocation(
                  new TEndPoint(conf.getRpcAddress(), conf.getRpcPort()),
                  new TEndPoint(conf.getRpcAddress(), conf.getConsensusPort()))));
    }
    return result;
  }

  /** Register ConfigNode when first startup */
  private void registerConfigNode() throws StartupException {
    TConfigNodeRegisterReq req =
        new TConfigNodeRegisterReq(
            new TConfigNodeLocation(
                new TEndPoint(conf.getRpcAddress(), conf.getRpcPort()),
                new TEndPoint(conf.getRpcAddress(), conf.getConsensusPort())),
            conf.getDataRegionConsensusProtocolClass(),
            conf.getSchemaRegionConsensusProtocolClass(),
            conf.getSeriesPartitionSlotNum(),
            conf.getSeriesPartitionExecutorClass(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTL(),
            conf.getTimePartitionInterval(),
            conf.getSchemaReplicationFactor(),
            conf.getDataReplicationFactor());

    TConfigNodeRegisterResp resp =
        SyncConfigNodeClientPool.getInstance().registerConfigNode(conf.getTargetConfigNode(), req);
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      conf.setPartitionRegionId(resp.getPartitionRegionId().getId());
      conf.setConfigNodeList(resp.getConfigNodeList());
    } else {
      throw new StartupException("Register ConfigNode failed!");
    }
  }

  /**
   * There are some special parameters that can't be changed after a ConfigNode first started.
   * Therefore, store them in confignode-system.properties during the first startup
   */
  private void writeSystemProperties() {
    // Startup configuration
    systemProperties.setProperty("rpc_address", String.valueOf(conf.getRpcAddress()));
    systemProperties.setProperty("rpc_port", String.valueOf(conf.getRpcPort()));
    systemProperties.setProperty("consensus_port", String.valueOf(conf.getConsensusPort()));

    // Consensus protocol configuration
    systemProperties.setProperty(
        "config_node_consensus_protocol_class", conf.getConfigNodeConsensusProtocolClass());
    systemProperties.setProperty(
        "data_region_consensus_protocol_class", conf.getDataRegionConsensusProtocolClass());
    systemProperties.setProperty(
        "schema_region_consensus_protocol_class", conf.getSchemaRegionConsensusProtocolClass());

    // PartitionSlot configuration
    systemProperties.setProperty(
        "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum()));
    systemProperties.setProperty(
        "series_partition_executor_class", conf.getSeriesPartitionExecutorClass());

    // ConfigNodeList
    systemProperties.setProperty(
        "confignode_list", NodeUrlUtils.convertTConfigNodeUrls(conf.getConfigNodeList()));

    try {
      systemProperties.store(new FileOutputStream(systemPropertiesFile), "");
    } catch (IOException e) {
      LOGGER.error(
          "Can't store system properties file {}.", systemPropertiesFile.getAbsolutePath());
    }
  }

  /** Ensure that special parameters are consistent with each startup except the first one */
  private void checkSystemProperties() throws ConfigurationException {
    boolean needReWrite = false;

    // Startup configuration
    String rpcAddress = systemProperties.getProperty("rpc_address", null);
    if (rpcAddress == null) {
      needReWrite = true;
    } else if (!rpcAddress.equals(conf.getRpcAddress())) {
      throw new ConfigurationException("rpc_address", conf.getRpcAddress(), rpcAddress);
    }

    if (systemProperties.getProperty("rpc_port", null) == null) {
      needReWrite = true;
    } else {
      int rpcPort = Integer.parseInt(systemProperties.getProperty("rpc_port"));
      if (rpcPort != conf.getRpcPort()) {
        throw new ConfigurationException(
            "rpc_port", String.valueOf(conf.getRpcPort()), String.valueOf(rpcPort));
      }
    }

    if (systemProperties.getProperty("consensus_port", null) == null) {
      needReWrite = true;
    } else {
      int consensusPort = Integer.parseInt(systemProperties.getProperty("consensus_port"));
      if (consensusPort != conf.getConsensusPort()) {
        throw new ConfigurationException(
            "consensus_port",
            String.valueOf(conf.getConsensusPort()),
            String.valueOf(consensusPort));
      }
    }

    // Consensus protocol configuration
    String configNodeConsensusProtocolClass =
        systemProperties.getProperty("config_node_consensus_protocol_class", null);
    if (configNodeConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!configNodeConsensusProtocolClass.equals(
        conf.getConfigNodeConsensusProtocolClass())) {
      throw new ConfigurationException(
          "config_node_consensus_protocol_class",
          conf.getConfigNodeConsensusProtocolClass(),
          configNodeConsensusProtocolClass);
    }

    String dataRegionConsensusProtocolClass =
        systemProperties.getProperty("data_region_consensus_protocol_class", null);
    if (dataRegionConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!dataRegionConsensusProtocolClass.equals(
        conf.getDataRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          "data_region_consensus_protocol_class",
          conf.getDataRegionConsensusProtocolClass(),
          dataRegionConsensusProtocolClass);
    }

    String schemaRegionConsensusProtocolClass =
        systemProperties.getProperty("schema_region_consensus_protocol_class", null);
    if (schemaRegionConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!schemaRegionConsensusProtocolClass.equals(
        conf.getSchemaRegionConsensusProtocolClass())) {
      throw new ConfigurationException(
          "schema_region_consensus_protocol_class",
          conf.getSchemaRegionConsensusProtocolClass(),
          schemaRegionConsensusProtocolClass);
    }

    // PartitionSlot configuration
    if (systemProperties.getProperty("series_partition_slot_num", null) == null) {
      needReWrite = true;
    } else {
      int seriesPartitionSlotNum =
          Integer.parseInt(systemProperties.getProperty("series_partition_slot_num"));
      if (seriesPartitionSlotNum != conf.getSeriesPartitionSlotNum()) {
        throw new ConfigurationException(
            "series_partition_slot_num",
            String.valueOf(conf.getSeriesPartitionSlotNum()),
            String.valueOf(seriesPartitionSlotNum));
      }
    }

    String seriesPartitionSlotExecutorClass =
        systemProperties.getProperty("series_partition_executor_class", null);
    if (seriesPartitionSlotExecutorClass == null) {
      needReWrite = true;
    } else if (!Objects.equals(
        seriesPartitionSlotExecutorClass, conf.getSeriesPartitionExecutorClass())) {
      throw new ConfigurationException(
          "series_partition_executor_class",
          conf.getSeriesPartitionExecutorClass(),
          seriesPartitionSlotExecutorClass);
    }

    if (needReWrite) {
      // Re-write special parameters if necessary
      writeSystemProperties();
    }
  }

  /** Only load ConfigNodeList from confignode-system.properties when restart */
  private void loadConfigNodeList() throws StartupException {
    String addresses = systemProperties.getProperty("confignode_list", null);
    if (addresses != null) {
      try {
        conf.setConfigNodeList(NodeUrlUtils.parseTConfigNodeUrls(addresses));
      } catch (BadNodeUrlException e) {
        throw new StartupException("Parse ConfigNodeList failed: {}", e.getMessage());
      }
    }
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
